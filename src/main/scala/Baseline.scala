import java.io.File

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.{SparkContext, SparkConf}
import org.slf4j.LoggerFactory

import scala.math.log
import org.apache.spark.mllib.linalg.{Vector, Vectors}

case class OneWayFriendship(anotherUser: Int, fType: Int)
//describe pair of user with common friend between them
case class MiddleFriendship(user1: Int, user2: Int, middleUserCommonFriendSize: Int, granulatedMergedFType: Int)
case class SquashedFriendship(weighedFLink: Double, fAccumulator: Int)
case class PairWithCommonFriends(person1: Int, person2: Int, commonFriendsCount: Double, fAccumulator: Int)
case class UserFriends(user: Int, friends: Array[OneWayFriendship])
case class Profile(create_date: Long, age: Int, sex: Int, country: Long, location: Long, loginRegion: Long)

object Baseline {
  val Log = LoggerFactory.getLogger(Baseline.getClass)

  val NumPartitions = 50
  val NumPartitionsGraph = 40

  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
      .setAppName("Baseline")
    val sc = new SparkContext(sparkConf)
    val sqlc = new SQLContext(sc)
    import sqlc.implicits._

    val dataDir = if (args.length == 1) args(0) else "./"

    val graphPath = dataDir + "trainGraph"
    val reversedGraphPath = dataDir + "trainSubReversedGraph"
    val commonFriendsPath = dataDir + "commonFriendsCountsPartitioned"
    val demographyPath = dataDir + "demography"
    val predictionPath = dataDir + "prediction"

    // read graph
    val graph = graphPrepare(sc, graphPath)
    reversedGraphPrepare(reversedGraphPath, graph, sqlc)
    commonFriendsPrepare(commonFriendsPath, reversedGraphPath, sqlc)

    // prepare data for training model
    // step 2
    val commonFriends = sqlc
        .read.parquet(commonFriendsPath + "/part_*/")
        .map(t => PairWithCommonFriends(t.getAs[Int](0), t.getAs[Int](1), t.getAs[Double](2), t.getAs[Int](3)))

    // step 3
    val usersBC = sc.broadcast(graph.map(userFriends => userFriends.user).collect().toSet)

    val positives = {
      graph
        .flatMap(
          userFriends => userFriends.friends
            .filter(oneWayFriendship => usersBC.value.contains(oneWayFriendship.anotherUser) && oneWayFriendship.anotherUser > userFriends.user)
            .map(x => (userFriends.user, x.anotherUser) -> 1.0)
        )
    }

    val ageSexBC = DataPreparingHelpers.prepareAgeSexBroadcast(sc, demographyPath)

    // split data into training (90%) and validation (10%)
    // step 6
    val trainCommonFriendsCounts = commonFriends.filter(pair => pair.person1 % 11 != 7 && pair.person2 % 11 != 7)
    val splits = trainCommonFriendsCounts.randomSplit(Array(0.7, 0.2, 0.1), seed = 11L)
    val training = splits(0).cache()
    val ensembleTraining = splits(1).cache()
    val validation = splits(2).cache()

    //prepare low-level models
    val aaTrainingData = DataPreparingHelpers.prepareAdamicAdarData(training, positives, ageSexBC)
        .map(t => LabeledPoint(t._2._2.getOrElse(0.0), t._2._1))
    val aaModel = Strategies.aaClassificationModel(aaTrainingData, sqlc)

    val ftTrainingData = DataPreparingHelpers.prepareFriendsTypeData(training, positives, ageSexBC)
        .map(t => LabeledPoint(t._2._2.getOrElse(0.0), t._2._1))
    val ftModel = Strategies.fTypeClassificationModel(ftTrainingData, sqlc)

    val userTrainingData = DataPreparingHelpers.prepareUserData(training, positives, ageSexBC)
        .map(t => LabeledPoint(t._2._2.getOrElse(0.0), t._2._1))
    val userModel = Strategies.userClassificationModel(userTrainingData, sqlc)

    //prepare data for ensemble model
    val aaETrainingData = toPreDF(DataPreparingHelpers.prepareAdamicAdarData(ensembleTraining, positives, ageSexBC))
    val aaETrainPredicted = aaModel.predict[String](aaETrainingData.toDF(DataFrameColumns.KEY, DataFrameColumns.LABEL, DataFrameColumns.FEATURES))

    val ftETrainingData = toPreDF(DataPreparingHelpers.prepareFriendsTypeData(ensembleTraining, positives, ageSexBC))
    val ftETrainPredicted = ftModel.predict[String](ftETrainingData.toDF(DataFrameColumns.KEY, DataFrameColumns.LABEL, DataFrameColumns.FEATURES))

    val userETrainingData = toPreDF(DataPreparingHelpers.prepareUserData(ensembleTraining, positives, ageSexBC))
    val userETrainPredicted = userModel.predict[String](userETrainingData.toDF(DataFrameColumns.KEY, DataFrameColumns.LABEL, DataFrameColumns.FEATURES))

    val trainForEnsemble = joinLabelsAndPredictionsToLabeledPoints(aaETrainingData.map({case (key, label, features) => (key, label)}),
        aaETrainPredicted, ftETrainPredicted, userETrainPredicted)

    //train ensemble
    val ensembleModel = Strategies.ensembleClassificationModel(trainForEnsemble, sqlc)

    //prepare data for ensemble validation
    val aaEValidationData = toPreDF(DataPreparingHelpers.prepareAdamicAdarData(validation, positives, ageSexBC))
    val aaEValidationPredicted = aaModel.predict[String](aaEValidationData.toDF(DataFrameColumns.KEY, DataFrameColumns.LABEL, DataFrameColumns.FEATURES))

    val ftEValidationData = toPreDF(DataPreparingHelpers.prepareFriendsTypeData(validation, positives, ageSexBC))
    val ftEValidationPredicted = ftModel.predict[String](ftEValidationData.toDF(DataFrameColumns.KEY, DataFrameColumns.LABEL, DataFrameColumns.FEATURES))

    val userEValidationData = toPreDF(DataPreparingHelpers.prepareUserData(validation, positives, ageSexBC))
    val userEValidationPredicted = userModel.predict[String](userEValidationData.toDF(DataFrameColumns.KEY, DataFrameColumns.LABEL, DataFrameColumns.FEATURES))

    val validationForEnsemble = joinLabelsAndPredictionsToLabeledPoints(
      aaEValidationData.map({case (key, label, features) => (key, label)}),
      aaEValidationPredicted, ftEValidationPredicted, userEValidationPredicted)
    val threshold = validateAndGetThreshold(validationForEnsemble, ensembleModel, sqlc)



    // compute scores on the test set
    // step 7
    val testCommonFriendsCounts = commonFriends.filter(pair => pair.person1 % 11 == 7 || pair.person2 % 11 == 7).cache()

    //prepare data for ensemble validation
    val aaETestData = onlyWithoutFriendship(toPreDF(DataPreparingHelpers.prepareAdamicAdarData(testCommonFriendsCounts, positives, ageSexBC)))
    val ftETestData = onlyWithoutFriendship(toPreDF(DataPreparingHelpers.prepareFriendsTypeData(testCommonFriendsCounts, positives, ageSexBC)))
    val userETestData = onlyWithoutFriendship(toPreDF(DataPreparingHelpers.prepareUserData(testCommonFriendsCounts, positives, ageSexBC)))

    val aaETestPredicted = aaModel.predict[String](aaETestData.toDF(DataFrameColumns.KEY, DataFrameColumns.LABEL, DataFrameColumns.FEATURES))
    val ftETestPredicted = ftModel.predict[String](ftETestData.toDF(DataFrameColumns.KEY, DataFrameColumns.LABEL, DataFrameColumns.FEATURES))
    val userETestPredicted = ftModel.predict[String](userETestData.toDF(DataFrameColumns.KEY, DataFrameColumns.LABEL, DataFrameColumns.FEATURES))

    val testForEnsemble = aaETestData.map({case (key, label, features) => (key, label)})
        .join(aaETestPredicted).join(ftETestPredicted).join(userETestPredicted)
        .map({case (key, (((label, aaPred), ftPred), userPred)) => (key, label, Vectors.dense(aaPred, ftPred, userPred))})

    val predictedRDD = ensembleModel.predict[String](testForEnsemble.toDF(DataFrameColumns.KEY, DataFrameColumns.LABEL, DataFrameColumns.FEATURES)).cache()

    val testPrediction = {
      predictedRDD
          .flatMap { case (pairStr, predictedProbability) =>
            val (user1, user2) = Helpers.deserializeTuple(pairStr)
            Seq(user1 -> (user2, predictedProbability), user2 -> (user1, predictedProbability))
          }
          .filter(t => t._1 % 11 == 7 && t._2._2 >= threshold)
          .groupByKey(NumPartitions)
          .map(t => {
            val user = t._1
            val friendsWithRatings = t._2
            val topBestFriends = friendsWithRatings.toList.sortBy(-_._2).take(100).map(x => x._1)
            (user, topBestFriends)
          })
          .sortByKey(true, 1)
          .map(t => t._1 + "\t" + t._2.mkString("\t"))
    }

    testPrediction.saveAsTextFile(predictionPath, classOf[GzipCodec])
  }

  def graphPrepare(sc: SparkContext, graphPath: String) = {
    sc.textFile(graphPath)
        .map(line => {
          val lineSplit = line.split("\t")
          val user = lineSplit(0).toInt
          val friends = {
            lineSplit(1)
                .replace("{(", "")
                .replace(")}", "")
                .split("\\),\\(")
                .map(t => t.split(","))
                .map(splitStr => OneWayFriendship(splitStr(0).toInt, FriendshipHelpers.normalizeFriendshipType(splitStr(1).toInt)))
          }
          UserFriends(user, friends)
        })
  }

  def reversedGraphPrepare(reversedGraphPath: String, graph: RDD[UserFriends], sqlc: SQLContext) = {
    import sqlc.implicits._

    if (new File(reversedGraphPath).exists()) {
      Log.warn("Reversed Graph Exists - SKIPPED")
    } else {
      // flat and reverse graph
      // step 1.a from description

      graph
          .filter(userFriends => userFriends.friends.length >= 2) //was 8 before
          .flatMap(userFriends => userFriends.friends.map(
            x => (x.anotherUser, OneWayFriendship(userFriends.user, FriendshipHelpers.invertFriendshipType(x.fType)))))
          .groupByKey(NumPartitions)
          .map({case (userFromList, oneWayFriendshipSeq) => oneWayFriendshipSeq.toArray})
          .filter(userFriends => userFriends.length >= 2)
          .map(userFriends => userFriends.sortBy({case oneWayFriendship => oneWayFriendship.anotherUser}))
          .map(friends => new Tuple1(friends))
          .toDF
          .write.parquet(reversedGraphPath)
    }
  }

  def commonFriendsPrepare(commonFriendsPath: String, reversedGraphPath: String, sqlc: SQLContext) = {
    import sqlc.implicits._

    if (new File(commonFriendsPath).exists()) {
      Log.warn("Commons Friend Exists - SKIPPED")
    } else {
      // for each pair of ppl count the amount of their common friends
      // amount of shared friends for pair (A, B) and for pair (B, A) is the same
      // so order pair: A < B and count common friends for pairs unique up to permutation
      // step 1.b
      for (partition <- 0 until NumPartitionsGraph) {
        val commonFriendsCounts = {
          sqlc.read.parquet(reversedGraphPath)
              .map(t => DataPreparingHelpers.generatePairs(
                t.getSeq[GenericRowWithSchema](0).map(t =>
                  new OneWayFriendship(t.getAs[Int](0), t.getAs[Int](1))), NumPartitionsGraph, partition))
              .flatMap(pairs => pairs.map(
                friendship => (friendship.user1, friendship.user2) ->
                    SquashedFriendship(Strategies.getCombinedFriendshipCoef(friendship.granulatedMergedFType) / log(friendship.middleUserCommonFriendSize), FriendshipHelpers.granulatedFTypeToFAccumulator(friendship.granulatedMergedFType)))
              )
              .reduceByKey({case (SquashedFriendship(weight1, fAccumulator1), SquashedFriendship(weight2, fAccumulator2)) =>
                SquashedFriendship(weight1 + weight2, FriendshipHelpers.mergeAccumulators(fAccumulator1, fAccumulator2))})
              .map({case ((user1, user2), SquashedFriendship(fScore, fAccumulator)) => PairWithCommonFriends(user1, user2, fScore, fAccumulator)})
              .filter(pair => pair.commonFriendsCount >= 1.5) //was 8 before
        }

        commonFriendsCounts.toDF.repartition(4).write.parquet(commonFriendsPath + "/part_" + partition)
      }
    }
  }

  def validateAndGetThreshold(validationData: RDD[LabeledPoint], model: UnifiedClassifier, sqlc: SQLContext): Double = {
    import sqlc.implicits._

    val validationWithKey = validationData
        .map({case LabeledPoint(label, features) => (Helpers.randomLong(), label, features)}).cache() //cache needed because we generate random ids here
    val predictedRDD = model.predict[Long](validationWithKey.toDF(DataFrameColumns.KEY, DataFrameColumns.LABEL, DataFrameColumns.FEATURES))
    val predictionAndLabels = validationWithKey.map({case (key, label, features) => (key, label)})
        .join(predictedRDD).map({case (key, (label, predictedProbability)) => (predictedProbability, label)})

    // estimate model quality
    @transient val metricsLogReg = new BinaryClassificationMetrics(predictionAndLabels, 100)
    val threshold = metricsLogReg.fMeasureByThreshold(2.0).sortBy(-_._2).take(1)(0)._1
    println("Use threshold = " + threshold)

    val rocLogReg = metricsLogReg.areaUnderROC()
    println("model ROC = " + rocLogReg.toString)

    threshold
  }

  def toPreDF(preparedData: RDD[((Int, Int), (Vector, Option[Double]))]): RDD[(String, Double, Vector)] = {
    preparedData.map({case (userPair, (features, positiveOption)) => (Helpers.serializeTuple(userPair), positiveOption.getOrElse(0.0), features)})
  }

  def onlyWithoutFriendship(preDF: RDD[(String, Double, Vector)]): RDD[(String, Double, Vector)] = {
    preDF.filter({case (key, label, features) => label == 0.0})
  }

  def joinLabelsAndPredictionsToLabeledPoints(labeled: RDD[(String, Double)], aaPrediction: RDD[(String, Double)], ftPrediction: RDD[(String, Double)], userPrediction: RDD[(String, Double)]) = {
    labeled.join(aaPrediction).join(ftPrediction).join(userPrediction)
        .map({case (key, (((label, aaPred), ftPred), userPred)) => LabeledPoint(label, Vectors.dense(aaPred, ftPred, userPred))})
  }
}
