import java.io.File

import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.{SparkContext, SparkConf}
import org.slf4j.LoggerFactory

import scala.math.log

case class OneWayFriendship(anotherUser: Int, fType: Int)
case class Friendship(user1: Int, user2: Int, commonFriendSize: Int, combinedFType: Int)
case class PairWithCommonFriends(person1: Int, person2: Int, commonFriendsCount: Double)
case class UserFriends(user: Int, friends: Array[OneWayFriendship])
case class Profile(age: Int, sex: Int, country: Long, location: Long, loginRegion: Long)

object Baseline {
  val Log = LoggerFactory.getLogger(Baseline.getClass)

  val NumPartitions = 140
  val NumPartitionsGraph = 90
  val MeaningfulMaxFriendsCount = 1000

  def main(args: Array[String]) {

    val sparkConf = new SparkConf()
      .setAppName("Baseline")
    val sc = new SparkContext(sparkConf)
    val sqlc = new SQLContext(sc)

    val dataDir = if (args.length == 1) args(0) else "./"

    val graphPath = dataDir + "trainGraph"
    val reversedGraphPath = dataDir + "trainSubReversedGraph"
    val commonFriendsPath = dataDir + "commonFriendsCountsPartitioned"
    val demographyPath = dataDir + "demography"
    val predictionPath = dataDir + "prediction"
    val modelPath = dataDir + "LogisticRegressionModel"

    // read graph
    val graph = graphPrepare(sc, graphPath)
    reversedGraphPrepare(reversedGraphPath, graph, sqlc)
    commonFriendsPrepare(commonFriendsPath, reversedGraphPath, sqlc)

    // prepare data for training model
    // step 2
    val commonFriendsCounts = {
      sqlc
        .read.parquet(commonFriendsPath + "/part_33")
        .map(t => new PairWithCommonFriends(t.getAs[Int](0), t.getAs[Int](1), t.getAs[Double](2)))
    }

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

    // step 5
    val trainData = {
      DataPreparingHelpers.prepareData(commonFriendsCounts, positives, ageSexBC)
        .map(t => LabeledPoint(t._2._2.getOrElse(0.0), t._2._1))
    }


    // split data into training (10%) and validation (90%)
    // step 6
    val splits = trainData.randomSplit(Array(0.1, 0.9), seed = 11L)
    val training = splits(0).cache()
    val validation = splits(1)

    // run training algorithm to build the model
    val model = ModelHelpers.logisticRegressionModel(training)

    model.save(sc, modelPath)

    val predictionAndLabels = {
      validation.map { case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
      }
    }

    // estimate model quality
    @transient val metricsLogReg = new BinaryClassificationMetrics(predictionAndLabels, 100)
    val threshold = metricsLogReg.fMeasureByThreshold(2.0).sortBy(-_._2).take(1)(0)._1

    val rocLogReg = metricsLogReg.areaUnderROC()
    println("model ROC = " + rocLogReg.toString)

    // compute scores on the test set
    // step 7
    val testCommonFriendsCounts = {
      sqlc
        .read.parquet(commonFriendsPath + "/part_*/")
        .map(t => PairWithCommonFriends(t.getAs[Int](0), t.getAs[Int](1), t.getAs[Double](2)))
        .filter(pair => pair.person1 % 11 == 7 || pair.person2 % 11 == 7)
    }

    val testData = {
      DataPreparingHelpers.prepareData(testCommonFriendsCounts, positives, ageSexBC)
        .map(t => t._1 -> LabeledPoint(t._2._2.getOrElse(0.0), t._2._1))
        .filter(t => t._2.label == 0.0)
    }

    ModelHelpers.buildPrediction(testData, model, threshold, predictionPath)
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
          .filter(userFriends => userFriends.friends.length >= 2 && userFriends.friends.length <= MeaningfulMaxFriendsCount) //was 8 before
          .flatMap(userFriends => userFriends.friends.map(
            x => (x.anotherUser, OneWayFriendship(userFriends.user, FriendshipHelpers.invertFriendshipType(x.fType)))))
          .groupByKey(NumPartitions)
          .map({case (userFromList, oneWayFriendshipSeq) => oneWayFriendshipSeq.toArray})
          .filter(userFriends => userFriends.length >= 2 && userFriends.length <= MeaningfulMaxFriendsCount)
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
                    (FriendshipHelpers.getCoefForCombinedFriendship(friendship.combinedFType) / log(friendship.commonFriendSize)))
              )
              .reduceByKey((x, y) => x + y)
              .map({case ((user1, user2), fScore) => PairWithCommonFriends(user1, user2, fScore)})
              .filter(pair => pair.commonFriendsCount >= 2) //was 8 before
        }

        commonFriendsCounts.toDF.repartition(4).write.parquet(commonFriendsPath + "/part_" + partition)
      }
    }
  }
}
