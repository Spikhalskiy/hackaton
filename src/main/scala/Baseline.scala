import java.io.File

import breeze.numerics.abs
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.{SparkContext, SparkConf}
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer
import scala.math.log

case class OneWayFriendship(anotherUser: Int, fType: Int)
case class Friendship(user1: Int, user2: Int, commonFriendSize: Int, combinedFType: Int)
case class PairWithCommonFriends(person1: Int, person2: Int, commonFriendsCount: Double)
case class UserFriends(user: Int, friends: Array[OneWayFriendship])
case class Profile(age: Int, sex: Int)

object Baseline {
  val Log = LoggerFactory.getLogger(Baseline.getClass)

  val NumPartitions = 120
  val NumPartitionsGraph = 60
  val MeaningfulMaxFriendsCount = 1000

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
    val modelPath = dataDir + "LogisticRegressionModel"

    // read graph
    val graph = {
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
                  .map(splitStr => OneWayFriendship(splitStr(0).toInt, Helpers.normalizeFriendshipType(splitStr(1).toInt)))
            }
            UserFriends(user, friends)
          })
    }

    if (new File(reversedGraphPath).exists()) {
      Log.warn("Reversed Graph Exists - SKIPPED")
    } else {
      // flat and reverse graph
      // step 1.a from description

      graph
          .filter(userFriends => userFriends.friends.length >= 2 && userFriends.friends.length <= MeaningfulMaxFriendsCount) //was 8 before
          .flatMap(userFriends => userFriends.friends.map(
              x => (x.anotherUser, OneWayFriendship(userFriends.user, Helpers.invertFriendshipType(x.fType)))))
          .groupByKey(NumPartitions)
          .map({case (userFromList, oneWayFriendshipSeq) => oneWayFriendshipSeq.toArray})
          .filter(userFriends => userFriends.length >= 2 && userFriends.length <= MeaningfulMaxFriendsCount)
          .map(userFriends => userFriends.sortBy({case oneWayFriendship => oneWayFriendship.anotherUser}))
          .map(friends => new Tuple1(friends))
          .toDF
          .write.parquet(reversedGraphPath)
    }

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
              .map(t => generatePairs(
                t.getSeq[GenericRowWithSchema](0).map(t =>
                  new OneWayFriendship(t.getAs[Int](0), t.getAs[Int](1))), NumPartitionsGraph, partition))
              .flatMap(pairs => pairs.map(
                friendship => (friendship.user1, friendship.user2) ->
                    ((if (friendship.combinedFType > 0) 1.0 else 1.0) / log(friendship.commonFriendSize)))
              )
              .reduceByKey((x, y) => x + y)
              .map({case ((user1, user2), fScore) => PairWithCommonFriends(user1, user2, fScore)})
              .filter(pair => pair.commonFriendsCount >= 1) //was 8 before
        }

        commonFriendsCounts.toDF.repartition(4).write.parquet(commonFriendsPath + "/part_" + partition)
      }
    }

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

    val ageSexBC = prepareAgeSexBroadcast(sc, demographyPath)

    // step 5
    val trainData = {
      prepareData(commonFriendsCounts, positives, ageSexBC)
        .map(t => LabeledPoint(t._2._2.getOrElse(0.0), t._2._1))
    }


    // split data into training (10%) and validation (90%)
    // step 6
    val splits = trainData.randomSplit(Array(0.1, 0.9), seed = 11L)
    val training = splits(0).cache()
    val validation = splits(1)

    // run training algorithm to build the model
    val model = {
      new LogisticRegressionWithLBFGS()
        .setNumClasses(2)
        .run(training)
    }

    model.clearThreshold()
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
      prepareData(testCommonFriendsCounts, positives, ageSexBC)
        .map(t => t._1 -> LabeledPoint(t._2._2.getOrElse(0.0), t._2._1))
        .filter(t => t._2.label == 0.0)
    }

    buildPrediction(testData, model, threshold, predictionPath)
  }

  // step 4
  def prepareAgeSexBroadcast(sc: SparkContext, demographyPath: String) = {
    val ageSex =
      sc.textFile(demographyPath)
        .map(line => {
          val lineSplit = line.trim().split("\t")
          if (lineSplit(2) == "") {
            lineSplit(0).toInt -> Profile(0, lineSplit(3).toInt)
          }
          else {
            lineSplit(0).toInt -> Profile(lineSplit(2).toInt, lineSplit(3).toInt)
          }
        })
    sc.broadcast(ageSex.collectAsMap())
  }

  // step 8
  def buildPrediction(
      testData: RDD[((Int, Int), LabeledPoint)], model: LogisticRegressionModel,
      threshold: Double, predictionPath: String) = {

    val testPrediction = {
      testData
          .flatMap { case (id, LabeledPoint(label, features)) =>
            val prediction = model.predict(features)
            Seq(id._1 -> (id._2, prediction), id._2 -> (id._1, prediction))
          }
          .filter(t => t._1 % 11 == 7 && t._2._2 >= threshold)
          .groupByKey(NumPartitions)
          .map(t => {
            val user = t._1
            val firendsWithRatings = t._2
            val topBestFriends = firendsWithRatings.toList.sortBy(-_._2).take(100).map(x => x._1)
            (user, topBestFriends)
          })
          .sortByKey(true, 1)
          .map(t => t._1 + "\t" + t._2.mkString("\t"))
    }

    testPrediction.saveAsTextFile(predictionPath,  classOf[GzipCodec])
  }


  def generatePairs(pplWithCommonFriends: Seq[OneWayFriendship], numPartitions: Int, k: Int) = {
    val pairs = ArrayBuffer.empty[Friendship]
    for (i <- pplWithCommonFriends.indices) {
      val f1 = pplWithCommonFriends(i)
      if (f1.anotherUser % numPartitions == k) {
        for (j <- i + 1 until pplWithCommonFriends.length) {
          val f2 = pplWithCommonFriends(j)
          pairs.append(Friendship(f1.anotherUser, f2.anotherUser,
            pplWithCommonFriends.length, Helpers.combineFriendshipTypesToMask(f1.fType, f2.fType)))
        }
      }
    }
    pairs
  }

  def prepareData(
      commonFriendsCounts: RDD[PairWithCommonFriends],
      positives: RDD[((Int, Int), Double)],
      ageSexBC: Broadcast[scala.collection.Map[Int, Profile]]) = {

    commonFriendsCounts
        .map(pair => (pair.person1, pair.person2) -> Vectors.dense(
          pair.commonFriendsCount,
          abs(ageSexBC.value.getOrElse(pair.person1, Profile(0, 0)).age - ageSexBC.value.getOrElse(pair.person2, Profile(0, 0)).age).toDouble,
          if (ageSexBC.value.getOrElse(pair.person1, Profile(0, 0)).sex == ageSexBC.value.getOrElse(pair.person2, Profile(0, 0)).sex) 1.0 else 0.0)
        )
        .leftOuterJoin(positives)
  }
}
