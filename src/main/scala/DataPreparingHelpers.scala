import java.util.Date
import java.util.concurrent.TimeUnit

import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.math._

object DataPreparingHelpers {
  val FEB_1_2016 = new Date(2016, 2, 1).getTime

  // step 4
  def prepareAgeSexBroadcast(sc: SparkContext, demographyPath: String) = {
    val ageSex =
      sc.textFile(demographyPath)
          .map(line => {
            val lineSplit = line.trim().split("\t")
            defaultInt(lineSplit, 0) -> Profile(defaultLong(lineSplit, 1), defaultInt(lineSplit, 2), defaultInt(lineSplit, 3),
              defaultLong(lineSplit, 4), defaultLong(lineSplit, 5), defaultLong(lineSplit, 6))
          })
    sc.broadcast(ageSex.collectAsMap())
  }

  def generatePairs(pplWithCommonFriends: Seq[OneWayFriendship], numPartitions: Int, k: Int) = {
    val pairs = ArrayBuffer.empty[MiddleFriendship]
    for (i <- pplWithCommonFriends.indices) {
      val f1 = pplWithCommonFriends(i)
      if (f1.anotherUser % numPartitions == k) {
        for (j <- i + 1 until pplWithCommonFriends.length) {
          val f2 = pplWithCommonFriends(j)
          pairs.append(MiddleFriendship(f1.anotherUser, f2.anotherUser,
            pplWithCommonFriends.length, FriendshipHelpers.combineFriendshipTypesToGranulatedFType(f1.fType, f2.fType)))
        }
      }
    }
    pairs
  }

  def prepareAdamicAdarData(
      commonFriendsCounts: RDD[PairWithCommonFriends],
      positives: RDD[((Int, Int), Double)],
      ageSexBC: Broadcast[scala.collection.Map[Int, Profile]]): RDD[((Int, Int), (Vector, Option[Double]))] = {

    commonFriendsCounts
        .map(pair => (pair.person1, pair.person2) -> Vectors.dense(pair.commonFriendsCount))
        .leftOuterJoin(positives)
  }

  def prepareUserData(
      commonFriendsCounts: RDD[PairWithCommonFriends],
      positives: RDD[((Int, Int), Double)],
      ageSexBC: Broadcast[scala.collection.Map[Int, Profile]]): RDD[((Int, Int), (Vector, Option[Double]))] = {

    commonFriendsCounts
        .map(pair => (pair.person1, pair.person2) -> {
          val user1 = ageSexBC.value.getOrElse(pair.person1, Profile(0, 0, 0, 0, 0 , 0))
          val user2 = ageSexBC.value.getOrElse(pair.person2, Profile(0, 0, 0, 0 ,0 , 0))
          val fType = pair.fAccumulator
          Vectors.dense(
            ageToYears(user1.age),
            (ageToYears(user1.age) - ageToYears(user2.age)).toDouble,
            combineSex(user1.sex, user2.sex),
            profileCreationDatesAreVeryClose(user1.create_date, user2.create_date),
            min(logProfileAge(user1.create_date), logProfileAge(user2.create_date)),

            if (user1.country != 0 && user2.country != 0) if (user1.country == user2.country) 1.0 else 0.0 else -1.0,
            if (user1.location != 0 && user2.location != 0) if (user1.location == user2.location) 1.0 else 0.0 else -1.0,
            if (user1.loginRegion != 0 && user2.loginRegion != 0) if (user1.loginRegion == user2.loginRegion) 1.0 else 0.0 else -1.0
          )
        }
        )
        .leftOuterJoin(positives)
  }

  def prepareFriendsTypeData(
      commonFriendsCounts: RDD[PairWithCommonFriends],
      positives: RDD[((Int, Int), Double)],
      ageSexBC: Broadcast[scala.collection.Map[Int, Profile]]): RDD[((Int, Int), (Vector, Option[Double]))] = {

    commonFriendsCounts
        .map(pair => (pair.person1, pair.person2) -> {
          val fAcc = pair.fAccumulator
          Vectors.dense(
            FriendshipHelpers.getGranulatedFTypeCountFromAccumulator(fAcc, FriendshipHelpers.CLOSE_FRIENDS_COUNT_MASK, FriendshipHelpers.CLOSE_FRIENDS_COUNT_OFFSET).toDouble,
            FriendshipHelpers.getGranulatedFTypeCountFromAccumulator(fAcc, FriendshipHelpers.FAMILY_COUNT_MASK, FriendshipHelpers.FAMILY_COUNT_OFFSET).toDouble,
            FriendshipHelpers.getGranulatedFTypeCountFromAccumulator(fAcc, FriendshipHelpers.CLOSE_FAMILY_COUNT_MASK, FriendshipHelpers.CLOSE_FAMILY_COUNT_OFFSET).toDouble,
            FriendshipHelpers.getGranulatedFTypeCountFromAccumulator(fAcc, FriendshipHelpers.SCHOOL_COUNT_MASK, FriendshipHelpers.SCHOOL_COUNT_OFFSET).toDouble,
            FriendshipHelpers.getGranulatedFTypeCountFromAccumulator(fAcc, FriendshipHelpers.WORK_COUNT_MASK, FriendshipHelpers.WORK_COUNT_OFFSET).toDouble,
            FriendshipHelpers.getGranulatedFTypeCountFromAccumulator(fAcc, FriendshipHelpers.UNIVERSITY_COUNT_MASK, FriendshipHelpers.UNIVERSITY_COUNT_OFFSET).toDouble,
            FriendshipHelpers.getGranulatedFTypeCountFromAccumulator(fAcc, FriendshipHelpers.ARMY_COUNT_MASK, FriendshipHelpers.ARMY_COUNT_OFFSET).toDouble,
            FriendshipHelpers.getGranulatedFTypeCountFromAccumulator(fAcc, FriendshipHelpers.PLAY_COUNT_MASK, FriendshipHelpers.PLAY_COUNT_OFFSET).toDouble
          )
        }
        )
        .leftOuterJoin(positives)
  }

  def combineSex(sex1: Int, sex2: Int): Double = {
    if (sex1 == 0 || sex2 == 0) {
      -1.0
    } else if (sex1 == sex2) {
      if (sex1 == 1) 1.0 else 2.0
    } else {
      if (sex1 == 1) 3.0 else 4.0
    }
  }

  def ageToYears(ageInDay: Int): Int = {
    ageInDay / 365
  }

  def defaultInt(strArray: Array[String], index: Int): Int = {
    if (strArray.length <= index) {
      0
    } else {
      val str = strArray(index)
      if (str == null || str.isEmpty) 0 else str.toInt
    }
  }

  def defaultLong(strArray: Array[String], index: Int): Long = {
    if (strArray.length <= index) {
      0L
    } else {
      val str = strArray(index)
      if (str == null || str.isEmpty) 0L else str.toLong
    }
  }

  def profileCreationDatesAreVeryClose(creationDate1: Long, creationDate2: Long): Double = {
    if (abs(creationDate1 - creationDate2) <= TimeUnit.DAYS.toMillis(7)) 1.0 else 0.0
  }

  def logProfileAge(creationDate: Long): Double = {
    log(FEB_1_2016 - creationDate)
  }
}
