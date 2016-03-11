import breeze.numerics._
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer

object DataPreparingHelpers {

  // step 4
  def prepareAgeSexBroadcast(sc: SparkContext, demographyPath: String) = {
    val ageSex =
      sc.textFile(demographyPath)
          .map(line => {
            val lineSplit = line.trim().split("\t")

            defaultInt(lineSplit, 0) -> Profile(defaultInt(lineSplit, 2), defaultInt(lineSplit, 3),
              defaultLong(lineSplit, 4), defaultLong(lineSplit, 5), defaultLong(lineSplit, 6))
          })
    sc.broadcast(ageSex.collectAsMap())
  }

  def generatePairs(pplWithCommonFriends: Seq[OneWayFriendship], numPartitions: Int, k: Int) = {
    val pairs = ArrayBuffer.empty[Friendship]
    for (i <- pplWithCommonFriends.indices) {
      val f1 = pplWithCommonFriends(i)
      if (f1.anotherUser % numPartitions == k) {
        for (j <- i + 1 until pplWithCommonFriends.length) {
          val f2 = pplWithCommonFriends(j)
          pairs.append(Friendship(f1.anotherUser, f2.anotherUser,
            pplWithCommonFriends.length, FriendshipHelpers.combineFriendshipTypesToMask(f1.fType, f2.fType)))
        }
      }
    }
    pairs
  }

  val FAMILY = Integer.parseInt(    "1", 2)
  val FRIENDS = Integer.parseInt(   "10", 2)
  val SCHOOL = Integer.parseInt(    "100", 2)
  val WORK = Integer.parseInt(      "1000", 2)
  val UNIVERSITY = Integer.parseInt("10000", 2)
  val ARMY = Integer.parseInt(      "100000", 2)
  val PLAY = Integer.parseInt(      "1000000", 2)

  def prepareData(
      commonFriendsCounts: RDD[PairWithCommonFriends],
      positives: RDD[((Int, Int), Double)],
      ageSexBC: Broadcast[scala.collection.Map[Int, Profile]]) = {

    commonFriendsCounts
        .map(pair => (pair.person1, pair.person2) -> {
          val user1 = ageSexBC.value.getOrElse(pair.person1, Profile(0, 0, 0, 0 , 0))
          val user2 = ageSexBC.value.getOrElse(pair.person2, Profile(0, 0, 0 ,0 , 0))
          val fType = pair.combinedFType
          Vectors.dense(
            pair.commonFriendsCount,
            abs(user1.age - user2.age).toDouble,
            if (user1.sex != 0 && user2.sex != 0) if (user1.sex == user2.sex) user2.sex.toDouble else 0.0 else -1.0,
            if (user1.country != 0 && user2.country != 0) if (user1.country == user2.country) 1.0 else 0.0 else -1.0,
            if (user1.location != 0 && user2.location != 0) if (user1.location == user2.location) 1.0 else 0.0 else -1.0,
            if (user1.loginRegion != 0 && user2.loginRegion != 0) if (user1.loginRegion == user2.loginRegion) 1.0 else 0.0 else -1.0,
            if (user1.location != 0 && user1.location == user2.loginRegion ||
                user2.location != 0 && user2.location == user1.loginRegion) 1.0 else 0.0,
            FriendshipHelpers.combinedFTypeContains(fType, FriendshipHelpers.FAMILY),
            FriendshipHelpers.combinedFTypeContains(fType, FriendshipHelpers.FRIENDS),
            FriendshipHelpers.combinedFTypeContains(fType, FriendshipHelpers.SCHOOL),
            FriendshipHelpers.combinedFTypeContains(fType, FriendshipHelpers.WORK),
            FriendshipHelpers.combinedFTypeContains(fType, FriendshipHelpers.UNIVERSITY),
            FriendshipHelpers.combinedFTypeContains(fType, FriendshipHelpers.ARMY),
            FriendshipHelpers.combinedFTypeContains(fType, FriendshipHelpers.PLAY)
          )
        }
        )
        .leftOuterJoin(positives)
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
}
