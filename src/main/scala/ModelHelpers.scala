import org.apache.spark.mllib.classification.ClassificationModel
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.tree.RandomForest
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object ModelHelpers {
  val NumPartitions = 120

  def logisticRegressionModel(training: RDD[LabeledPoint]) = {
    val model = new LogisticRegressionWithLBFGS()
        .setNumClasses(2)
        .run(training)
    model.clearThreshold()
    model
  }

  def randomForestModel(training: RDD[LabeledPoint]) = {
    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 50 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 20
    val maxBins = 50

    RandomForest.trainClassifier(training, numClasses, categoricalFeaturesInfo,
      numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
  }

  // step 8
  def buildPrediction(
      testData: RDD[((Int, Int), LabeledPoint)], model: ClassificationModel,
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
            val friendsWithRatings = t._2
            val topBestFriends = friendsWithRatings.toList.sortBy(-_._2).take(100).map(x => x._1)
            (user, topBestFriends)
          })
          .sortByKey(true, 1)
          .map(t => t._1 + "\t" + t._2.mkString("\t"))
    }

    testPrediction.saveAsTextFile(predictionPath,  classOf[GzipCodec])
  }
}
