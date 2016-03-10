import org.apache.spark.mllib.classification.ClassificationModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

object Strategies {
  def getCombinedFriendshipCoef(combinedFType: Int): Double = {
    //FriendshipHelpers.getCoefForCombinedFriendship(combinedFType)
    1.0
  }

  def classificationModel(training: RDD[LabeledPoint]): ClassificationModel = {
    ModelHelpers.logisticRegressionModel(training)
  }
}
