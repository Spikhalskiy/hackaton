import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

object Strategies {
  def getCombinedFriendshipCoef(combinedFType: Int): Double = {
    FriendshipHelpers.getCoefForCombinedFriendship(combinedFType)
//    1.0
  }

  def classificationModel(training: RDD[LabeledPoint], sqlc: SQLContext): UnifiedClassifier = {
    ModelHelpers.decisionTreeModel(training, sqlc)
  }
}
