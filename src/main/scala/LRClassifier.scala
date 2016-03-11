import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.GeneralizedLinearModel
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

class LRClassifier(val model: GeneralizedLinearModel) extends UnifiedClassifier with Serializable {
  override def predict[T](data: DataFrame): RDD[(T, Double)] = {
    data.map(row => {
      val prob = model.predict(row.getAs[Vector](DataFrameColumns.FEATURES))
      (row.getAs[T](DataFrameColumns.KEY), prob)
    })
  }
}
