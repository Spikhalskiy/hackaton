import org.apache.spark.ml.PipelineModel
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

class PipelineClassifier(val pipeline: PipelineModel) extends UnifiedClassifier with Serializable {
  override def predict[T](data: DataFrame): RDD[(T, Double)] = {
    val singletonDF = ModelHelpers.addMetadata(data)
    val predictions = pipeline.transform(singletonDF)
    predictions.map(row => {
      val firstClass = row.getAs[DenseVector](DataFrameColumns.RAW_PREDICTION)(1)
      val zeroClass = row.getAs[DenseVector](DataFrameColumns.RAW_PREDICTION)(0)
      val prob = firstClass.toDouble / (firstClass.toDouble + zeroClass.toDouble)
      (row.getAs[T](DataFrameColumns.KEY), prob)
    })
  }
}
