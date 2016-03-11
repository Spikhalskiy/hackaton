import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * @author Dmitry Spikhalskiy <dspikhalskiy@pulsepoint.com>
  */
trait UnifiedClassifier {
  def predict[T](data: DataFrame): RDD[(T, Double)]
}
