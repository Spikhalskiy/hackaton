import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.ml.attribute.NominalAttribute
import org.slf4j.LoggerFactory

object ModelHelpers {
  val Log = LoggerFactory.getLogger(Baseline.getClass)

  val NumPartitions = 50

  def logisticRegressionModel(training: RDD[LabeledPoint]): UnifiedClassifier = {
    val model = new LogisticRegressionWithLBFGS()
        .setNumClasses(2)
        .run(training)
    model.clearThreshold()

    new LRClassifier(model)
  }

  def decisionTreeModel(training: RDD[LabeledPoint], sqlc: SQLContext): UnifiedClassifier = {
    import sqlc.implicits._
    val trainingDF = addMetadata(training.toDF())

    // Automatically identify categorical features, and index them.
    val featureIndexer = new VectorIndexer()
        .setInputCol(DataFrameColumns.FEATURES)
        .setOutputCol("indexedFeatures")
        .setMaxCategories(8)
        .fit(trainingDF)

    val dt = new DecisionTreeClassifier()
        .setLabelCol(DataFrameColumns.LABEL)
        .setFeaturesCol("indexedFeatures")

        .setImpurity("gini")
        .setMaxBins(64)

        .setMaxDepth(30)
        .setMinInstancesPerNode(50)


    // Chain indexers and tree in a Pipeline
    val pipeline = new Pipeline()
        .setStages(Array(featureIndexer, dt))

    // Train model.  This also runs the indexers.
    val model = pipeline.fit(trainingDF)

    new PipelineClassifier(model)
  }

  def addMetadata(df: DataFrame) = {
    val meta = NominalAttribute
        .defaultAttr
        .withName(DataFrameColumns.LABEL)
        .withValues("0.0", "1.0")
        .toMetadata

    df.withColumn(DataFrameColumns.LABEL, df.col(DataFrameColumns.LABEL).as(DataFrameColumns.LABEL, meta))
  }
}
