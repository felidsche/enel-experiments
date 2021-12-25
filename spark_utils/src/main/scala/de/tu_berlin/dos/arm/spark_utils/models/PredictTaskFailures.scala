package de.tu_berlin.dos.arm.spark_utils.models

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Imputer, IndexToString, OneHotEncoder, StringIndexer, Tokenizer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

/**
 * An ML Pipeline for binary task failure classification
 * Uses Nested CV: Each fold is split into two folds:
 * one fold for Pipeline Selection CV
 * one fold for Pipeline Evaluation CV
 */
object PredictTaskFailures {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("PredictTaskFailures")
      .master("local[2]")
      .getOrCreate()

    // load cleaned training data from disk
    var training = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true")).csv(
      //"/home/felix/TUB_Master_ISM/SoSe21/MA/acs-simulation/data/alibaba_clusterdata_v2018",
      "/home/felix/TUB_Master_ISM/SoSe21/MA/acs-simulation/data/alibaba_clusterdata_v2018/batch_task_clean_1F_001S/*.csv.gz"
    )

    /*
     *Configure an ML pipeline, which consists of
     * data preprocesssors (transformers): StringIndexer (str cols), OneHotEncoder (binary cols), Imputer (numeric cols), VectorAssembler, Normalizer
     * feature preprocessors (transforners):
     * binary classifier (estimator): GBT/RandomForest
     */

    // drop columns that are irrelevant for model training
    training = training.drop(
      "task_name", "job_name", "start_time", "end_time", "earliest", "logical_job_name",
      "latest", "status", "mtts_task", "ttr_task", "tts_task", "ttf_task", "reduce_checkpoint", "second_quant_checkpoint", "third_quant_checkpoint"
    )

    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")

    val strIndexer = new StringIndexer()
      .setInputCol("map_reduce")
      .setOutputCol("mr_indexed")

    val oneHotEncoder = new OneHotEncoder()
      .setInputCol(strIndexer.getOutputCol)
      .setOutputCol(strIndexer.getOutputCol + "_onehot")

    // for comprehension to filter for IntegerType
    val numericCols: Array[String] = for (i <- training.dtypes if i._2 == "IntegerType") yield i._1
    // for comprehension to add suffix to all imputed cols
    val imputedCols: Array[String] = for (j <- numericCols) yield j + "_imp"

    val imputer = new Imputer()
      .setInputCols(numericCols)
      .setOutputCols(imputedCols)

    // append the encoded col to the imputed cols to get the feature cols
    val features: Array[String] = imputer.getOutputCols :+ oneHotEncoder.getOutputCol

    val assembler = new VectorAssembler()
      .setInputCols(features)
      .setOutputCol("features")

    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    val featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)


    // Train a GBT model.
    val gbt = new GBTClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setFeatureSubsetStrategy("auto")
      .setMinInstancesPerNode(50) // a small value because of imbalanced classes
      .setSubsamplingRate(0.8)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")

    val pipeline = new Pipeline()
      .setStages(Array(labelIndexer, strIndexer, oneHotEncoder, imputer, assembler, featureIndexer, gbt, labelConverter))

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // With 3 values for gbt.maxIter and 2 values for gbt.maxDepth,
    // this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
    // https://www.analyticsvidhya.com/blog/2016/02/complete-guide-parameter-tuning-gradient-boosting-gbm-python/
    val paramGrid = new ParamGridBuilder()
      .addGrid(gbt.maxIter, Array(10, 100, 250))  // for each iteration one decision tree is added to the ensemble
      .addGrid(gbt.maxDepth, Array(5, 8))
      .build()

    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    // This will allow us to jointly choose parameters for all Pipeline stages.
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    // Note that the evaluator here is a BinaryClassificationEvaluator and its metric is F1-score

    val evaluator = new BinaryClassificationEvaluator()
      .setMetricName("f1")
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(2) // Use 3+ in practice
      .setParallelism(2) // Evaluate up to 2 parameter settings in parallel

    // Run cross-validation, and choose the best set of parameters.
    val cvModel = cv.fit(training)

    // Prepare test documents, which are unlabeled (id, text) tuples.
    val test = spark.createDataFrame(Seq(
      (4L, "spark i j k"),
      (5L, "l m n"),
      (6L, "mapreduce spark"),
      (7L, "apache hadoop")
    )).toDF("id", "text")

    // Make predictions on test documents. cvModel uses the best model found (lrModel).
    cvModel.transform(test)
      .select("id", "text", "probability", "prediction")
      .collect()
      .foreach { case Row(id: Long, text: String, prob: Vector, prediction: Double) =>
        println(s"($id, $text) --> prob=$prob, prediction=$prediction")
      }

    spark.stop()
  }
}