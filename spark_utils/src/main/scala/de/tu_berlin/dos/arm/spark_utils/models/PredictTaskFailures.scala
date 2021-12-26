package de.tu_berlin.dos.arm.spark_utils.models

import org.apache.log4j.Logger
import Array._
import org.apache.spark.ml.{Estimator, Pipeline, PipelineModel}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Imputer, IndexToString, MinMaxScaler, OneHotEncoder, StringIndexer, Tokenizer, VectorAssembler, VectorIndexer}
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

    val logger = Logger.getRootLogger

    val seed = 77

    val spark = SparkSession
      .builder
      .appName("PredictTaskFailures")
      .master("local[2]")
      .getOrCreate()

    // load cleaned training data from disk
    var data = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true")).csv(
      //"/home/felix/TUB_Master_ISM/SoSe21/MA/acs-simulation/data/alibaba_clusterdata_v2018/batch_jobs_clean",
      "/home/felix/TUB_Master_ISM/SoSe21/MA/acs-simulation/data/alibaba_clusterdata_v2018/batch_jobs_clean_1perc_sample/*.csv.gz"
    )



    /*
     *Configure an ML pipeline, which consists of
     * data preprocesssors (transformers): StringIndexer (str cols), OneHotEncoder (binary cols), Imputer (numeric cols), VectorAssembler, Normalizer
     * feature preprocessors (transforners):
     * binary classifier (estimator): GBT/RandomForest
     */

    // drop columns that are irrelevant for model training
    data = data.drop(
      "task_name", "job_name", "start_time", "end_time", "earliest", "logical_job_name",
      "latest", "status", "mtts_task", "ttr_task", "tts_task", "ttf_task", "reduce_checkpoint", "second_quant_checkpoint", "third_quant_checkpoint"
    )

    var Array(training, testing, validation) = data.randomSplit(Array(0.5, 0.25, 0.25), seed)

    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    val labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")

    val strIndexer = new StringIndexer()
      .setInputCol("map_reduce")
      .setOutputCol("mrIndexed")

    val oneHotEncoder = new OneHotEncoder()
      .setInputCol(strIndexer.getOutputCol)
      .setOutputCol(strIndexer.getOutputCol + "OneHot")

    val numericTypes = Array[String]("IntegerType", "FloatType", "DoubleType")
    // for comprehension to filter for numeric columns
    val numericCols: Array[String] = for (i <- data.dtypes if numericTypes.contains(i._2)) yield i._1
    // for comprehension to add suffix to all imputed cols
    val imputedCols: Array[String] = for (j <- numericCols) yield j + "_imp"

    val imputer = new Imputer()
      .setInputCols(numericCols)
      .setOutputCols(imputedCols)

    val cpuCols: Array[String] = Array("plan_cpu", "cpu_avg", "cpu_max")

    val cpuColsAssembler = new VectorAssembler()
      .setInputCols(cpuCols)
      .setOutputCol("CpuColsVec")

    val scaler = new MinMaxScaler()
      .setInputCol(cpuColsAssembler.getOutputCol)
      .setOutputCol(cpuColsAssembler.getOutputCol + "Scaled")

    // concat the outputs of the transformers
    val features: Array[String] = concat(imputer.getOutputCols, Array(scaler.getOutputCol, oneHotEncoder.getOutputCol))

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
      .setStages(Array(labelIndexer, strIndexer, oneHotEncoder, imputer, scaler, assembler, featureIndexer, gbt, labelConverter))

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // With 3 values for gbt.maxIter and 2 values for gbt.maxDepth,
    // this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
    // https://www.analyticsvidhya.com/blog/2016/02/complete-guide-parameter-tuning-gradient-boosting-gbm-python/
    val paramGrid = new ParamGridBuilder()
      .addGrid(gbt.maxIter, Array(10, 100, 250)) // for each iteration one decision tree is added to the ensemble
      .addGrid(gbt.maxDepth, Array(5, 8))
      .build()

    // choose cross-validation techniques for the inner and outer loops, independently of the dataset.
    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    // This will allow us to jointly choose parameters for all Pipeline stages.
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    // Note that the evaluator here is a BinaryClassificationEvaluator and its metric is areaUnderROC
    val folds = 5 // 5 or 10 are common values
    val parallelism = 2 // Evaluate up to X parameter settings in parallel

    val innerCv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(folds)
      .setParallelism(parallelism)

    // Run inner CV and choose the best set of parameters.
    val innerCvModel = innerCv.fit(training)
    val modelPath = "/home/felix/TUB_Master_ISM/SoSe21/MA/msc-thesis-saft-experiments/output/models/"
    innerCvModel.save(modelPath)

    // log the metrics for all parameter combinations
    val innerCvAvgMetrics = innerCvModel.avgMetrics
    logger.info("Model selection metrics: \n")
    logger.info(innerCvAvgMetrics)

    // Get the best selected pipeline model
    val bestModel = innerCvModel.bestModel
    // we use an empty parameter grid to use CrossValidator for Model evaluation
    val emptyParamGrid = new ParamGridBuilder().build()

    // create another pipeline to do a second CV
    val evalPipeline = new Pipeline().setStages(Array(bestModel))
    /*
     * areaUnderPR is more informative if there is a huge class imbalance: http://pages.cs.wisc.edu/~jdavis/davisgoadrichcamera2.pdf
     */
    val evaluator = new BinaryClassificationEvaluator().setMetricName("areaUnderPR")

    val outerCv = new CrossValidator()
      .setEstimator(evalPipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(emptyParamGrid)
      .setNumFolds(folds)
      .setParallelism(parallelism)

    // Run outer CV to evalute the bestModel on unseen testing data.
    val outerCvModel = outerCv.fit(testing)

    // log the metrics
    val outerCvAvgMetrics = outerCvModel.avgMetrics
    logger.info("Model evaluation metrics: \n")
    logger.info(outerCvAvgMetrics)

    /*
     * total no. of models trained:
     * Model selection: 5 * ( 3 * 2) = 30
     * Model evaluation: 5
     */

    validation = outerCvModel.bestModel.transform(validation)

    logger.info("Model validation: \n")
    logger.info(validation.count())

    validation.repartition(2).write.options(Map("compression" -> "gzip", "delimiter" -> ",", "header" -> "true")).csv(
      "/home/felix/TUB_Master_ISM/SoSe21/MA/msc-thesis-saft-experiments/output/eval/PredictTaskFailure"
    )

    spark.stop()
  }
}