package de.tu_berlin.dos.arm.spark_utils.models

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.GBTClassifier
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.tuning.{CrossValidator, CrossValidatorModel, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

import java.io.File
import scala.Array._

/**
 * An ML Pipeline for binary task failure classification
 * Uses Nested CV: Each fold is split into two folds:
 * one fold for Pipeline Selection CV
 * one fold for Pipeline Evaluation CV
 */
object GBTNestedCVTaskFailBinPred {

  def main(args: Array[String]): Unit = {

    val logger = Logger.getRootLogger

    val seed = 77
    val conf = new SparkConf().set("spark.driver.memory", "2g")
    val spark = SparkSession
      .builder
      .appName("PredictTaskFailures")
      .master("local[2]")
      .config(conf)
      .getOrCreate()

    // load cleaned training data from disk
    var data = spark.read.options(Map("inferSchema" -> "true", "delimiter" -> ",", "header" -> "true")).csv(
      //"/home/felix/TUB_Master_ISM/SoSe21/MA/acs-simulation/data/alibaba_clusterdata_v2018/batch_jobs_clean",
      "/home/felix/TUB_Master_ISM/SoSe21/MA/acs-simulation/data/" +
        "alibaba_clusterdata_v2018/batch_jobs_clean_001inst_01task_05S_1F/*.csv.gz"
    ).limit(10000)

    /*
     *Configure an ML pipeline, which consists of
     * data/feature preprocesssors (transformers): StringIndexer (str cols), OneHotEncoder (binary cols), Imputer (numeric cols), VectorAssembler, Normalizer
     * binary classifier (estimator): GBT/RandomForest
     */


    var Array(training, testing, validation) = data.randomSplit(Array(0.5, 0.25, 0.25), seed)

    /*
     * drop columns that are irrelevant for model training
     * e.g. because of: little intuitive impact, too many categories
     */
    training = training.drop(
      "task_name", "job_name", "instance_name", "instance_status", "start_time", "end_time", "earliest", "logical_job_name",
      "latest", "status", "mtts_task", "ttr_task", "tts_task", "ttf_task", "reduce_checkpoint",
      "second_quant_checkpoint", "third_quant_checkpoint", "machine_id"
    )

    // OneHotEncode "m", "r" values
    val strIndexer = new StringIndexer()
      .setInputCol("map_reduce")
      .setOutputCol("mrIndexed")

    val oneHotEncoder = new OneHotEncoder()
      .setInputCol(strIndexer.getOutputCol)
      .setOutputCol(strIndexer.getOutputCol + "OneHot")

    // for comprehension to filter for feature types
    val numericTypes = Array[String]("IntegerType", "FloatType", "DoubleType")
    val numericCols: Array[String] = for (i <- training.dtypes if (numericTypes.contains(i._2)) && (i._1 != "labels")) yield i._1

    // for comprehension to add suffix to all imputed cols
    val imputedCols: Array[String] = for (j <- numericCols) yield j + "_imp"

    val imputer = new Imputer()
      .setInputCols(numericCols)
      .setOutputCols(imputedCols)

    val cpuCols: Array[String] = Array("plan_cpu_imp", "cpu_avg_imp", "cpu_max_imp")

    val cpuColsAssembler = new VectorAssembler()
      .setInputCols(cpuCols)
      .setOutputCol("CpuColsVec")

    // CPU cols are not scaled yet as opposed to MEM cols
    val scaler = new MinMaxScaler()
      .setMin(0.0)
      .setMax(1.0)
      .setInputCol(cpuColsAssembler.getOutputCol)
      .setOutputCol(cpuColsAssembler.getOutputCol + "Scaled")

    // concat the outputs of the transformers
    val features: Array[String] = concat(imputer.getOutputCols, Array(scaler.getOutputCol, oneHotEncoder.getOutputCol))

    val assembler = new VectorAssembler()
      .setInputCols(features)
      .setOutputCol("features")

    // Define a Baseline GBT Classifier
    val gbt = new GBTClassifier()
      .setLabelCol("labels")
      .setFeaturesCol("features")
      .setFeatureSubsetStrategy("auto")
      .setSubsamplingRate(0.8) // Typical values ~0.8 generally work fine but can be fine-tuned further.
      .setStepSize(0.1) // (default) learning-rate in sk-learn -> lower to get more robust models
      .setSeed(seed)

    var pipeline = new Pipeline()
      .setStages(Array(strIndexer, oneHotEncoder, imputer, cpuColsAssembler, scaler, assembler, gbt))
    // concat the inputs of the transformers to drop them since they are not needed anymore
    val preprocInputs: Array[String] = {
      concat(imputer.getInputCols, Array(scaler.getInputCol, strIndexer.getInputCol, oneHotEncoder.getInputCol))
    }
    // training = training.drop(preprocInputs: _*)

    logger.info("========== MODEL SELECTION ==========")


    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // this grid will have {numParameters} x {numOptions} parameter settings for CrossValidator to choose from.
    // https://www.analyticsvidhya.com/blog/2016/02/complete-guide-parameter-tuning-gradient-boosting-gbm-python/
    var paramGrid = new ParamGridBuilder()
      .addGrid(gbt.maxDepth, range(2, 11, 5)) // important -> tune first
      .addGrid(gbt.maxBins, Array(12, 20, 32)) // must be at least 2 and at lest max number of categories(features)
      .addGrid(gbt.minInstancesPerNode, Array(5, 10, 20)) // a small value because of imbalanced classes
      .addGrid(gbt.minInfoGain, Array(0.0, 0.1, 0.2)) // similar to min_samples_split in sk-learn
      .addGrid(gbt.maxIter, range(20, 101, 5)) // for each iteration one decision tree is added to the ensemble
      .build()

    // smaller param space
    paramGrid = new ParamGridBuilder()
      .addGrid(gbt.maxDepth, range(2, 10)) // important -> tune first
      .addGrid(gbt.maxBins, Array(12, 20)) // must be at least 2 and at lest max number of categories(features)
      //addGrid(gbt.minInstancesPerNode, Array(5, 10)) // a small value because of imbalanced classes
      //.addGrid(gbt.minInfoGain, Array(0.0, 0.1)) // similar to min_samples_split in sk-learn
      //.addGrid(gbt.maxIter, range(20, 70)) // for each iteration one decision tree is added to the ensemble
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
      .setEvaluator(new BinaryClassificationEvaluator().setLabelCol("labels"))
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(folds)
      .setParallelism(parallelism)

    logger.info {
      "The training data: \n" + training.dtypes.mkString("Array(", ", ", ")" + "\n"
        + training.show(10, truncate = false))
    }

    val innerCVPath = "/home/felix/TUB_Master_ISM/SoSe21/MA/msc-thesis-saft-experiments/output/models/GBTInnerCV"

    val innerCvModel: CrossValidatorModel = loadOrTrainAndSaveCV(logger, training, innerCv, innerCVPath)


    // log the metrics for all parameter combinations
    val innerCvAvgMetrics: Array[Double] = innerCvModel.avgMetrics
    logger.info("Model selection metrics: \n")
    logger.info(innerCvAvgMetrics)

    logger.info("========== MODEL EVALUATION ==========")

    // Get the best selected pipeline model
    val bestModel = innerCvModel.bestModel.asInstanceOf[PipelineModel]

    // we use an empty parameter grid to use CrossValidator for Model evaluation
    val emptyParamGrid = new ParamGridBuilder().build()

    // change the classifier stage of the existing pipeline to do a second CV
    pipeline.setStages(Array(bestModel))

    // areaUnderPR is more informative if there is a huge class imbalance: http://pages.cs.wisc.edu/~jdavis/davisgoadrichcamera2.pdf
    val evaluator = new BinaryClassificationEvaluator().setMetricName("areaUnderPR").setLabelCol("labels")

    val outerCv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(emptyParamGrid)
      .setNumFolds(folds)
    //.setParallelism(parallelism)

    // Run outer CV to evalute the bestModel on unseen testing data.
    val outerCvPath = "/home/felix/TUB_Master_ISM/SoSe21/MA/msc-thesis-saft-experiments/output/models/GBTOuterCV"
    val outerCvModel = loadOrTrainAndSaveCV(logger, testing, outerCv, outerCvPath)


    // log the metrics
    val outerCvAvgMetrics: Array[Double] = outerCvModel.avgMetrics
    logger.info("Model evaluation metrics: \n")
    logger.info(outerCvAvgMetrics)

    // select all initial columns and the predictions column
    val validationCols = data.columns :+ "prediction"
    validation = outerCvModel.bestModel.transform(validation)
    validation = validation.select(
      validationCols.map(col): _*
    )

    validation.repartition(2).write.mode(SaveMode.Overwrite).options(Map("compression" -> "gzip", "delimiter" -> ",", "header" -> "true")).csv(
      "/home/felix/TUB_Master_ISM/SoSe21/MA/msc-thesis-saft-experiments/output/eval/PredictTaskFailure"
    )

    spark.stop()
  }

  private def loadOrTrainAndSaveCV(logger: Logger, dataset: Dataset[Row], cv: CrossValidator, modelPath: String): CrossValidatorModel = {
    /*
     * if the model path exists, the model is returned. Else the model is trained and saved
     */
    val modelExists: Boolean = new File(modelPath).exists

    val cvModel = {
      if (modelExists) CrossValidatorModel.load(modelPath) else cv.fit(dataset)
    }

    if (modelExists) {
      logger.info("CV model was loaded from disk")
    } else {
      logger.info("Run CV and choose the best set of parameters.")
      cvModel.write.overwrite().save(modelPath)
    }
    cvModel
  }
}