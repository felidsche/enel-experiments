# Benchmark Jobs and Data Generation

* Environment
  
  - Spark: `v3.1.2`
  - Scala: `2.13.6`


* Usage
    1. GBT
    ```bash
    $SPARK_HOME/bin/spark-submit \
    --class de.tu_berlin.dos.arm.spark_utils.jobs.GradientBoostedTrees \
    --master local \
    --driver-java-options "-Dlog4j.configuration=file:src/main/resources/log4j.properties" \
    target/spark-checkpoint-workload-1.0-SNAPSHOT-jar-with-dependencies.jar \
    --iterations 20 --checkpoint 1 --checkpoint-interval 5 ../../../../samples/small.txt
    ```
    2. KMeans
    ```bash
    $SPARK_HOME/bin/spark-submit \
    --class de.tu_berlin.dos.arm.spark_utils.jobs.KMeans \
    --master local \
    --driver-java-options "-Dlog4j.configuration=file:/resources/log4j.properties" \
    target/runtime-adjustments-experiments-1.0-SNAPSHOT-jar-with-dependencies.jar \
    --k 3 --iterations 3 ../samples/KMeans.txt
    ```
    3. LDA
    ```bash
    $SPARK_HOME/bin/spark-submit \
    --class de.tu_berlin.dos.arm.spark_utils.jobs.LDAWorkload \
    --master local \
    --driver-java-options "-Dlog4j.configuration=file:src/main/resources/log4j.properties" \
    target/spark-checkpoint-workload-1.0-SNAPSHOT-jar-with-dependencies.jar \
    --k 3 --iterations 3 --checkpoint 1 --checkpoint-interval 1 ../../../../samples/wikipedia-corpus.txt ../../../../samples/stopwords.txt
    ```
    4. Analytics
    ```bash
    $SPARK_HOME/bin/spark-submit \
    --class de.tu_berlin.dos.arm.spark_utils.jobs.Analytics \
    --master local \
    --driver-java-options "-Dlog4j.configuration=file:src/main/resources/log4j.properties" \
    target/spark-checkpoint-workload-1.0-SNAPSHOT-jar-with-dependencies.jar \
    --sampling-fraction 0.01 --checkpoint 1 /Users/fschnei4/TUB_Master_ISM/SoSe21/MA/code/BigDataBench_V5.0_BigData_ComponentBenchmark/BigDataGeneratorSuite/Table_datagen/e-com/output/OS_ORDER_ITEM.txt /Users/fschnei4/TUB_Master_ISM/SoSe21/MA/code/BigDataBench_V5.0_BigData_ComponentBenchmark/BigDataGeneratorSuite/Table_datagen/e-com/output/OS_ORDER.txt
    ```

  
* Dataset Generator
- read more in `src/main/scala/datagens/README.md`