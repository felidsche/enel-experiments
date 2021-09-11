# Benchmark Jobs and Data Generation

* Environment
  
  - Spark: `v3.1.2`
  - Scala: `2.13.6`


* Usage
    0. Prerequisites
    ```bash
    mvn clean package
    $SPARK_HOME/sbin/start-history-server.sh
    mkdir /tmp/spark-events
    ```
    1. GBT
    ```bash
    $SPARK_HOME/bin/spark-submit \
    --class de.tu_berlin.dos.arm.spark_utils.jobs.GradientBoostedTrees \
    --master local \
    --driver-java-options "-Dlog4j.configuration=file:/Users/fschnei4/spark-3.1.2-bin-hadoop3.2/conf/log4j.properties" \
    target/runtime-adjustments-experiments-1.0-SNAPSHOT-jar-with-dependencies.jar \
    --iterations 1000 --checkpoint-interval 5 ../samples/GBT.txt
    ```
    2. KMeans
    ```bash
    $SPARK_HOME/bin/spark-submit \
    --class de.tu_berlin.dos.arm.spark_utils.jobs.KMeans \
    --master local \
    --driver-java-options "-Dlog4j.configuration=file:/Users/fschnei4/spark-3.1.2-bin-hadoop3.2/conf/log4j.properties" \
    target/runtime-adjustments-experiments-1.0-SNAPSHOT-jar-with-dependencies.jar \
    --k 3 --iterations 3 ../samples/KMeans.txt
    ```
    3. LDA
    ```bash
    $SPARK_HOME/bin/spark-submit \
    --class de.tu_berlin.dos.arm.spark_utils.jobs.LDAWorkload \
    --master local \
    --driver-java-options "-Dlog4j.configuration=file:/Users/fschnei4/spark-3.1.2-bin-hadoop3.2/conf/log4j.properties" \
    target/runtime-adjustments-experiments-1.0-SNAPSHOT-jar-with-dependencies.jar \
    --k 10 ../samples/wikipedia-corpus.txt ../samples/stopwords.txt
    ```

  
* Dataset Generator
  
  1. KMeans // TODO

* Workload
  
  Workloads are located in `de.tu_berlin.dos.arm.spark_utils.jobs`.