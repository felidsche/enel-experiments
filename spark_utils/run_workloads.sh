# run Batch workload without and with checkpoint

for checkpoint in 0 1
do
  # Analytics of Big Data Bench e-com data
  $SPARK_HOME/bin/spark-submit \
    --class de.tu_berlin.dos.arm.spark_utils.jobs.Analytics \
    --master local \
    --driver-java-options "-Dlog4j.configuration=file:/Users/fschnei4/spark-3.1.2-bin-hadoop3.2/conf/log4j.properties" \
    spark_utils/target/spark-checkpoint-workloads-1.0-SNAPSHOT-jar-with-dependencies.jar \
    --sampling-fraction 0.0001 --checkpoint-rdd "$checkpoint" /Users/fschnei4/TUB_Master_ISM/SoSe21/MA/code/BigDataBench_V5.0_BigData_ComponentBenchmark/BigDataGeneratorSuite/Table_datagen/e-com/output/OS_ORDER_ITEM.txt /Users/fschnei4/TUB_Master_ISM/SoSe21/MA/code/BigDataBench_V5.0_BigData_ComponentBenchmark/BigDataGeneratorSuite/Table_datagen/e-com/output/OS_ORDER.txt

  # wait for the history server
  sleep 10

  # get .csv of Batch workload run
  python3 experiments_service/src/experiment_metrics.py $checkpoint

  # delete the app.log for the next run
  rm /Users/fschnei4/spark-3.1.2-bin-hadoop3.2/app-logs/app.log

  # GBT regression on generated data
  $SPARK_HOME/bin/spark-submit \
    --class de.tu_berlin.dos.arm.spark_utils.jobs.GradientBoostedTrees \
    --master local \
    --driver-java-options "-Dlog4j.configuration=file:/Users/fschnei4/spark-3.1.2-bin-hadoop3.2/conf/log4j.properties" \
    spark_utils/target/spark-checkpoint-workloads-1.0-SNAPSHOT-jar-with-dependencies.jar \
    --iterations 10 --checkpoint "$checkpoint" --checkpoint-interval 2 samples/GBT.txt

  # wait for the history server
  sleep 10

  # get .csv of Batch workload run
  python3 experiments_service/src/experiment_metrics.py $checkpoint

  # delete the app.log for the next run
  rm /Users/fschnei4/spark-3.1.2-bin-hadoop3.2/app-logs/app.log

  # LDA clustering of Big Data Bench Wikipedia corpus
  $SPARK_HOME/bin/spark-submit \
    --class de.tu_berlin.dos.arm.spark_utils.jobs.LDAWorkload \
    --master local \
    --driver-java-options "-Dlog4j.configuration=file:/Users/fschnei4/spark-3.1.2-bin-hadoop3.2/conf/log4j.properties" \
    spark_utils/target/spark-checkpoint-workloads-1.0-SNAPSHOT-jar-with-dependencies.jar \
    --k 3 --iterations 3 --checkpoint "$checkpoint" --checkpoint-interval 1 samples/wikipedia-corpus.txt samples/stopwords.txt

  # wait for the history server
  sleep 10

  # get .csv of Batch workload run
  python3 experiments_service/src/experiment_metrics.py $checkpoint

  # delete the app.log for the next run
  rm /Users/fschnei4/spark-3.1.2-bin-hadoop3.2/app-logs/app.log


done


