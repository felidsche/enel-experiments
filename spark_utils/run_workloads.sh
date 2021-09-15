# run Batch workload without and with checkpoint

for checkpoint in 0 1
do

  $SPARK_HOME/bin/spark-submit \
    --class de.tu_berlin.dos.arm.spark_utils.jobs.Analytics \
    --master local \
    --driver-java-options "-Dlog4j.configuration=file:/Users/fschnei4/spark-3.1.2-bin-hadoop3.2/conf/log4j.properties" \
    spark_utils/target/runtime-adjustments-experiments-1.0-SNAPSHOT-jar-with-dependencies.jar \
    --sampling-fraction 0.0001 --checkpoint-rdd "$checkpoint" /Users/fschnei4/TUB_Master_ISM/SoSe21/MA/code/BigDataBench_V5.0_BigData_ComponentBenchmark/BigDataGeneratorSuite/Table_datagen/e-com/output/OS_ORDER_ITEM.txt /Users/fschnei4/TUB_Master_ISM/SoSe21/MA/code/BigDataBench_V5.0_BigData_ComponentBenchmark/BigDataGeneratorSuite/Table_datagen/e-com/output/OS_ORDER.txt

  # wait for the history server
  sleep 10

  # get .csv of Batch workload run
  python3 experiments_service/src/experiment_metrics.py $checkpoint

  # delete the app.log for the next run
  rm /Users/fschnei4/spark-3.1.2-bin-hadoop3.2/app-logs/app.log
done


