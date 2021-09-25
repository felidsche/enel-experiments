# Experiment to measure the time for checkpointing an RDD in two Spark Workloads: GBT and analytics (see spark_utils/jobs)
# Facts:
  # Iterations: 10, Checkpoint interval: 3 -> produces 3 checkpoints
# Datasets:
  # GBT: hdfs:///spark/sgd/100000000_19.txt (35GB) -> already on HDFS!, Analytics: hdfs:///spark/analytics/OS_ORDER_ITEM_30GB.txt, hdfs:///spark/analytics/OS_ORDER_30GB.txt,
# DRMS k8s Cluster:
  # Drivers: 1 Driver cores 4: Driver memory: 4096m
  # Executors: 10 Executor cores: 4 Executor memory: 8048m --> each executor processes 8GB (5 * 8GB = 40GB max file size in one run)
# Date: 25.09.2021

# 1. GBT
  # run the workloads
  kubectl apply -f conf/gbt/gbt_100000000_19.yaml
  # sparkapplication.sparkoperator.k8s.io/gbt_100000000_19.yaml created
  # with checkpoint
  kubectl apply -f conf/gbt/gbt_100000000_19_ckpt.yaml
  # get driver logs
  kubectl logs pod/gbt-100000000-19-driver > /Users/fschnei4/TUB_Master_ISM/SoSe21/MA/msc-thesis-saft-experiments/cluster_experiment/logs/gbt/20210925/gbt-100000000-19-10ex-8gbexmem-4exc-driver.log
  kubectl logs pod/gbt-100000000-19-checkpoint-driver > /Users/fschnei4/TUB_Master_ISM/SoSe21/MA/msc-thesis-saft-experiments/cluster_experiment/logs/gbt/20210925/gbt-100000000-19-10ex-8gbexmem-4exc-checkpoint-driver.log
  # delete the sparkapplications on the cluster
  kubectl delete sparkapplication gbt-100000000-19-checkpoint
  kubectl delete sparkapplication gbt-100000000-19

# 2. Analytics
  # generate the 30GB dataset on the cluster
  cd /Users/fschnei4/TUB_Master_ISM/SoSe21/MA/code/BigDataBench_V5.0_BigData_ComponentBenchmark/BigDataGeneratorSuite/Table_datagen/e-com
  ./generate_table.sh
  # input 30
  # upload the dataset to the cluster
  ./upload_analytics_30gb.sh
