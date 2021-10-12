# Experiment to measure the time for checkpointing an RDD in two Spark Workloads: GBT and analytics (see spark_utils/jobs)
# Facts:
  # Iterations: 5, Checkpoint interval: 2 -> produces 2 checkpoints
# Datasets:
  # GBT: hdfs:///spark/sgd/100000000_19.txt (35GB) -> already on HDFS!, Analytics: hdfs:///spark/analytics/OS_ORDER_ITEM_30GB.txt, hdfs:///spark/analytics/OS_ORDER_30GB.txt,
# DRMS k8s Cluster:
  # Drivers: 1 Driver cores 4: Driver memory: 4096m
  # Executors: 10 Executor cores: 4 Executor memory: 8048m --> each executor processes 8GB (5 * 8GB = 40GB max file size in one run)
# Start Date: 25.09.2021

# 1. GBT (25.09.2021)
  # run the workloads
  kubectl apply -f conf/gbt/gbt_100000000_19.yaml
  # sparkapplication.sparkoperator.k8s.io/gbt_100000000_19.yaml created
  # with checkpoint
  kubectl apply -f conf/gbt/gbt_100000000_19_ckpt.yaml
  # get driver logs
  kubectl logs pod/gbt-100000000-19-5it-driver > /Users/fschnei4/TUB_Master_ISM/SoSe21/MA/msc-thesis-saft-experiments/cluster_experiment/logs/gbt/20210925/gbt-100000000-19-5it-10ex-8gbexmem-4exc-driver.log
  kubectl logs pod/gbt-100000000-19-5it-checkpoint-driver > /Users/fschnei4/TUB_Master_ISM/SoSe21/MA/msc-thesis-saft-experiments/cluster_experiment/logs/gbt/20210925/gbt-100000000-19-5it-10ex-8gbexmem-4exc-checkpoint-driver.log
  # delete the sparkapplications on the cluster
  kubectl delete sparkapplication gbt-100000000-19-checkpoint-5it
  kubectl delete sparkapplication gbt-100000000-19-5it
  # generate the csv output
  cd ..
  python experiments_runner/src/run_experiments.py 0 GradientBoostedTrees cluster_experiment/logs/gbt/20210925/gbt-100000000-19-5it-10ex-8gbexmem-4exc-driver.log 0
  python experiments_runner/src/run_experiments.py 0 GradientBoostedTrees cluster_experiment/logs/gbt/20210925/gbt-100000000-19-5it-10ex-8gbexmem-4exc-checkpoint-driver.log 1

# 2. Analytics (29.09.2021)
  # generate and upload the 30GB dataset on the cluster using a pod
  cat BigDataBench_V5.0_BigData_ComponentBenchmark/BigDataGeneratorSuite/Table_datagen/e-com-generator/README.md
  # run the workloads
  kubectl apply -f conf/analytics/analytics_30gb.yaml
  kubectl apply -f conf/analytics/analytics_30gb_ckpt.yaml
  # monitor the application
  kubectl get sparkapplication analytics-30gb-checkpoint
  kubectl describe sparkapplication analytics-30gb-checkpoint
  # checkpoint size = ~40GB (200 files at 200MB each)
  # get driver logs
  kubectl logs pod/analytics-30gb-driver > /Users/fschnei4/TUB_Master_ISM/SoSe21/MA/msc-thesis-saft-experiments/cluster_experiment/logs/analytics/20210925/analytics-30gb-10ex-8gbexmem-4exc-driver.log
  kubectl logs pod/analytics-30gb-5-checkpoint-driver > /Users/fschnei4/TUB_Master_ISM/SoSe21/MA/msc-thesis-saft-experiments/cluster_experiment/logs/analytics/20210925/analytics-30gb-5-10ex-8gbexmem-4exc-checkpoint-driver.log
  # generate the csv output
  cd ..
  python experiments_runner/src/run_experiments.py 0 Analytics cluster_experiment/logs/analytics/20210925/analytics-30gb-5-10ex-8gbexmem-4exc-driver.log 0
  python experiments_runner/src/run_experiments.py 0 Analytics cluster_experiment/logs/analytics/20210925/analytics-30gb-5-10ex-8gbexmem-4exc-checkpoint-driver.log 1
  # Reminder: delete checkpoint dir...
  kubectl delete sparkapplication analytics-30gb-5
  kubectl delete sparkapplication analytics-30gb-5-checkpoint
  # repeat n times...