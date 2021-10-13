# Actual data uploaded on the cluster
- for the first iteration of measuring tcms
- all datasets should be around ~1GB of size
- 20 iterations
## 0. commands to generate the data
21.09.2021
## SGD
### sgd1miosmp_50dim.txt (local)
#### 1. generating the data
- Usage: `SGDDataGeneratorLocal <samples> <dimension> <output>`
- Full command:
```bash
java -cp ../spark_utils/target/spark-checkpoint-workloads-1.0-SNAPSHOT-jar-with-dependencies.jar de.tu_berlin.dos.arm.spark_utils.datagens.SGDDataGeneratorLocal 1000000 50 sgd_1miosmp_50dim.txt
```
## 2. uploading a jar to the cluster
22.09.2021
- *Note*: this needs VPN connection and `$HDFSCLI_CONFIG` to be set (see `README.md`)
### Write a single jar to HDFS.
```bash
hdfscli upload --alias=prod ../spark_utils/target/spark-checkpoint-workloads-1.0-SNAPSHOT-jar-with-dependencies.jar jar-files/ -v
# or with logging
python src/hdfs_service.py file_upload http://domain:port user / jar-files/spark-checkpoint-workloads-1.0-SNAPSHOT-jar-with-dependencies.jar ../spark_utils/target/spark-checkpoint-workloads-1.0-SNAPSHOT-jar-with-dependencies.jar True
```

## 3. executing a job on the cluster
```bash
kubectl apply -f conf/gbt/gbt_small.yaml  # spark-3c566556aa494a7e9f462949432186c2
# sparkapplication.sparkoperator.k8s.io/gbtsmall created
kubectl apply -f conf/gbt/gbt_9000000_10.yaml # spark-d442e8781ae944f181fd4c5e555e4ccc
# sparkapplication.sparkoperator.k8s.io/gbt-9000000-10 created
kubectl apply -f conf/gbt/gbt_9000000_10_ckpt.yaml
# sparkapplication.sparkoperator.k8s.io/gbt-9000000-10-checkpoint created
kubectl apply -f conf/gbt/gbt_100000000_19.yaml.yaml
# sparkapplication.sparkoperator.k8s.io/gbt_100000000_19.yaml created
```

## 4. look at the job on the history server and prometheus
- port forwarding to localhost
```bash
kubectl port-forward service/prometheus-rest 9090:9090
kubectl port-forward service/spark-history-server-web  18081:18080  # localhost:cluster
```
- get the `sparkoperator` log
```bash
kubectl -n=namespace logs -f drms-cluster-sparkoperator-6996854bf7-7t445
```
- contains the name of the executing containers
- `gbt-driver`, 
```
  "executorState": {
    "gradientboostedtrees-358b727c0d34e002-exec-1": "PENDING",
    "gradientboostedtrees-358b727c0d34e002-exec-2": "PENDING",
    "gradientboostedtrees-358b727c0d34e002-exec-3": "FAILED",
    "gradientboostedtrees-358b727c0d34e002-exec-4": "FAILED"
  },

```
- contains failure messages like: `"errorMessage": "driver container failed with ExitCode: 1, Reason: Error"`
- get the driver pod log (vanishes if job successful else stays): `kubectl logs pod/gbt-driver`

## 5. creating a dir on HDFS
```bash
python src/hdfs_service.py mkdir http://domain:port user / checkpoints/felix-schneider-thesis
```

## 6. safe the application driver log to local fs (to get the checkpoint data)
````bash
kubectl logs pod/gbtsmall-driver > /Users/fschnei4/TUB_Master_ISM/SoSe21/MA/msc-thesis-saft-experiments/cluster_experiment/logs/gbt/20210922/gbt_small-checkpoint-driver.log
kubectl logs pod/gbt-9000000-10-driver > /Users/fschnei4/TUB_Master_ISM/SoSe21/MA/msc-thesis-saft-experiments/cluster_experiment/logs/gbt/20210922/logs/gbt-9000000-10-driver.log
kubectl logs pod/gbt-9000000-10-checkpoint-driver > /Users/fschnei4/TUB_Master_ISM/SoSe21/MA/msc-thesis-saft-experiments/cluster_experiment/logs/gbt/20210922/gbt-9000000-10-checkpoint-driver.log
kubectl logs pod/gbt-100000000-19-driver > /Users/fschnei4/TUB_Master_ISM/SoSe21/MA/msc-thesis-saft-experiments/cluster_experiment/logs/gbt/20210924/gbt-100000000-19-driver.log
````

## 7. look at checkpoints 
- ON HDFS: `/checkpoints/felix-schneider-thesis`
- sadly no files <_< (files only with `Analytics` workload)
  - spark app to folder mapping:
    - `spark-3c566556aa494a7e9f462949432186c2` (`gbt_small.yaml`): `a0c2a4ef-d78e-4449-b272-33e2eddb3472` 
    - `spark-5852506aefd747988e6e3e4545f09fbd` (`gbt_9000000_10_ckpt.yaml`): `52a40551-b920-4a3c-9d18-9df65874d2e0`, `c08bcafb-7ee3-483b-9c63-7a8b284c332c`

## 8. delete `sparkapplication` from the cluster
- show all `sparkapplications`: `kubectl get sparkapplications`
- delete a certain `sparkapplication`: `kubectl delete sparkapplication <SparkApplication name>`
- 24.09.21 GBT:
- `kubectl delete sparkapplication gbtsmall`
-> `sparkapplication.sparkoperator.k8s.io "gbtsmall" deleted`
```bash
kubectl delete sparkapplication gbt-9000000-10
kubectl delete sparkapplication gbt-9000000-10-checkpoint
kubectl delete sparkapplication gbt-100000000-19

```

## 9. check a (running) `sparkapplication` on the cluster
- `kubectl describe sparkapplication <SparkApplication name>`
- ``kubectl logs pod/<SparkApplication name>-driver``

## 10. fail a spark job
see `gbt-100000000-19-driver.log`, `gbt-100000000-19` was too big 

## 11. run `Analytics` workload on the cluster with and without checkpoint
- 24.09.2021
- HdFsS fiLeNaMe is case sEnSiTiVe
```bash
# run analytics workload once with and without checkpoint
kubectl apply -f conf/analytics/analytics_1gb.yaml
kubectl apply -f conf/analytics/analytics_1gb_ckpt.yaml
# copy the driver logs from k8s to local
kubectl logs pod/analytics-1gb-driver > /Users/fschnei4/TUB_Master_ISM/SoSe21/MA/msc-thesis-saft-experiments/cluster_experiment/logs/analytics/20210924/analytics-1gb-driver.log
kubectl logs pod/analytics-1gb-checkpoint-driver > /Users/fschnei4/TUB_Master_ISM/SoSe21/MA/msc-thesis-saft-experiments/cluster_experiment/logs/analytics/20210924/analytics-1gb-checkpoint-driver.log
# generate the csv output
cd ..
python experiments_runner/src/run_experiments.py 0 Analytics cluster_experiment/logs/analytics/20210924/analytics-1gb-driver.log 0
python experiments_runner/src/run_experiments.py 0 Analytics cluster_experiment/logs/analytics/20210924/analytics-1gb-checkpoint-driver.log 1
```