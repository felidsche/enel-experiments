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
python hdfs_service file_upload http://domain:port user / jar_files/spark-checkpoint-workloads-1.0-SNAPSHOT-jar-with-dependencies.jar ../spark_utils/target/spark-checkpoint-workloads-1.0-SNAPSHOT-jar-with-dependencies.jar True
```

## 3. executing a job on the cluster
```bash
kubectl apply -f gbt_small.yaml
# sparkapplication.sparkoperator.k8s.io/gbt created
```

## 4. look at the job on the history server and prometheus
- port forwarding to localhost
```bash
kubectl port-forward service/prometheus-rest 9090:9090
kubectl port-forward service/spark-history-server-web  18081:18080  # localhost:cluster
```
- get the `sparkoperator` log
```bash
kubectl -n=drms logs -f drms-cluster-sparkoperator-6996854bf7-7t445
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