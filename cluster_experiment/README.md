# Actual data uploaded on the cluster
- for the first iteration of measuring tcms
- all datasets should be around ~1GB of size
- 20 iterations
## commands to generate the data
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
# Write a single jar to HDFS.
hdfscli upload --alias=prod ../spark_utils/target/spark-checkpoint-workloads-1.0-SNAPSHOT-jar-with-dependencies.jar jar-files/ -v
# or with logging
python hdfs_service file_upload http://domain:port user / jar_files/spark-checkpoint-workloads-1.0-SNAPSHOT-jar-with-dependencies.jar ../spark_utils/target/spark-checkpoint-workloads-1.0-SNAPSHOT-jar-with-dependencies.jar True
````