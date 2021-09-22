# Actual data uploaded on the cluster
- for the first iteration of measuring tcms
- all datasets should be around ~1GB of size
- 20 iterations
## commands to generate the data
## SGD
### sgd1miosmp_50dim.txt (local)
#### 1. generating the data
- Usage: `SGDDataGeneratorLocal <samples> <dimension> <output>`
- Full command:
```bash
java -cp ../spark_utils/target/spark-checkpoint-workloads-1.0-SNAPSHOT-jar-with-dependencies.jar de.tu_berlin.dos.arm.spark_utils.datagens.SGDDataGeneratorLocal 1000000 50 sgd_1miosmp_50dim.txt
```
#### 2. uploading the data
- *Note*: this needs VPN connection and `$HDFSCLI_CONFIG` to be set
````bash
# Write a single file to HDFS.
hdfscli upload sgd_1miosmp_50dim.txt spark/sgd/ -v
````