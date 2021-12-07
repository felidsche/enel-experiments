# msc-thesis-saft-experiments
###Experiment to collect MTTR and Tc for sample workloads: 
- `Analytics` of Big Data Bench e-com data
- GBT regression on generated data
- `LDA` clustering of Big Data Bench Wikipedia corpus

## Prerequisites
- VPN connection for operations on the DOS ARM cluster
```bash
export HOMEBREW_NO_AUTO_UPDATE=1
brew install openvpn
sudo openvpn --config /Users/fschnei4/TUB_Master_ISM/SoSe21/MA/spark-k8s-cluster/felix.j.schneider@campus.tu-berlin.de.ovpn --redirect-gateway  # the last flag is only necessary when down- or uploading
  ```
- create a configuration for the HDFS cluster
```bash
touch .hdfscli.cfg
[global]
default.alias = prod

[prod.alias]
url = http://domain:port
user = user
root = /
```
### versions

- ``$SPARK_HOME`` should point to the spark installation
```bash
    pip install -r requirements.txt
    cd spark_utils
    mvn clean package
    $SPARK_HOME/sbin/start-history-server.sh # localhost:18080
    # tell  HdfsCLI to use the config in this repo
     export HDFSCLI_CONFIG=$(pwd)/.hdfscli.cfg
    cd ..
  ```
## local execution

- runs the workloads
- captures the metrics
- generates a csv
### Usage
````bash
python <script_path> <local>
````
### Example
```bash
python experiments_runner/src/run_experiments.py 1  # bool
```

## remote execution
- captures the metrics
- generates a csv
### Requirements
- job ran on the cluster
- VPN is activated (see above)
- port forwarding of the spark history server is active:
````bash
kubectl port-forward service/spark-history-server-web  18081:18080  # localhost:cluster
````
### Usage
````bash
python <script_path> <local: 0/1> <app_name> <log_path> <has_checkpoint>
````
### Example
```bash
python experiments_runner/src/run_experiments.py 0 GradientBoostedTrees cluster_experiment/logs/gbt/20210922/gbt-9000000-10-checkpoint-driver.log 1
```

