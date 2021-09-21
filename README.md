# msc-thesis-saft-experiments
###Experiment to collect MTTR and Tc for sample workloads: 
- `Analytics` of Big Data Bench e-com data
- GBT regression on generated data
- `LDA` clustering of Big Data Bench Wikipedia corpus

## Prerequisites
- VPN connection for operations on the DOS ARM cluster
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
## Usage
````bash 
python run_experiments.py <LOCAL> # pass 1 if local execution is wanted
````

