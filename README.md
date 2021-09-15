# msc-thesis-saft-experiments
Experiment to collect MTTR and Tc for sample workloads
## Prerequisites
```bash
    cd spark_utils
    mvn clean package
    $SPARK_HOME/sbin/start-history-server.sh # localhost:18080
  ```
## Usage
````bash 
./run_experiments.sh
````

