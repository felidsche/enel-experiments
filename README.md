# msc-thesis-saft-experiments
Experiment to collect MTTR and Tc for sample workloads: 
- Analytics of Big Data Bench e-com data
- GBT regression on generated data
- LDA clustering of Big Data Bench Wikipedia corpus
## Prerequisites
```bash
    cd spark_utils
    mvn clean package
    $SPARK_HOME/sbin/start-history-server.sh # localhost:18080
    cd ..
  ```
## Usage
````bash 
./run_experiments.sh
````

