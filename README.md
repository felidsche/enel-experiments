# msc-thesis-saft-experiments
Experiment to collect MTTR and Tc for sample workloads: 
##Analytics of Big Data Bench e-com data
##GBT regression on generated data
##LDA clustering of Big Data Bench Wikipedia corpus
## Prerequisites
- ``$SPARK_HOME`` should point to the spark installation
```bash
    pip install -r requirements.txt
    cd spark_utils
    mvn clean package
    $SPARK_HOME/sbin/start-history-server.sh # localhost:18080
    cd ..
  ```
## Usage
````bash 
python run_experiments.py <LOCAL> # pass 1 if local execution is wanted
````

