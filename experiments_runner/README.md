# experiments_runner

---

Python application to generate `.csv` output data for an experiment

## Usage
````bash
python3 src/run_experiments.py {local (0/1)} {app name} {log_path/app_id} {checkpoint (0/1)}
````
### 1. local
1.1 executes the Spark application locally

1.2 generates the output data

```bash
python3 src/run_experiments.py 1
```
### 2. cluster
2.1 generates the output data from a log file
````bash
python3 src/run_experiments.py 0 GradientBoostedTrees cluster_experiment/logs/gbt/20210922/gbt-9000000-10-driver.log 0
````
2.2 or from the app_id
````bash
python3 src/run_experiments.py 0 Analytics spark-d493e730d6be481896910ff2a003db4e 1
````
## Logs
extensive application logs are written to `log/ExperimentRunner.log`

## Output
- *Note*: same file name -> DOES NOT overwrite