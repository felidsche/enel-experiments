# run Batch workload without and with checkpoint
import os
import subprocess
import time
from _ast import Tuple
from os.path import exists
import sys
import pandas as pd
import logging
from experiments_service.src.experiment_metrics import ExperimentMetrics

logger = logging.getLogger(__name__ + "ExperimentRunner")  # holds the name of the module


def get_log(path: str):
    logger.info(f"Getting log from: {path}...")
    with open(path, mode="r") as file:
        return file.read()


def write_results(app_data: pd.DataFrame, key: str, app_id: str, has_checkpoint: bool):
    if has_checkpoint:
        checkpoint_path = "checkpoint"
    else:
        checkpoint_path = "normal"
    filepath = f"output/{key}/{app_id}_{checkpoint_path}.csv"
    logger.info(f"Writing results of {app_id} to {filepath}...")
    file_exists = exists(filepath)
    if not file_exists:
        app_data.to_csv(
            path_or_buf=filepath,
            na_rep="nan"
        )


def execute_command(commands: list, local: bool):
    command = "".join(commands)
    logger.info(f"Executing command: {command}... local: {local}")
    if local:
        # if it fails there will be an exception
        return_code = subprocess.check_call(commands)
        logger.info(f"Return code: {return_code} for command:  {command}")
        logger.info(f"Waiting for 10 seconds for the history server...")
        time.sleep(10)


class ExperimentsRunner():

    def __init__(self, local: bool, spark_home: str = "/Users/fschnei4/spark-3.1.2-bin-hadoop3.2", jobs_classpath: str = "de.tu_berlin.dos.arm.spark_utils.jobs",
                 log4j_configfile_path: str = "spark_utils/src/main/resources/log4j.properties",
                 fatjarfile_path: str = "spark_utils/target/spark-checkpoint-workloads-1.0-SNAPSHOT-jar-with-dependencies.jar",
                 history_server_url: str = "http://localhost:18080/api/v1/"):
        self.classpath = jobs_classpath
        self.log4j_configfile_path = log4j_configfile_path
        self.local = local
        self.fatjarfile_path = fatjarfile_path
        self.spark_home = spark_home
        self.history_server_url = history_server_url

    checkpoints = [0, 1]

    log_path = "/Users/fschnei4/spark-3.1.2-bin-hadoop3.2/app-logs/app.log"

    def get_spark_submit(self, workload: str, local: bool, args: str) -> list:
        logger.info("Get spark_submit..")
        if local:
            master = "local"
        else:
            # TODO
            master = None
        spark_submit = [
            f"{self.spark_home}/bin/spark-submit",
            "--class",
            f"{self.classpath}.{workload}",
            "--master",
            f"{master}",
            "--driver-java-options",
            f'-Dlog4j.configuration=file:"{self.log4j_configfile_path}"',
            f"{self.fatjarfile_path}",
        ] + args.split(" ")
        return spark_submit

    def run(self):
        logger.info("""
        ########################################
        #       STARTING A NEW EXPERIMENT      #
        ########################################
        """)
        if exists(self.log_path):
            logger.warning("There is still is an app.log file from a previous run, deleting it...")
            os.remove(self.log_path)
        for checkpoint in self.checkpoints:

            # the key is the name of the class of the workload and the value is the program argument string
            workloads = {
                "Analytics": f"--sampling-fraction 0.01 --checkpoint-rdd {checkpoint} samples/OS_ORDER_ITEM.txt samples/OS_ORDER.txt",
                "LDAWorkload": f"--k 3 --iterations 10 --checkpoint {checkpoint} --checkpoint-interval 1 samples/LDA_wiki_noSW_90_Sampling_1 samples/stopwords.txt",
                "GradientBoostedTrees": f"--iterations 10 --checkpoint {checkpoint} --checkpoint-interval 5 samples/sgd.txt",
                "PageRank": f"--save-path output/ --iterations 10 --checkpoint {checkpoint} samples/google_g_16.txt"
            }

            for key, value in workloads.items():
                spark_submit = self.get_spark_submit(workload=key, args=value, local=self.local)
                logger.info(f"Runing workload: {key}  with args: {value}  locally: {local}")
                # execute the spark_submit command
                execute_command(commands=spark_submit, local=self.local)

                has_checkpoint = bool(checkpoint)
                app_id, metrics = self.get_metrics(has_checkpoint=has_checkpoint)
                write_results(app_data=metrics, key=key, app_id=app_id, has_checkpoint=has_checkpoint)

                logger.info(f"Result: {metrics.head(3)}")

                # remove app.log to read the next one
                logger.info(f"Removing: {self.log_path}")
                os.remove(self.log_path)
        logger.info("""
        ########################################
        #           EXPERIMENT DONE            #
        ########################################
        """)

    def get_metrics(self, has_checkpoint: bool) -> Tuple(str, pd.DataFrame):
        logger.info(f"Getting metrics from Spark Application  checkpoint: {has_checkpoint}")
        # get the experiment metrics
        em = ExperimentMetrics(has_checkpoint=has_checkpoint)

        log = get_log(self.log_path)
        app_id = em.get_app_id(log=log)
        app_data = em.get_app_data(app_id=app_id)
        tcs = em.get_tcs(log=log)

        rdds = em.get_checkpoint_rdds(log=log)
        duration = em.get_app_duration(app_id=app_id)
        logger.info(f"For App_ID: {app_id} The total duration was {duration} ms")
        if has_checkpoint:
            logger.info(f"For App_ID: {app_id} The rdds: {rdds} were checkpointed and it took {sum(tcs)} ms")
        rdd_tcs = em.merge_tc_rdds(
            tcs=tcs,
            rdds=rdds
        )
        app_data_tc = em.add_tc_to_app_data(rdd_tcs=rdd_tcs, app_data=app_data)

        return app_id, app_data_tc


if __name__ == '__main__':
    import logging.config
    logging.basicConfig(level=logging.INFO, filename=f"log/ExperimentRunner.log",
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    local = bool(int(sys.argv[1]))
    runner = ExperimentsRunner(local=local)
    runner.run()
