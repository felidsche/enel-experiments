# run Batch workload without and with checkpoint
import logging
import os
import subprocess
import sys
import time
from _ast import Tuple
from os.path import exists

import pandas as pd

from experiments_service.src.experiment_metrics import ExperimentMetrics
from get_workloads import get_workloads

logger = logging.getLogger(__name__ + "ExperimentRunner")  # holds the name of the module


def get_log(path: str):
    logger.info(f"Getting log from: {path}...")
    with open(path, mode="r") as file:
        return file.read()


def execute_command(commands: list, local: bool):
    command = "".join(commands)
    logger.info(f"Executing command: {command}... local: {local}")
    if local:
        # if it fails there will be an exception
        return_code = subprocess.check_call(commands)
        logger.info(f"Return code: {return_code} for command:  {command}")
        logger.info(f"Waiting for 10 seconds for the history server...")
        time.sleep(10)


class ExperimentsRunner:

    def __init__(self, local: bool, spark_home: str = "/Users/fschnei4/spark-3.1.2-bin-hadoop3.2",
                 jobs_classpath: str = "de.tu_berlin.dos.arm.spark_utils.jobs",
                 log4j_configfile_path: str = "spark_utils/src/main/resources/log4j.properties",
                 fatjarfile_path: str = "spark_utils/target/spark-checkpoint-workloads-1.0-SNAPSHOT-jar-with-dependencies.jar",
                 history_server_url: str = "http://localhost:18080/api/v1/",
                 log_path: str = "/Users/fschnei4/spark-3.1.2-bin-hadoop3.2/app-logs/app.log"):
        self.classpath = jobs_classpath
        self.log4j_configfile_path = log4j_configfile_path
        self.local = local
        self.fatjarfile_path = fatjarfile_path
        self.spark_home = spark_home
        self.history_server_url = history_server_url
        self.log_path = log_path

    def get_spark_submit(self, workload: str, args: str) -> list:
        logger.info("Get spark_submit..")
        master = "local"
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

    def write_results(self, app_data: pd.DataFrame, key: str, app_id: str, has_checkpoint: bool):
        if has_checkpoint:
            checkpoint_path = "checkpoint"
        else:
            checkpoint_path = "normal"

        if self.local:
            mode = "local"
        else:
            mode = "cluster"
        file_dir = f"output/{mode}/{key}"
        file_name = f"{app_id}_{checkpoint_path}.csv"
        file_path = f"{file_dir}/{file_name}"
        logger.info(f"Writing results of {app_id} to {file_path}...")
        file_exists = exists(file_path)
        if not file_exists:
            app_data.to_csv(
                path_or_buf=file_path,
                na_rep="nan"
            )
        else:
            logger.warning(
                f"No output is written because the file: {file_path} exists already in.")
        return file_path

    def run_local(self, workloads: dict) -> str:
        logger.info("""
            ########################################
            #   STARTING A NEW LOCAL EXPERIMENT    #
            ########################################
            """)
        file_path = None
        if exists(self.log_path):
            logger.warning("There is still is an app.log file from a previous run, deleting it...")
            os.remove(self.log_path)
        for key, value in workloads.items():
            spark_submit = self.get_spark_submit(workload=key, args=value)
            logger.info(f"Runing workload: {key}  with args: {value}  locally: {self.local}")
            # execute the spark_submit command
            execute_command(commands=spark_submit, local=self.local)

            has_checkpoint = bool(checkpoint)
            app_id, metrics = self.get_metrics(has_checkpoint=has_checkpoint,
                                               hist_server_url=self.history_server_url)
            file_path = self.write_results(app_data=metrics, key=key, app_id=app_id, has_checkpoint=has_checkpoint)

            logger.info(f"Result: {metrics.head(3)}")

            # remove app.log to read the next one
            logger.info(f"Removing: {self.log_path}")
            os.remove(self.log_path)
        logger.info("""
            ########################################
            #           LOCAL EXPERIMENT DONE      #
            ########################################
            """)
        return file_path

    def get_metrics(self, has_checkpoint: bool, hist_server_url: str, app_id: str = None,
                    cache_df_file_path: str = None) -> Tuple(str,
                                                             pd.DataFrame):
        """
        function that renders the output data
        :param has_checkpoint:
        :param hist_server_url:
        :param app_id:
        :param cache_df_file_path:
        :return: the ID and the output data of the app
        """
        logger.info(f"Getting metrics from Spark Application  checkpoint: {has_checkpoint}")
        # get the experiment metrics
        em = ExperimentMetrics(has_checkpoint=has_checkpoint, hist_server_url=hist_server_url, local=self.local)
        log = get_log(self.log_path)
        if app_id is None:
            app_id = em.get_app_id(log=log)
        app_data = em.get_app_data(app_id=app_id, cache_df_file_path=cache_df_file_path)
        tcs = em.get_tcs(log=log)

        rdds = em.get_checkpoint_rdds(log=log)
        duration = em.get_app_duration(app_id=app_id)
        logger.info(f"For App_ID: {app_id} The total duration was {duration} ms")

        rdd_tcs = em.merge_tc_rdds(
            tcs=tcs,
            rdds=rdds
        )
        if has_checkpoint:
            for key, value in rdd_tcs.items():
                logger.info(f"App_id: {app_id}; Checkpointing the rdd with ID: {key} took: {value} ms")
            logger.info(f"In sum, checkpointing App_id: {app_id} took: {sum(tcs)} ms.")
        try:
            tc_per_stage = em.get_tc_per_stage(app_id=app_id)
            app_data_tc = em.add_tc_to_tasks_in_checkpoint_stage(tc_per_stage=tc_per_stage, app_data=app_data)
            return app_id, app_data_tc
        except AttributeError as e:
            logging.error(f"{e}, vpn connected and ports from history server forwarded?")
            print(f"{e}, vpn connected and ports from history server forwarded?")
            sys.exit()

    def run_remote(self, has_checkpoint: bool, app_name: str, app_id: str = None,
                   cache_df_file_path: str = None) -> str:
        """
        produces a .csv with the data of the workload on task granularity and the time for a checkpoint
        :param has_checkpoint:
        :param app_name:
        :param cache_df_file_path:
        :return: the path to the output file
        """
        logger.info("""
            ########################################
            #   STARTING A NEW REMOTE EXPERIMENT   #
            ########################################
            """)
        if app_id is None:
            app_id, metrics = self.get_metrics(hist_server_url=self.history_server_url, has_checkpoint=has_checkpoint,
                                           cache_df_file_path=cache_df_file_path)
        else:
            _, metrics = self.get_metrics(hist_server_url=self.history_server_url, has_checkpoint=has_checkpoint,
                                       cache_df_file_path=cache_df_file_path, app_id=app_id)
        file_path = self.write_results(app_data=metrics, key=app_name, app_id=app_id, has_checkpoint=has_checkpoint)

        logger.info(f"Result: {metrics.head(3)}")

        logger.info("""
            ########################################
            #           REMOTE EXPERIMENT DONE     #
            ########################################
            """)
        return file_path


if __name__ == '__main__':
    import logging.config

    logging.basicConfig(level=logging.INFO, filename=f"log/ExperimentRunner.log",
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    local = bool(int(sys.argv[1]))

    if local:
        runner = ExperimentsRunner(local=local)
        checkpoint = 0
        iterations = 1
        k = 3
        sampling_fraction = 0.001
        checkpoint_interval = 5
        analytics_data_paths = ["samples/OS_ORDER_ITEM.txt", "samples/OS_ORDER.txt"]
        lda_data_paths = ["samples/LDA_wiki_noSW_90_Sampling_1", "samples/stopwords.txt"]
        gbt_data_path = "samples/gbt_small.txt"
        pagerank_data_path = "samples/google_g_16.txt"

        workloads = get_workloads(
            checkpoint=checkpoint,
            iterations=iterations,
            k=k,
            sampling_fraction=sampling_fraction,
            checkpoint_interval=checkpoint_interval,
            analytics_data_paths=analytics_data_paths,
            lda_data_paths=lda_data_paths,
            gbt_data_path=gbt_data_path,
            pagerank_data_path=pagerank_data_path,
            analytics_only=True
        )
        runner.run_local(workloads=workloads)
    else:
        # remote execution
        app_name = sys.argv[2]
        log_path = sys.argv[3]
        has_checkpoint = bool(int(sys.argv[4]))

        if "/" in log_path:
            # use the log path
            runner = ExperimentsRunner(local=local, history_server_url="http://localhost:18081/api/v1/", log_path=log_path)
            runner.run_remote(has_checkpoint=has_checkpoint, app_name=app_name)
        else:
            # use the app id
            runner = ExperimentsRunner(local=local, history_server_url="http://localhost:18081/api/v1/")
            runner.run_remote(has_checkpoint=has_checkpoint, app_name=app_name, app_id=log_path)

