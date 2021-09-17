# run Batch workload without and with checkpoint
import subprocess
from _ast import Tuple
from os.path import exists
import sys
import pandas as pd
import logging
from experiment_metrics import ExperimentMetrics

logging.basicConfig(level=logging.INFO, filename=f"log/ExperimentRunner.log",
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)  # holds the name of the module


def get_log(path: str):
    logger.info(f"Getting log from: {path}...")
    with open(path, mode="r") as file:
        return file.read()


def write_results(app_data: pd.DataFrame, key: str, app_id: str):
    filepath = f"experiments_service/output/{key}/{app_id}_stage_task_and_tc_data.csv"
    logger.info(f"Writing results of {app_id} to {filepath}...")
    file_exists = exists(filepath)
    if not file_exists:
        app_data.to_csv(
            path_or_buf=filepath,
            na_rep="nan"
        )


def execute_command(commands: list, local: bool):
    command = "".join(commands)
    logger.info(f"Executing command: {command}...\n local: {local}")
    if local:
        # if it fails there will be an exception
        return_code = subprocess.check_call(commands)
        logger.info(f"Return code: {return_code} for command: \n {command}")


class ExperimentsRunner():

    def __init__(self, local: bool, spark_home: str = "/Users/fschnei4/spark-3.1.2-bin-hadoop3.2", jobs_classpath: str = "de.tu_berlin.dos.arm.spark_utils.jobs",
                 log4j_configfile_path: str = "/Users/fschnei4/spark-3.1.2-bin-hadoop3.2/conf/log4j.properties",
                 fatjarfile_path: str = "spark_utils/target/spark-checkpoint-workloads-1.0-SNAPSHOT-jar-with-dependencies.jar"):
        self.classpath = jobs_classpath
        self.log4j_configfile_path = log4j_configfile_path
        self.local = local
        self.fatjarfile_path = fatjarfile_path
        self.spark_home = spark_home

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
        for checkpoint in self.checkpoints:
            # the key is the name of the class of the workload and the value is the program argument string
            workloads = {
                "Analytics": f"--sampling-fraction 0.01 --checkpoint-rdd {checkpoint} /Users/fschnei4/TUB_Master_ISM/SoSe21/MA/code/BigDataBench_V5.0_BigData_ComponentBenchmark/BigDataGeneratorSuite/Table_datagen/e-com/output/OS_ORDER_ITEM.txt /Users/fschnei4/TUB_Master_ISM/SoSe21/MA/code/BigDataBench_V5.0_BigData_ComponentBenchmark/BigDataGeneratorSuite/Table_datagen/e-com/output/OS_ORDER.txt",
                "GBT": f"",
                "LDA": f""

            }
            for key, value in workloads.items():
                spark_submit = self.get_spark_submit(workload=key, args=value, local=self.local)
                logger.info(f"Runing workload: {key} \n with args: {value} \n locally: {local}")
                # execute the spark_submit command
                execute_command(commands=spark_submit, local=self.local)

                has_checkpoint = bool(checkpoint)
                app_id, metrics = self.get_metrics(has_checkpoint=has_checkpoint)
                write_results(app_data=metrics, key=key, app_id=app_id)

                logger.info(f"Result:\n {metrics.head(3)}")

    def get_metrics(self, has_checkpoint: bool) -> Tuple(str, pd.DataFrame):
        logger.info(f"Getting metrics from Spark Application \n checkpoint: {has_checkpoint}")
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
    local = bool(int(sys.argv[1]))
    runner = ExperimentsRunner(local=local)
    runner.run()
