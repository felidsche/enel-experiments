from os.path import exists
from unittest import TestCase

from get_workloads import get_workloads
from run_experiments import ExperimentsRunner


class TestExperimentsRunner(TestCase):

    def test_run_local(self):
        local = bool(1)

        # requires vpn connection and port forwarding of spark history server
        runner = ExperimentsRunner(
            local=local,
            log4j_configfile_path="../../spark_utils/src/main/resources/log4j.properties",
            fatjarfile_path="../../spark_utils/target/spark-checkpoint-workloads-1.0-SNAPSHOT-jar-with-dependencies.jar")

        checkpoint = 0
        iterations = 1
        k = 3
        sampling_fraction = 0.001
        checkpoint_interval = 5
        analytics_data_paths = ["../../samples/OS_ORDER_ITEM.txt", "../../samples/OS_ORDER.txt"]
        lda_data_paths = ["../../samples/LDA_wiki_noSW_90_Sampling_1", "../../samples/stopwords.txt"]
        gbt_data_path = "../../samples/small.txt"
        pagerank_data_path = "../../samples/google_g_16.txt"

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
        file_path = runner.run_local(workloads=workloads)
        self.assertIsInstance(file_path, str, "file_path is not a string, sth went wrong")

    def test_run_remote(self):
        local = bool(0)
        app_name = "GradientBoostedTrees"
        log_path = "cluster_experiment/logs/gbt/20210922/gbt-9000000-10-checkpoint-driver.log"
        has_checkpoint = bool(1)

        # requires vpn connection and port forwarding of spark history server
        runner = ExperimentsRunner(local=local, history_server_url="http://localhost:18081/api/v1/", log_path=log_path)
        file_path = runner.run_remote(has_checkpoint=has_checkpoint, app_name=app_name)
        self.assertTrue(exists(file_path))
