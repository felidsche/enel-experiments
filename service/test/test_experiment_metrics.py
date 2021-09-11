from unittest import TestCase

from experiment_metrics import ExperimentMetrics
import pandas as pd


def get_log():
    with open("/Users/fschnei4/spark-3.1.2-bin-hadoop3.2/app-logs/app.log", mode="r") as file:
        return file.read()


class TestExperimentMetrics(TestCase, ExperimentMetrics):
    # the Spark History server needs to run for the tests to work

    em = ExperimentMetrics(has_checkpoint=True)

    def __init__(self, methodName: str = ...):
        super().__init__(methodName)
        self.app_id = "local-1631359088830"
        self.stage_id = "218"
        self.stage_attempt_id = "0"

    def test_get_tc(self):
        if self.em.get_has_checkpoint():
            tcs = self.em.get_tcs(log=get_log())
            self.assertIsInstance(tcs, list, "Tcs is not a list")
            self.assertGreater(len(tcs), 0, "No tcs found")

    def test_get_checkpoint_rdd(self):
        if self.em.get_has_checkpoint():
            tcs = self.em.get_tcs(log=get_log())
            checkpoint_rdds = self.em.get_checkpoint_rdds(log=get_log())
            self.assertIsInstance(checkpoint_rdds, list, "checkpoint_rdds is not a list")
            self.assertGreater(len(checkpoint_rdds), 0, "No checkpoint_rdds found")
            self.assertEqual(len(tcs), len(checkpoint_rdds), "The amount of tcs and checkpoint_rdds does not match")

    def test_merge_tc_rdds(self):
        if self.em.get_has_checkpoint():
            tcs = self.em.get_tcs(log=get_log())
            rdds = self.em.get_checkpoint_rdds(log=get_log())
            rdd_tcs = self.em.merge_tc_rdds(tcs=tcs, rdds=rdds)
            self.assertIsInstance(rdd_tcs, dict, "rdd_tcs is not a dict")
            self.assertEqual(len(rdd_tcs.keys()), len(rdd_tcs.values()), "not the same amount of keys and values")
            self.assertIsNotNone(rdd_tcs.keys(), "Keys are not present")
            self.assertIsNotNone(rdd_tcs.values(), "Keys are not present")

    def test_add_tc_to_app_data(self):
        app_data = self.em.get_app_data(app_id=self.app_id)
        rdd_tcs = self.em.merge_tc_rdds(
            tcs=self.em.get_tcs(log=get_log()),
            rdds=self.em.get_checkpoint_rdds(log=get_log())
        )
        app_data_tc = self.em.add_tc_to_app_data(rdd_tcs=rdd_tcs, app_data=app_data)
        self.assertIsInstance(app_data_tc, pd.DataFrame, "Not a pandas Dataframe")
        self.assertGreater(app_data_tc.shape[1], app_data.shape[1], "No columns were gained")
        self.assertGreater(app_data_tc.shape[0], app_data.shape[0], "No rows were gained")

    def test_get_data(self):

        jobs = self.em.get_data(
            hist_server_url=self.em.hist_server_url,
            endpoint="applications"
        )
        self.assertIsInstance(jobs, list)

    def test_get_task_list(self):
        task_list = self.em.get_task_list(
            hist_server_url=self.em.hist_server_url,
            app_id=self.app_id,
            stage_id=self.stage_id,
            stage_attempt_id=self.stage_attempt_id
        )
        self.fail()

    def test_get_tc_zero(self):
        log = "Chejkpoint took: 952938 ms"
        self.em = ExperimentMetrics(has_checkpoint=True)
        if self.em.get_has_checkpoint():
            tcs = self.em.get_tcs(log=log)
            self.assertEqual(len(tcs), 0)

    def test_tc_not_ms(self):
        # if tc is not in ms, then the regex does not match
        log = "Checkpointing took: 952938 s"
        if self.em.get_has_checkpoint():
            tcs = self.em.get_tcs(log=log)
            self.assertEqual(len(tcs), 0)

    def test_calc_mttr(self):
        # TODO: implement this once the failure injector is there
        mttr = self.calc_mttr()
        print(f"MTTR: {mttr}")
        self.assertIsInstance(mttr, int, "MTTR is int")
        self.assertGreater(mttr, 0, "MTTR <= 0")

    def test_get_app_data(self):
        app_id = self.app_id
        app_data = self.em.get_app_data(app_id=app_id)
        self.assertIsInstance(app_data, pd.DataFrame)

    def test_get_stages_attempt_data(self):
        app_id = self.app_id
        stages_data = self.em.get_stages_attempt_data(app_id=app_id)
        self.assertIsInstance(stages_data, list, "Stages data is not a list")
