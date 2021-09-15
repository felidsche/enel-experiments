from unittest import TestCase

import pandas as pd

from experiment_metrics import ExperimentMetrics, get_log


class TestExperimentMetrics(TestCase, ExperimentMetrics):
    # the Spark History server needs to run for the tests to work

    em = ExperimentMetrics(has_checkpoint=True)

    def __init__(self, methodName: str = ...):
        super().__init__(methodName)
        self.ckpt_run_log_path = f"fixtures/Analytics/2021091514-ana-crdd1-ect.log"
        self.non_ckpt_run_log_path = f"fixtures/Analytics/2021091514-ana-crdd0-ect.log"

    def test_get_tc(self):
        if self.em.get_has_checkpoint():
            tcs = self.em.get_tcs(log=get_log(path=self.ckpt_run_log_path))
            print(f"TCs: {tcs}")
            print(f"TC sum: {sum(tcs)} ms")
            self.assertIsInstance(tcs, list, "Tcs is not a list")
            self.assertGreater(len(tcs), 0, "No tcs found")

    def test_get_checkpoint_rdd(self):
        if self.em.get_has_checkpoint():
            tcs = self.em.get_tcs(log=get_log(path=self.ckpt_run_log_path))
            checkpoint_rdds = self.em.get_checkpoint_rdds(log=get_log(path=self.ckpt_run_log_path))
            self.assertIsInstance(checkpoint_rdds, list, "checkpoint_rdds is not a list")
            self.assertGreater(len(checkpoint_rdds), 0, "No checkpoint_rdds found")
            self.assertEqual(len(tcs), len(checkpoint_rdds), "The amount of tcs and checkpoint_rdds does not match")

    def test_merge_tc_rdds(self):
        if self.em.get_has_checkpoint():
            tcs = self.em.get_tcs(log=get_log(path=self.ckpt_run_log_path))
            rdds = self.em.get_checkpoint_rdds(log=get_log(path=self.ckpt_run_log_path))
            rdd_tcs = self.em.merge_tc_rdds(tcs=tcs, rdds=rdds)
            self.assertIsInstance(rdd_tcs, dict, "rdd_tcs is not a dict")
            self.assertEqual(len(rdd_tcs.keys()), len(rdd_tcs.values()), "not the same amount of keys and values")
            self.assertIsNotNone(rdd_tcs.keys(), "Keys are not present")
            self.assertIsNotNone(rdd_tcs.values(), "Keys are not present")

    def test_add_tc_to_app_data(self):
        # this is very slow for large logs and needs improvement
        log = get_log(path=self.ckpt_run_log_path)
        app_id = self.em.get_app_id(log=log)
        app_data = self.em.get_app_data(app_id=app_id)
        rdd_tcs = self.em.merge_tc_rdds(
            tcs=self.em.get_tcs(log=get_log(path=self.ckpt_run_log_path)),
            rdds=self.em.get_checkpoint_rdds(log=get_log(path=self.ckpt_run_log_path))
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
        self.skipTest("# TODO: implement this once the failure injector is there")
        mttr = self.calc_mttr()
        print(f"MTTR: {mttr}")
        self.assertIsInstance(mttr, int, "MTTR is int")
        self.assertGreater(mttr, 0, "MTTR <= 0")

    def test_get_app_data(self):
        log = get_log(path=self.ckpt_run_log_path)
        app_id = self.em.get_app_id(log=log)
        app_data = self.em.get_app_data(app_id=app_id)
        self.assertIsInstance(app_data, pd.DataFrame)

    def test_get_stages_attempt_data(self):
        log = get_log(path=self.ckpt_run_log_path)
        app_id = self.em.get_app_id(log=log)
        stages_data = self.em.get_stages_attempt_data(app_id=app_id)
        self.assertIsInstance(stages_data, list, "Stages data is not a list")

    def test_get_app_id(self):
        log = get_log(path=self.ckpt_run_log_path)
        app_id = self.em.get_app_id(log=log)
        self.assertIsInstance(app_id, str, "App ID is not a string")
        self.assertGreater(len(app_id), 0, "App ID too short")
        self.assertEqual(app_id[0:6], "local-")

    def test_get_app_duration(self):
        log = get_log(self.ckpt_run_log_path)
        app_id = self.get_app_id(log=log)
        app_duration = self.em.get_app_duration(app_id=app_id)
        self.assertIsInstance(app_duration, int, "app duration is NaN")
        self.assertGreater(app_duration, 0, "app id was not found, or duration is 0")
        app_duration = self.em.get_app_duration(app_id="123")
        self.assertEqual(app_duration, 0, "not getting default value for wrong app id")

    def test_checkpoint_duration(self):
        """
        test if the checkpoint duration calculated by the service is the same as:
         ( sum(task_duration) of checkpoint run) - sum(task_duration) of non-checkpoint run )
        """

        ckpt_run_log = get_log(path=self.ckpt_run_log_path)
        non_ckpt_run_log = get_log(path=self.non_ckpt_run_log_path)
        tcs_ckpt_run_log = self.em.get_tcs(
            log=ckpt_run_log
        )
        tc_sum_ckpt_run_log = sum(tcs_ckpt_run_log)  # in ms
        assert tc_sum_ckpt_run_log > 0

        tcs_non_ckpt_run_log = self.em.get_tcs(
            log=non_ckpt_run_log
        )

        tc_sum_non_ckpt_run_log = sum(tcs_non_ckpt_run_log)
        assert tc_sum_non_ckpt_run_log == 0

        app_id_ckpt_run = self.em.get_app_id(log=ckpt_run_log)
        app_id_non_ckpt_run = self.em.get_app_id(log=non_ckpt_run_log)

        ckpt_run_duration = self.em.get_app_duration(app_id=app_id_ckpt_run)
        non_ckpt_run_duration = self.em.get_app_duration(app_id=app_id_non_ckpt_run)

        ckpt_run_duration_calc = non_ckpt_run_duration + tc_sum_ckpt_run_log
        self.assertEqual(ckpt_run_duration_calc, ckpt_run_duration,
                         f"The calculation differs by: {abs(ckpt_run_duration_calc - ckpt_run_duration)} ms")
