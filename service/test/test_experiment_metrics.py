from unittest import TestCase

from src.experiment_metrics import ExperimentMetrics


def get_log():
    with open("fixtures/app.log", mode="r") as file:
        return file.read()


class TestExperimentMetrics(TestCase, ExperimentMetrics):

    def test_get_tc(self):
        em = ExperimentMetrics(has_checkpoint=True)
        tc = em.get_tc(log=get_log())
        if em.get_has_checkpoint():
            print(f"Tc: {tc}")
            self.assertIsInstance(tc, int, "Tc is int")
            self.assertGreater(tc, 0, f"TC: <= 0")

    def test_get_tc_zero(self):
        log = "Chejkpoint took: 952938 ms"
        em = ExperimentMetrics(has_checkpoint=True)
        tc = em.get_tc(log=log)
        self.assertEqual(tc, 0)

    def test_calc_mttr(self):
        mttr = self.calc_mttr()
        print(f"MTTR: {mttr}")
        self.assertIsInstance(mttr, int, "MTTR is int")
        self.assertGreater(mttr, 0, "MTTR <= 0")
