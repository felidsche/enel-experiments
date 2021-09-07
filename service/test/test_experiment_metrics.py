from unittest import TestCase
from src.experiment_metrics import ExperimentMetrics

class TestExperimentMetrics(TestCase, ExperimentMetrics):

    def test_get_tc(self):
        tc = self.get_tc()
        self.assertTrue(tc == 0)


    def test_calc_mttr(self):
        mttr = self.calc_mttr()
        self.assertTrue(mttr == 0)
