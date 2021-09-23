from unittest import TestCase

from get_workloads import get_workloads

class TestGetWorkloads(TestCase):
    def test_get_workloads(self):
        checkpoint = 0
        iterations = 1
        k = 3
        checkpoint_interval = 5
        analytics_data_paths = ["../../samples/OS_ORDER_ITEM.txt", "../../samples/OS_ORDER.txt"]
        lda_data_paths = ["../../samples/LDA_wiki_noSW_90_Sampling_1", "../../samples/stopwords.txt"]
        gbt_data_path = "../../samples/small.txt"
        pagerank_data_path = "../../samples/google_g_16.txt"

        workloads = get_workloads(
            checkpoint=checkpoint,
            iterations=iterations,
            k=k,
            checkpoint_interval=checkpoint_interval,
            analytics_data_paths=analytics_data_paths,
            lda_data_paths=lda_data_paths,
            gbt_data_path=gbt_data_path,
            pagerank_data_path=pagerank_data_path
        )
        self.assertEquals(len(workloads.keys()), 4, "Workload dict got not generated properly")
