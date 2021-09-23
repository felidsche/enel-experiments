# the key is the name of the class of the workload and the value is the program argument string
def get_workloads(checkpoint, analytics_data_paths: list, lda_data_paths: list, gbt_data_path: str,
                  pagerank_data_path: str, k: int, iterations: int, checkpoint_interval: int,
                  sampling_fraction: float = 0.01, analytics_only: bool = False) -> dict:
    if analytics_only:
        # for a faster run
        workloads = {
            "Analytics": f"--sampling-fraction {sampling_fraction} --checkpoint-rdd {checkpoint} {analytics_data_paths[0]} {analytics_data_paths[1]}"
        }
    else:
        workloads = {
            "Analytics": f"--sampling-fraction {sampling_fraction} --checkpoint-rdd {checkpoint} {analytics_data_paths[0]} {analytics_data_paths[1]}",
            "LDAWorkload": f"--k {k} --iterations {iterations} --checkpoint {checkpoint} --checkpoint-interval {checkpoint_interval} {lda_data_paths[0]} {lda_data_paths[1]}",
            "GradientBoostedTrees": f"--iterations {iterations} --checkpoint {checkpoint} --checkpoint-interval {checkpoint_interval} {gbt_data_path}",
            "PageRank": f"--save-path output/ --iterations {iterations} --checkpoint {checkpoint} {pagerank_data_path}"
        }

    return workloads
