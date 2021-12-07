# generate the csv output for the Experiment on 25.09.2021 (see tc_exp_gbt_analytics_30gb_5it_2civ.sh for more detail)
# GBT non-checkpoint
#python experiments_runner/src/run_experiments.py 0 GradientBoostedTrees spark-4f315e53e8a14b8bb8e7e7553cab59c9 0
# GBT checkpoint
python experiments_runner/src/run_experiments.py 0 GradientBoostedTrees spark-21eb97c0b76a4aeaa9e93282e926c9f3 1
# Analytics non-checkpoint 1
python experiments_runner/src/run_experiments.py 0 Analytics spark-9078bc6ddb3a4c0b913072d827c939bc 0
# Analytics checkpoint 1
python experiments_runner/src/run_experiments.py 0 Analytics spark-d493e730d6be481896910ff2a003db4e 1
# Analytics non-checkpoint 2
python experiments_runner/src/run_experiments.py 0 Analytics spark-827da77e3d5946b395e7359bb0534f22 0
# Analytics checkpoint 2
python experiments_runner/src/run_experiments.py 0 Analytics spark-86ce2033320d452ebfb4c69e0d5aaaad 1
# Analytics non-checkpoint 3
python experiments_runner/src/run_experiments.py 0 Analytics spark-ab3aa599abf141568bed1c53aee2f842 0
# Analytics checkpoint 3
python experiments_runner/src/run_experiments.py 0 Analytics spark-581a0b09967648cca77d0084ed25af2f 1
# Analytics non-checkpoint 4
python experiments_runner/src/run_experiments.py 0 Analytics spark-f9330f93633948d195315dbeba6313f2 0
# Analytics checkpoint 4
python experiments_runner/src/run_experiments.py 0 Analytics spark-83e529840d2c4c0eb550105373fed434 1
# Analytics non-checkpoint 5
python experiments_runner/src/run_experiments.py 0 Analytics spark-af529ab0a80b48168d3c95c60bf7bca7 0
# Analytics checkpoint 5
python experiments_runner/src/run_experiments.py 0 Analytics spark-4c736126ec9a4b72a96a76c1155cd03e 1