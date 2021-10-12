# 3. get executor logs (12.10.2021)
kubectl apply -f conf/analytics/analytics_1gb_ckpt.yaml  # deleteOnTermination: False
# monitor the application
kubectl get sparkapplication analytics-1gb-checkpoint
kubectl describe sparkapplication analytics-1gb-checkpoint
# save the logs of the 4 executors
kubectl logs pod/analytics-b477027c74f0b53e-exec-1 > /Users/fschnei4/TUB_Master_ISM/SoSe21/MA/msc-thesis-saft-experiments/cluster_experiment/logs/analytics/executor_logs_20211012/1gb-checkpoint-4ex-4gbexmem-4exc-exec-1.log
kubectl logs pod/analytics-b477027c74f0b53e-exec-2 > /Users/fschnei4/TUB_Master_ISM/SoSe21/MA/msc-thesis-saft-experiments/cluster_experiment/logs/analytics/executor_logs_20211012/1gb-checkpoint-4ex-4gbexmem-4exc-exec-2.log
kubectl logs pod/analytics-b477027c74f0b53e-exec-3 > /Users/fschnei4/TUB_Master_ISM/SoSe21/MA/msc-thesis-saft-experiments/cluster_experiment/logs/analytics/executor_logs_20211012/1gb-checkpoint-4ex-4gbexmem-4exc-exec-3.log
kubectl logs pod/analytics-b477027c74f0b53e-exec-4 > /Users/fschnei4/TUB_Master_ISM/SoSe21/MA/msc-thesis-saft-experiments/cluster_experiment/logs/analytics/executor_logs_20211012/1gb-checkpoint-4ex-4gbexmem-4exc-exec-4.log
# Result: no info on checkpointing
kubectl delete sparkapplication analytics-1gb-checkpoint