current_date=`date +"%d-%m-%Y%T"`
log_path=log/$current_date.log
./spark_utils/run_workloads.sh > $log_path