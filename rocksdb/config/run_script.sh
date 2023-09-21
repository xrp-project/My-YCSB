nohup python3 bench_rocksdb.py \
    --results-file=/mydata/jeremy-home/rocksdb_zipf_results.json \
    --cpu 2 \
    --db-path=/mydata/database \
    --bench-path=/mydata/jeremy-home/My-YCSB/build/run_rocksdb \
    --config-dir=/mydata/jeremy-home/My-YCSB/rocksdb/config \
    --temp-db-path=/mydata/datebase_temp \
    --disk-type=local \
    --threads-per-core=8 \
    > /mydata/jeremy-home/bench_rocksdb.log \
    2>&1 & \
tail -f /mydata/jeremy-home/bench_rocksdb.log
