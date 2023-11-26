#include <iostream>
#include <list>
#include "rocksdb_client.h"
#include "rocksdb_config.h"
#include "worker.h"

int main(int argc, char *argv[]) {
    if (argc < 3) {
        printf("Usage: %s <config file> <trace file 1> [<trace file 2> ...]\n", argv[0]);
        return -EINVAL;
    }

    YAML::Node file = YAML::LoadFile(argv[1]);
    RocksDBConfig config = RocksDBConfig::parse_yaml(file);

    RocksDBFactory factory(config.rocksdb.data_dir, config.rocksdb.options_file,
                           config.rocksdb.cache_size,
                           config.rocksdb.print_stats);

    std::list<std::string> trace_file_list;
    // Starting from 2 as 0 is the program name and 1 is the config file
    for (int i = 2; i < argc; ++i) {
        trace_file_list.push_back(argv[i]);
    }

    run_init_trace_workload_with_op_measurement(
        "Initialization", &factory, config.database.key_size, config.database.value_size,
        1, trace_file_list, 0, 0, 0, nullptr);
}
