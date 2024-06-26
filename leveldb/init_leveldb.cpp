#include <iostream>
#include "leveldb_client.h"
#include "leveldb_config.h"
#include "worker.h"

int main(int argc, char *argv[]) {
    if (argc != 2) {
        printf("Usage: %s <config file>\n", argv[0]);
        return -EINVAL;
    }
    YAML::Node file = YAML::LoadFile(argv[1]);
    LevelDBConfig config = LevelDBConfig::parse_yaml(file);

    LevelDBFactory factory(config.leveldb.data_dir, config.leveldb.options_file,
                           config.leveldb.cache_size,
                           config.leveldb.print_stats);

    run_init_workload_with_op_measurement(
        "Initialization", &factory, config.database.nr_entry,
        config.database.key_size, config.database.value_size,
        1); // Use 1 thread for now, as the key shuffling is buggy with many threads
}
