#include <iostream>
#include "worker.h"
#include "wt_udp_client.h"
#include "wt_udp_config.h"

int main(int argc, char *argv[]) {
	if (argc != 2) {
		printf("Usage: %s <config file>\n", argv[0]);
		return -EINVAL;
	}
	YAML::Node file = YAML::LoadFile(argv[1]);
	WiredTigerUDPConfig config = WiredTigerUDPConfig::parse_yaml(file);

	boost::uuids::random_generator uuid_gen;
	WiredTigerUDPFactory factory(uuid_gen);
	run_init_workload_with_op_measurement("Initialization", &factory,
	                                      config.database.nr_entry,
	                                      config.database.key_size,
	                                      config.database.value_size,
	                                      1);
}
