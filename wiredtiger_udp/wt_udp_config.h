#ifndef YCSB_WT_CONFIG_H
#define YCSB_WT_CONFIG_H

#include <string>
#include <list>
#include "yaml-cpp/yaml.h"

using std::string;
using std::list;

struct WiredTigerUDPConfig {
	struct {
		long key_size;
		long value_size;
		long nr_entry;
	} database;
	struct {
		long nr_warmup_op;
		long nr_op;
		int nr_thread;
		long next_op_interval_ns;
		struct {
			float read;
			float update;
			float insert;
			float scan;
			float read_modify_write;
		} operation_proportion;
		string request_distribution;
		double zipfian_constant;
		list<string> trace_file_list;
		long scan_length;
	} workload;
	struct {
		bool print_stats;
	} wiredtiger_udp;

	static WiredTigerUDPConfig parse_yaml(YAML::Node &root);
};


WiredTigerUDPConfig WiredTigerUDPConfig::parse_yaml(YAML::Node &root) {
	WiredTigerUDPConfig config;
	YAML::Node database = root["database"];
	config.database.key_size = database["key_size"].as<long>();
	config.database.value_size = database["value_size"].as<long>();
	config.database.nr_entry = database["nr_entry"].as<long>();

	YAML::Node workload = root["workload"];
	config.workload.nr_warmup_op = workload["nr_warmup_op"].as<long>();
	config.workload.nr_op = workload["nr_op"].as<long>();
	config.workload.nr_thread = workload["nr_thread"].as<int>();
	config.workload.next_op_interval_ns = workload["next_op_interval_ns"].as<long>();
	YAML::Node operation_proportion = workload["operation_proportion"];
	config.workload.operation_proportion.read = operation_proportion["read"].as<float>();
	config.workload.operation_proportion.update = operation_proportion["update"].as<float>();
	config.workload.operation_proportion.insert = operation_proportion["insert"].as<float>();
	config.workload.operation_proportion.scan = operation_proportion["scan"].as<float>();
	config.workload.operation_proportion.read_modify_write = operation_proportion["read_modify_write"].as<float>();
	config.workload.request_distribution = workload["request_distribution"].as<string>();
	config.workload.zipfian_constant = workload["zipfian_constant"].as<double>();
	YAML::Node trace_file_list = workload["trace_file_list"];
	for (YAML::iterator iter = trace_file_list.begin(); iter != trace_file_list.end(); ++iter)
		config.workload.trace_file_list.push_back((*iter).as<string>());
	config.workload.scan_length = workload["scan_length"].as<long>();

	// Add if needed!
	// YAML::Node wiredtiger_udp = root["wiredtiger_udp"];

	return config;
}

#endif //YCSB_WT_CONFIG_H
