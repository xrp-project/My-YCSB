#include <string>
#include <iostream>

#include "rocksdb_client.h"
#include "rocksdb/cache.h"
#include "rocksdb/customizable.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/status.h"
#include "rocksdb/statistics.h"
#include "rocksdb/utilities/options_util.h"


RocksDBClient::RocksDBClient(RocksDBFactory *factory, int id)
	: Client(id, factory) {
	this->db = factory->db;
	}

RocksDBClient::~RocksDBClient() {}

int RocksDBClient::do_operation(Operation *op) {
	switch (op->type) {
	case UPDATE:
		return this->do_update(op->key_buffer, op->value_buffer);
	case INSERT:
		return this->do_insert(op->key_buffer, op->value_buffer);
	case READ:
		return this->do_read(op->key_buffer, &op->value_buffer);
	case SCAN:
		return this->do_scan(op->key_buffer, op->scan_length);
	case READ_MODIFY_WRITE:
		return this->do_read_modify_write(op->key_buffer, op->value_buffer);
	default:
		throw std::invalid_argument("invalid op type");
	}
}

int RocksDBClient::do_read(char *key_buffer, char **value) {
	rocksdb::Status status;
	rocksdb::Slice key(key_buffer);
	std::string value_str;
	status = this->db->Get(rocksdb::ReadOptions(), key_buffer, &value_str);
	if (!status.ok()) {
		fprintf(stderr, "RocksDBClient: read failed, ret: %s\n", status.ToString().c_str());
		return -1;
	}
	memcpy(*value, value_str.c_str(), value_str.size());
	(*value)[value_str.size()] = '\0';
	return 0;
}

int RocksDBClient::do_update(char *key_buffer, char *value_buffer) {
	return this->do_read_modify_write(key_buffer, value_buffer);
}

int RocksDBClient::do_insert(char *key_buffer, char *value_buffer) {
	rocksdb::Status status;
	rocksdb::Slice key(key_buffer), value(value_buffer);
	std::string value_str;

	status = this->db->Put(rocksdb::WriteOptions(), key, value);
	if (!status.ok()) {
		fprintf(stderr, "RocksDBClient: update failed, ret: %s\n", status.ToString().c_str());
		return -1;
	}
	return 0;
}

int RocksDBClient::do_read_modify_write(char *key_buffer, char *value_buffer) {
	int ret;
	char *old_value;
	ret = this->do_read(key_buffer, &old_value);
	if (ret != 0) {
		return ret;
	}
	delete[] old_value;
	return this->do_insert(key_buffer, value_buffer);
}

int RocksDBClient::do_scan(char *key_buffer, long scan_length) {
	// Unimplemented
	throw std::invalid_argument("scan not implemented for rocksdb");
}

int RocksDBClient::reset() {return 0;}

void RocksDBClient::close() {}

RocksDBFactory::RocksDBFactory(std::string data_dir, std::string options_file,
							   int cache_size, bool print_stats): client_id(0) {
	this->data_dir = data_dir;
	this->print_stats = print_stats;

	rocksdb::Status status;
	rocksdb::Options options;
	rocksdb::DBOptions db_options;
	std::vector<rocksdb::ColumnFamilyDescriptor> cf_descs;
	std::vector<rocksdb::ColumnFamilyHandle *> cf_handles;

	fprintf(stderr, "RocksDBFactory: data_dir: %s, options_file: %s, cache_size: %dMB, print_stats: %d\n",
		data_dir.c_str(), options_file.c_str(), cache_size / 1000000, print_stats);
	if (options_file == "") {
		throw std::invalid_argument("you really want to specify an options file!");
	}
	this->_cache = rocksdb::NewLRUCache(cache_size);

	if (options_file != "") {
		status = rocksdb::LoadOptionsFromFile(options_file, rocksdb::Env::Default(), &db_options, &cf_descs);
		if (!status.ok()) {
			fprintf(stderr, "RocksDBFactory: failed to load options from file, ret: %s\n", status.ToString().c_str());
			throw std::invalid_argument("failed to load options from file");
		}
		if (this->print_stats) {
			db_options.statistics = rocksdb::CreateDBStatistics();
			db_options.statistics->set_stats_level(rocksdb::StatsLevel::kAll);
		}
		auto existing_table_options = cf_descs[0].options.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
		existing_table_options->block_cache = this->_cache;
		status = rocksdb::DB::Open(db_options, data_dir, cf_descs, &cf_handles, &this->db);
	} else {
		auto existing_table_options = options.table_factory->GetOptions<rocksdb::BlockBasedTableOptions>();
		existing_table_options->block_cache = this->_cache;
		options.create_if_missing = true;
		if (this->print_stats) {
			options.statistics = rocksdb::CreateDBStatistics();
		}
		status = rocksdb::DB::Open(options, data_dir, &this->db);
	}
	if (!status.ok()) {
		fprintf(stderr, "RocksDBFactory: failed to open db, ret: %s\n", status.ToString().c_str());
		throw std::invalid_argument("failed to open db");
	}
	fprintf(stderr, "RocksDBFactory: DB has '%d' levels\n" , this->db->NumberLevels());
}

RocksDBFactory::~RocksDBFactory() {
	delete this->db;
	this->_cache.reset();
}

void rocksdb_print_stat(rocksdb::DB *db, const char* key) {
	fprintf(stdout, "\n==== DB: %s ===\n", db->GetName().c_str());
	std::string stats;
	if (!db->GetProperty(key, &stats)) {
		stats = "(failed)";
	}
	fprintf(stdout, "\n%s\n", stats.c_str());
}

void RocksDBFactory::do_print_stats() {
	rocksdb_print_stat(this->db, "rocksdb.levelstats");
	rocksdb_print_stat(this->db, "rocksdb.stats");
	fprintf(stderr, "Cache usage: %luMB\n", this->_cache->GetUsage() / 1000000);
	fprintf(stdout, "=== RocksDB Stats Start ===\n");
	fprintf(stdout, "%s\n", this->db->GetOptions().statistics->ToString().c_str());
	fprintf(stdout, "=== RocksDB Stats End ===\n");
}

void RocksDBFactory::reset_stats() {
	// Start gathering RocksDB stats
	this->db->GetOptions().statistics->Reset();
}

RocksDBClient * RocksDBFactory::create_client() {
	return new RocksDBClient(this, this->client_id++);
}

void RocksDBFactory::destroy_client(Client *client) {
	RocksDBClient *rocksdb_client = (RocksDBClient *)client;
	delete rocksdb_client;
}
