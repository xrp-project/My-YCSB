#include <string>
#include <iostream>
#include <atomic>

#include "leveldb/db.h"
#include "leveldb/options.h"
#include "leveldb/status.h"

#include "leveldb_client.h"

std::atomic<unsigned int> key_fails = 0;

LevelDBClient::LevelDBClient(LevelDBFactory *factory, int id)
	: Client(id, factory) {
	this->db = factory->db;
	}

LevelDBClient::~LevelDBClient() {}

int LevelDBClient::do_operation(Operation *op) {
	int ret = 0;
repeat:
	switch (op->type) {
	case UPDATE:
		ret = this->do_update(op->key_buffer, op->value_buffer);
		break;
	case INSERT:
		ret = this->do_insert(op->key_buffer, op->value_buffer);
		break;
	case READ:
		ret = this->do_read(op->key_buffer, &op->value_buffer);
		break;
	case SCAN:
		ret = this->do_scan(op->key_buffer, op->scan_length);
		break;
	case READ_MODIFY_WRITE:
		ret = this->do_read_modify_write(op->key_buffer, op->value_buffer);
		break;
	default:
		throw std::invalid_argument("invalid op type");
	}
	if (ret < 0) {
		std::cout << "Key actually failed: " << op->key_buffer << std::endl;
		//goto repeat;
	}
	return ret;
}

int LevelDBClient::do_read(char *key_buffer, char **value) {
	leveldb::Status status;
	leveldb::Slice key(key_buffer);
	std::string value_str;

	leveldb::ReadOptions read_options = leveldb::ReadOptions();

	status = this->db->Get(read_options, key_buffer, &value_str);
	if (!status.ok()) {
		fprintf(stderr, "LevelDBClient: read failed, ret: %s\n", status.ToString().c_str());
		return -1;
	}
	memcpy(*value, value_str.c_str(), value_str.size());
	(*value)[value_str.size()] = '\0';
	return 0;
}

int LevelDBClient::do_update(char *key_buffer, char *value_buffer) {
	return this->do_read_modify_write(key_buffer, value_buffer);
}

int LevelDBClient::do_insert(char *key_buffer, char *value_buffer) {
	leveldb::Status status;
	leveldb::Slice key(key_buffer), value(value_buffer);

	status = this->db->Put(leveldb::WriteOptions(), key, value);
	if (!status.ok()) {
		fprintf(stderr, "LevelDBClient: update failed, ret: %s\n", status.ToString().c_str());
		return -1;
	}
	return 0;
}

int LevelDBClient::do_read_modify_write(char *key_buffer, char *value_buffer) {
	int ret;
	char *old_value = new char[strlen(value_buffer)+1];
	ret = this->do_read(key_buffer, &old_value);
	delete[] old_value;
	if (ret != 0) {
		return ret;
	}
	return this->do_insert(key_buffer, value_buffer);
}

int LevelDBClient::do_scan(char *key_buffer, long scan_length) {
	// Unimplemented
	throw std::invalid_argument("scan not implemented for leveldb");
}

int LevelDBClient::reset() {return 0;}

void LevelDBClient::close() {}

LevelDBFactory::LevelDBFactory(std::string data_dir, std::string options_file,
							   long long cache_size, bool print_stats): client_id(0) {
	this->data_dir = data_dir;
	this->print_stats = print_stats;

	fprintf(stderr, "LevelDBFactory: data_dir: %s, print_stats: %d\n",
	data_dir.c_str(), options_file.c_str(), cache_size / 1000000, print_stats);

	leveldb::DB* db;
	leveldb::Status status;
	leveldb::Options options;

	options.create_if_missing = true;
	status = leveldb::DB::Open(options, data_dir, &db);

	if (!status.ok()) {
		fprintf(stderr, "LevelDBFactory: failed to open db, ret: %s\n", status.ToString().c_str());
		throw std::invalid_argument("failed to open db");
	}
	this->db = db;
}

LevelDBFactory::~LevelDBFactory() {
	delete this->db;
}

void leveldb_print_stat(leveldb::DB *db, const char* key) {
	fprintf(stdout, "\n==== DB ===\n");
	std::string stats;
	if (!db->GetProperty(key, &stats)) {
		stats = "(failed)";
	}

	fprintf(stdout, "\n%s\n", stats.c_str());
}

void LevelDBFactory::do_print_stats() {
	fprintf(stdout, "=== LevelDB Stats Start ===\n");
	leveldb_print_stat(this->db, "leveldb.stats");
	fprintf(stdout, "=== LevelDB Stats End ===\n");
}

void LevelDBFactory::reset_stats() {
	// no-op
}

LevelDBClient * LevelDBFactory::create_client() {
	return new LevelDBClient(this, this->client_id++);
}

void LevelDBFactory::destroy_client(Client *client) {
	LevelDBClient *leveldb_client = static_cast<LevelDBClient *>(client);
	delete leveldb_client;
}
