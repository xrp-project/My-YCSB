#ifndef YCSB_WORKLOAD_H
#define YCSB_WORKLOAD_H

#include <cerrno>
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <cmath>
#include <stdexcept>
#include <numeric>
#include <fstream>
#include <string>
#include <sstream>
#include <list>
#include <vector>
#include <mutex>
#include <memory>

enum OperationType {
	UPDATE = 0,
	INSERT,
	READ,
	SCAN,
	READ_MODIFY_WRITE,
	NR_OP_TYPE,
};

extern const char* operation_type_name[];

struct Operation {
	OperationType type;
	char *key_buffer;
	char *value_buffer;  /* for UPDATE, INSERT, and READ_MODIFY_WRITE */
	int value_buffer_size;
	char *reply_value_buffer;  /* for READ */
	long scan_length;  /* for SCAN */
	bool is_last_op;
};

struct OpProportion {
	float op[NR_OP_TYPE];
};

struct Workload {
	long key_size;
	long value_size;
	bool record_keys = false;
	std::vector<unsigned long> recorded_keys;

	Workload(long key_size, long value_size);
	virtual void next_op(Operation *op) = 0;
	virtual bool has_next_op() = 0;

protected:
	static long generate_random_long(unsigned int *seedp);
	static double generate_random_double(unsigned int *seedp);
};

struct UniformWorkload : public Workload {
	/* configuration */
	long nr_entry;
	long nr_op;
	long scan_length;
	struct OpProportion op_prop;

	/* constants */
	static constexpr int key_format_len = 64;

	/* states */
	unsigned int seed;
	long cur_nr_op;
	char key_format[key_format_len];

	UniformWorkload(long key_size, long value_size, long scan_length, long nr_entry, long nr_op, struct OpProportion op_prop, unsigned int seed);
	void next_op(Operation *op) override;
	bool has_next_op() override;

private:
	void generate_key_string(char *key_buffer, long key);
	void generate_value_string(char *value_buffer);
};

struct ZipfianWorkload : public Workload {
	/* configuration */
	long nr_entry;
	long nr_op;
	long scan_length;
	struct OpProportion op_prop;
	double zipfian_constant;
	int scan_worker_count;
	bool do_only_scans = false;

	/* constants */
	static constexpr int key_format_len = 64;

	/* states */
	unsigned int seed;
	long cur_nr_op;
	char key_format[key_format_len];

	double zetan;
	double theta;
	double zeta2theta;
	double alpha;
	double eta;

	ZipfianWorkload(long key_size, long value_size, long scan_length, long nr_entry, long nr_op, struct OpProportion op_prop, double zipfian_constant, unsigned int seed);
	void next_op(Operation *op) override;
	bool has_next_op() override;
	ZipfianWorkload *clone(unsigned int new_seed);

private:
	static unsigned long fnv1_64_hash(unsigned long value);
	unsigned long generate_zipfian_random_ulong(bool hash);
	void generate_key_string(char *key_buffer, unsigned long key);
	void generate_value_string(char *value_buffer);
};

struct InitWorkload : public Workload {
	/* configuration */
	long nr_entry;
	long start_key;

	/* constants */
	static constexpr int key_format_len = 64;

	/* states */
	unsigned int seed;
	long cur_nr_entry;
	char key_format[key_format_len];
	std::vector<unsigned long> key_shuffle;
	std::mutex lock;


	InitWorkload(long nr_entry, long start_key, long key_size, long value_size, unsigned int seed);
	void next_op(Operation *op) override;
	bool has_next_op() override;
private:
	bool has_next_op_unsafe();
	void generate_key_string(char *key_buffer, long key);
	void generate_value_string(char *value_buffer);
};

struct LatestWorkload : public Workload {
	/* configuration */
	long nr_entry;
	long nr_op;
	double read_ratio;
	double zipfian_constant;

	/* constants */
	static constexpr int key_format_len = 64;

	/* states */
	unsigned int seed;
	long cur_nr_op;
	unsigned long cur_ack_key;
	char key_format[key_format_len];

	double zetan;
	double theta;
	double zeta2theta;
	double alpha;
	double eta;

	LatestWorkload(long key_size, long value_size, long nr_entry, long nr_op, double read_ratio, double zipfian_constant, unsigned int seed);
	void next_op(Operation *op) override;
	bool has_next_op() override;
	LatestWorkload *clone(unsigned int new_seed);

private:
	static unsigned long fnv1_64_hash(unsigned long value);
	unsigned long generate_zipfian_random_ulong(bool hash);
	void generate_key_string(char *key_buffer, unsigned long key);
	void generate_value_string(char *value_buffer);
};

struct TraceIterator {
	TraceIterator(std::string trace_path) {
		// Load the trace file lines into memory
		std::ifstream trace_file(trace_path);
		if (!trace_file.is_open()) {
			throw std::invalid_argument("Failed to open trace file: " + trace_path);
		}
		std::string line;
		while (std::getline(trace_file, line)) {
			line_list.push_back(line);
		}
		line_iterator = line_list.begin();

		// Print the trace file and number of lines
		fprintf(stderr, "Number of lines in trace file: %zu\n", line_list.size());
	}

	bool next_op(Operation *op) {
		// Get the next line. The format is:
		// <operation> <key>
		// Split into tokens
		// Generate the operation
		std::string line;
		{
			std::lock_guard<std::mutex> guard(lock);
			if (line_iterator == line_list.end()) {
				return false;
			}
			line = *line_iterator;
			++line_iterator;

			std::istringstream iss(line);
			std::vector<std::string> tokens;
			std::string token;
			while (iss >> token) {
				tokens.push_back(token);
			}

			if (tokens.size() != 2) {
				throw std::runtime_error("Invalid trace file format, got " + std::to_string(tokens.size()) + " tokens: " + line);
			}

			if (tokens[0] == "READ") {
				op->type = READ;
			} else if (tokens[0] == "WRITE") {
				op->type = INSERT;
			} else {
				throw std::runtime_error("Unsupported operation type in trace file");
			}
			strcpy(op->key_buffer, tokens[1].c_str());
			return true;
		}
	}
private:
	// line list iterator
	std::vector<std::string>::iterator line_iterator;
	std::vector<std::string> line_list;
	std::mutex lock;
};


struct TraceWorkload : public Workload {
	/* configuration */
	long nr_op;
	std::string trace_path; // unused now
	std::shared_ptr<TraceIterator> trace_iterator;

	/* states */
	unsigned int seed;
	long cur_nr_op;
	std::ifstream trace_file;
	std::list<std::string> line_list;

	TraceWorkload(long key_size, long value_size, long nr_op, std::string trace_path, unsigned int seed);
	void next_op(Operation *op) override;
	bool has_next_op() override;

private:
	void generate_value_string(char *value_buffer);
};


#endif //YCSB_WORKLOAD_H
