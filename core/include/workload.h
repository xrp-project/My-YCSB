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

struct TraceWorkload : public Workload {
	/* configuration */
	long nr_op;
	std::string trace_path;

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
