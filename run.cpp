#include <iostream>
#include <stdexcept>
#include <thread>
#include <chrono>
#include "measurement.h"
#include "client.h"
#include "workload.h"

void worker_thread_fn(Client *client, Workload *workload, OpMeasurement *measurement) {
	OperationType type;
	char *key_buffer = new char[workload->key_size];
	char *value_buffer= new char[workload->value_size];

	while (workload->has_next_op()) {
		workload->next_op(&type, key_buffer, value_buffer);
		switch (type) {
		case SET:
			client->do_set(key_buffer, value_buffer);
			break;
		case GET:
			char *value;
			client->do_get(key_buffer, &value);
			break;
		default:
			throw std::invalid_argument("invalid op type");
		}
		measurement->record_op(type);
		measurement->record_progress(1);
	}
	free(key_buffer);
	free(value_buffer);
}

void monitor_thread_fn(const char *task, OpMeasurement *measurement) {
	double rt_throughput[NR_OP_TYPE];
	double progress;
	long epoch = 0;
	for (;!measurement->finished
	     ;std::this_thread::sleep_for(std::chrono::seconds(1))) {
		measurement->get_rt_throughput(rt_throughput);
		progress = measurement->get_progress_percent();
		printf("%s (%.2f%%): read throughput %.2lf ops/sec, write throughput %.2lf ops/sec, total throughput %.2lf ops/sec\n",
		       task, 100 * progress, rt_throughput[GET], rt_throughput[SET], rt_throughput[GET] + rt_throughput[SET]);
	}
	printf("%s overall: read throughput %.2lf ops/sec, write throughput %.2lf ops/sec, total throughput %.2lf ops/sec\n",
	       task, measurement->get_throughput(GET), measurement->get_throughput(SET),
	       measurement->get_throughput(GET) + measurement->get_throughput(SET));
}

enum {
	PARAM_KEY_SIZE = 1,
	PARAM_VALUE_SIZE,
	PARAM_NR_ENTRY,
	PARAM_NR_THREAD,
	PARAM_READ_RATIO,
	PARAM_NR_OP,
	PARAM_ARGC
};

void initialize_workload(WiredTigerFactory *factory, long nr_entry, long key_size, long value_size, unsigned int seed) {
	InitWorkload init_workload(nr_entry, key_size, value_size, 'i');
	OpMeasurement init_measurement;
	WiredTigerClient *init_client = factory->create_client();

	std::thread init_thread(worker_thread_fn, init_client, &init_workload, &init_measurement);
	std::thread init_stat_thread(monitor_thread_fn, "initialization", &init_measurement);
	init_thread.join();
	init_stat_thread.join();
	factory->destroy_client(init_client);
}

int main(int argc, char *argv[]) {
	if (argc != PARAM_ARGC) {
		printf("Usage: %s <key size> <value size> <number of entries> <number of threads> <read ratio> <number of ops>\n", argv[0]);
		return EINVAL;
	}
	long key_size = atol(argv[PARAM_KEY_SIZE]);
	long value_size = atol(argv[PARAM_VALUE_SIZE]);
	long nr_entry = atol(argv[PARAM_NR_ENTRY]);
	int nr_thread = atol(argv[PARAM_NR_THREAD]);
	double read_ratio = atol(argv[PARAM_READ_RATIO]);
	long nr_op = atol(argv[PARAM_NR_OP]);

	WiredTigerFactory factory(nullptr, nullptr, nullptr, nullptr, nullptr, true, nullptr);

	factory.update_cursor_config(WiredTigerClient::cursor_bulk_config);
	initialize_workload(&factory, nr_entry, key_size, value_size, 'i');

	factory.update_cursor_config(nullptr);
	Client **client_arr = new Client *[nr_thread];
	RandomWorkload **workload_arr = new RandomWorkload *[nr_thread];
	std::thread **thread_arr = new std::thread *[nr_thread];
	OpMeasurement measurement;
	for (int thread_index = 0; thread_index < nr_thread; ++thread_index) {
		client_arr[thread_index] = factory.create_client();
		workload_arr[thread_index] = new RandomWorkload(key_size, value_size, nr_entry, nr_op, read_ratio, thread_index);
	}
	measurement.start_measure();
	for (int thread_index = 0; thread_index < nr_thread; ++thread_index) {
		thread_arr[thread_index] = new std::thread(worker_thread_fn, client_arr[thread_index], workload_arr[thread_index], &measurement);
	}
	std::thread stat_thread(monitor_thread_fn, "RandomWorkload", &measurement);
	for (int thread_index = 0; thread_index < nr_thread; ++thread_index) {
		thread_arr[thread_index]->join();
	}
	measurement.finish_measure();
	stat_thread.join();
}