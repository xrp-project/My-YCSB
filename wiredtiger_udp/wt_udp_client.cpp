#include <stdio.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <strings.h>
#include <string.h>
#include <fcntl.h>
#include <stdlib.h>

#include "wt_udp_client.h"

WiredTigerUDPClient::WiredTigerUDPClient(WiredTigerUDPFactory *factory, int id)
	: Client(id, factory) {

	// Open a UDP socket per-request
	int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
	if (sockfd < 0) {
        perror("cannot open socket");
		throw std::invalid_argument("cannot open socket");
    }
	this->sockfd = sockfd;
}

WiredTigerUDPClient::~WiredTigerUDPClient() {}

int WiredTigerUDPClient::do_operation(Operation *op) {
	switch (op->type) {
	case UPDATE:
		return this->do_update(op->key_buffer, op->value_buffer);
	case INSERT:
		return this->do_insert(op->key_buffer, op->value_buffer);
	case READ:
		return this->do_read(op->key_buffer, &op->reply_value_buffer);
	case SCAN:
		return this->do_scan(op->key_buffer, op->scan_length);
	case READ_MODIFY_WRITE:
		return this->do_read_modify_write(op->key_buffer, op->value_buffer);
	default:
		throw std::invalid_argument("invalid op type");
	}
}

int WiredTigerUDPClient::do_read(char *key_buffer, char **value) {
	std::string msg = std::string("GET ") + std::string(key_buffer);
	while (true){
		std::string ans = udp_send_receive(this->sockfd, msg.c_str(),
									       "127.0.0.1", 11211);

		// Message format:
		// GET <key> <value>

		// We may get a delayed message
		// Skip it
		if (ans.find("GET ") != 0) {
			continue;
		}
		std::string key = ans.substr(4, ans.find(" ", 4) - 1);
		if (key != std::string(key_buffer)) {
			continue;
		}
		// We got the correct answer!
		return 0;
	}

	return 0;
}

int WiredTigerUDPClient::do_update(char *key_buffer, char *value_buffer) {
	throw std::logic_error("Not implemented yet!");
}

int WiredTigerUDPClient::do_insert(char *key_buffer, char *value_buffer) {
	return this->do_update(key_buffer, value_buffer);
}

int WiredTigerUDPClient::do_read_modify_write(char *key_buffer, char *value_buffer) {
	throw std::logic_error("Not implemented yet!");
}

int WiredTigerUDPClient::do_scan(char *key_buffer, long scan_length) {
	throw std::logic_error("Not implemented yet!");
}

int WiredTigerUDPClient::reset() {
	// Our client has no state!
	return 0;
}

void WiredTigerUDPClient::close() {}

WiredTigerUDPClient * WiredTigerUDPFactory::create_client() {
	return new WiredTigerUDPClient(this, this->client_id++);
}

WiredTigerUDPFactory::WiredTigerUDPFactory() : client_id(0) {}

void WiredTigerUDPFactory::destroy_client(Client *client) {
	WiredTigerUDPClient *wt_client = (WiredTigerUDPClient *)client;
	wt_client->close();
	delete wt_client;
}

std::string udp_send_receive(int sockfd, const char *msg, char *hostname, unsigned short port) {
	struct sockaddr_in servaddr = {};
	servaddr.sin_family = AF_INET;
	servaddr.sin_port = htons(port);
	servaddr.sin_addr.s_addr = inet_addr(hostname);

	if (sendto(sockfd, msg, strlen(msg), 0,
               (sockaddr*)&servaddr, sizeof(servaddr)) < 0){
        perror("cannot send message");
		throw std::invalid_argument("cannot send message");
    }

	char incoming_msg_buf[100];
	int nbytes = recvfrom(sockfd, incoming_msg_buf, 100, 0, NULL, NULL);
	if (nbytes < 0) {
		perror("cannot receive message");
		throw std::invalid_argument("cannot receive message");
	};
	incoming_msg_buf[nbytes] = '\0';
	return std::string(incoming_msg_buf);
}