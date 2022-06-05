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
	: Client(id, factory) {}

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
	throw std::logic_error("Not implemented yet!");
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

void udp_send(int sockfd, const char *msg, char *hostname, unsigned short port) {
	struct sockaddr_in servaddr = {};
	servaddr.sin_family = AF_INET;
	servaddr.sin_port = htons(port);
	servaddr.sin_addr.s_addr = inet_addr(hostname);

	if (sendto(sockfd, msg, strlen(msg)+1, 0, // +1 to include terminator
               (sockaddr*)&servaddr, sizeof(servaddr)) < 0){
        perror("cannot send message");
		throw std::invalid_argument("cannot send message");
    }
}