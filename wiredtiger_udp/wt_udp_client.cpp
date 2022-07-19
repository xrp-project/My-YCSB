#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <unistd.h>
#include <iostream>

#include <boost/uuid/uuid_io.hpp>

#include "wt_udp_client.h"

std::vector<std::string> str_split(std::string str) {
    std::istringstream iss(str);
    std::vector<std::string> results(std::istream_iterator<std::string>{iss},
                                     std::istream_iterator<std::string>());
    return results;
}

enum response_type { RESPONSE_TYPE_GET, RESPONSE_TYPE_SET };

struct Response {
    response_type type;
    std::string id;
    std::string value;

    Response(std::string msg);
};

Response::Response(std::string msg) {
    std::vector<std::string> parts = str_split(msg);
    if (parts[0] == "VALUE") {
        // Get Response
        // Format: VALUE <value> <id>
        this->type = RESPONSE_TYPE_GET;
        this->value = parts[1];
        this->id = parts[2];
    } else if (parts[0] == "STORED") {
        // Set Response
        // Format: STORED <id>
        this->type = RESPONSE_TYPE_SET;
        this->id = parts[1];
    }
}

// udp_send_receive sends a UDP packet and waits for a response. It retries
// until it receives a response. It's assumed that the correct timeout option
// is set on the given sockfd.
std::string udp_send_receive(int sockfd, const char *msg, const char *hostname,
                             unsigned short port) {
    struct sockaddr_in servaddr = {};
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(port);
    servaddr.sin_addr.s_addr = inet_addr(hostname);
    char incoming_msg_buf[100];

    while (true) {
        int ret = sendto(sockfd, msg, strlen(msg), 0, (sockaddr *)&servaddr,
                         sizeof(servaddr));
        if (ret < 0) {
            perror("cannot send message");
            throw std::invalid_argument("cannot send message");
        }

        ret = recvfrom(sockfd, incoming_msg_buf, 100, 0, NULL, NULL);
        if (ret < 0) {
            if (errno != EWOULDBLOCK && errno != EAGAIN) {
                perror("recvmsg: Unexpected errno value (not from timeout)");
                throw std::invalid_argument("recvmsg: Unexpected errno value");
            } else {
                perror("Waiting for response timed out, retrying...");
            }
            continue;
        };
        incoming_msg_buf[ret] = '\0';
        break;
    }
    return std::string(incoming_msg_buf);
}

WiredTigerUDPClient::WiredTigerUDPClient(
    WiredTigerUDPFactory *factory, int id,
    boost::uuids::random_generator &uuid_gen)
    : Client(id, factory), uuid_gen(uuid_gen) {
    // Open a UDP socket per-request
    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) {
        perror("cannot open socket");
        throw std::invalid_argument("cannot open socket");
    }
    struct timeval tv;
    tv.tv_sec = 0;
    tv.tv_usec = 50000;  // 50000 us = 50 ms
    if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0) {
        perror("Failed to set timeout option on socket!");
        throw std::invalid_argument("Failed to set timeout option on socket!");
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

// TODO: Generic response parsing function

int WiredTigerUDPClient::do_read(char *key_buffer, char **value) {
    // Request format:
    // GET <key> <req_id>
    std::string uuid = boost::uuids::to_string(this->uuid_gen());
    std::string msg =
        std::string("GET ") + std::string(key_buffer) + std::string(" ") + uuid;
    while (true) {
        std::string hostname = std::string("127.0.0.1");
        std::string ans = udp_send_receive(this->sockfd, msg.c_str(),
                                           hostname.c_str(), 11211);

        // Check the id to match request and response
        struct Response resp(ans);
        if (resp.id != uuid) {
            printf(
                "Got response ID '%s', expected '%s'. Probably old response "
                "that got delayed.",
                resp.id.c_str(), uuid.c_str());
            continue;
        }
        // We got the correct answer!
        break;
    }
    return 0;
}

int WiredTigerUDPClient::do_update(char *key_buffer, char *value_buffer) {
    // Request format:
    // SET <key> <value> <req_id>
    std::string uuid = boost::uuids::to_string(this->uuid_gen());
    std::string msg = std::string("SET ") + std::string(key_buffer) +
                      std::string(" ") + std::string(value_buffer) +
                      std::string(" ") + uuid;
    while (true) {
        std::string hostname = std::string("127.0.0.1");
        std::string ans = udp_send_receive(this->sockfd, msg.c_str(),
                                           hostname.c_str(), 11211);

        // Check the id to match request and response
        struct Response resp(ans);
        if (resp.id != uuid) {
            printf(
                "Got response ID '%s', expected '%s'. Probably old response "
                "that got delayed.\n",
                resp.id.c_str(), uuid.c_str());
            continue;
        }
        // We got the correct answer!
        break;
    }
    return 0;
}

int WiredTigerUDPClient::do_insert(char *key_buffer, char *value_buffer) {
    return this->do_update(key_buffer, value_buffer);
}

int WiredTigerUDPClient::do_read_modify_write(char *key_buffer,
                                              char *value_buffer) {
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

WiredTigerUDPClient *WiredTigerUDPFactory::create_client() {
    boost::uuids::random_generator uuid_gen;
    return new WiredTigerUDPClient(this, this->client_id++, uuid_gen);
}

WiredTigerUDPFactory::WiredTigerUDPFactory(const char* server_addr) : client_id(0) {
    this->server_addr = server_addr;
}

void WiredTigerUDPFactory::destroy_client(Client *client) {
    WiredTigerUDPClient *wt_client = (WiredTigerUDPClient *)client;
    wt_client->close();
    delete wt_client;
}
