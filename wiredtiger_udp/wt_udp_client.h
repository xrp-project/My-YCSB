#ifndef YCSB_WT_CLIENT_H
#define YCSB_WT_CLIENT_H

#include <boost/uuid/random_generator.hpp>

#include "client.h"

struct WiredTigerUDPFactory;

struct WiredTigerUDPClient : public Client {
    boost::uuids::random_generator &uuid_gen;
    std::string server_addr;

    WiredTigerUDPClient(WiredTigerUDPFactory *factory, int id,
                        boost::uuids::random_generator &uuid_gen,
                        std::string server_addr);
    ~WiredTigerUDPClient();
    int do_operation(Operation *op) override;
    int reset() override;
    void close() override;

   private:
    int sockfd;

    int do_update(char *key_buffer, char *value_buffer);
    int do_insert(char *key_buffer, char *value_buffer);
    int do_read(char *key_buffer, char **value);
    int do_scan(char *key_buffer, long scan_length);
    int do_read_modify_write(char *key_buffer, char *value_buffer);
};

struct WiredTigerUDPFactory : public ClientFactory {
    std::atomic<int> client_id;
    std::string server_addr;
    bool print_stats;
    WiredTigerUDPFactory(const char *server_addr);

    WiredTigerUDPClient *create_client() override;
    void destroy_client(Client *client) override;
};

#endif  // YCSB_WT_CLIENT_H
