//
// Created by lingo on 19-4-6.
//

#ifndef YCSB_C_SOFTDB_DB_H
#define YCSB_C_SOFTDB_DB_H

#include "softdb/db.h"
#include "core/db.h"
#include <string>
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include "leveldb_config.h"

using std::string;

namespace ycsbc {
    class SoftDB : public DB {
    public:
        SoftDB(const char *dbfilename);

        int Read(const std::string &table, const std::string &key,
                 const std::vector <std::string> *fields,
                 std::vector <KVPair> &result);

        int Scan(const std::string &table, const std::string &key,
                 int len, const std::vector <std::string> *fields,
                 std::vector <std::vector<KVPair>> &result);

        int Insert(const std::string &table, const std::string &key,
                   std::vector <KVPair> &values);

        int Update(const std::string &table, const std::string &key,
                   std::vector <KVPair> &values);


        int Delete(const std::string &table, const std::string &key);

        void printStats();

        ~SoftDB();

    private:
        softdb::DB *db_;
        unsigned noResult;
    };
}


#endif //YCSB_C_SOFTDB_DB_H
