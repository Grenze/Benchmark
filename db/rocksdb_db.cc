//
// Created by wujy on 1/23/19.
//
/*
#include "rocksdb_db.h"
#include <iostream>
#include "rocksdb/filter_policy.h"
#include "rocksdb/cache.h"
#include "leveldb_config.h"
#include "rocksdb/flush_block_policy.h"

using namespace std;

namespace ycsbc {
        RocksDB::RocksDB(const char *dbfilename) :noResult(0){
        //get rocksdb config
        ConfigLevelDB config = ConfigLevelDB();
        int bloomBits = config.getBloomBits();
        size_t blockCache = config.getBlockCache();
        bool seekCompaction = config.getSeekCompaction();
        bool compression = config.getCompression();
        bool directIO = config.getDirectIO();
        //set options
        rocksdb::Options options;
        rocksdb::BlockBasedTableOptions bbto;
        options.create_if_missing = true;
        if(!compression)
            options.compression = rocksdb::kNoCompression;
        if(bloomBits>0) {
            bbto.filter_policy.reset(rocksdb::NewBloomFilterPolicy(bloomBits));
        }
        bbto.block_cache = rocksdb::NewLRUCache(blockCache);
        options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(bbto));

        rocksdb::Status s = rocksdb::DB::Open(options,dbfilename,&db_);
        if(!s.ok()){
            cerr<<"Can't open rocksdb "<<dbfilename<<endl;
            exit(0);
        }
        cout<<"\nbloom bits:"<<bloomBits<<"bits\ndirectIO:"<<(bool)directIO<<"\nseekCompaction:"<<(bool)seekCompaction<<endl;
    }

    int RocksDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                      std::vector<KVPair> &result) {
        string value;
        rocksdb::Status s = db_->Get(rocksdb::ReadOptions(),key,&value);
        if(s.ok()) return DB::kOK;
        if(s.IsNotFound()){
            noResult++;
            cout<<noResult<<endl;
            return DB::kOK;
        }else{
            cerr<<"read error"<<endl;
            exit(0);
        }
    }


    int RocksDB::Scan(const std::string &table, const std::string &key, int len, const std::vector<std::string> *fields,
                      std::vector<std::vector<KVPair>> &result) {
        auto it=db_->NewIterator(rocksdb::ReadOptions());
        it->Seek(key);
        std::string val;
        std::string k;
        for(int i=0;i<len&&it->Valid();i++){
            k = it->key().ToString();
            val = it->value().ToString();
            it->Next();
        }
        return DB::kOK;
    }

    int RocksDB::Insert(const std::string &table, const std::string &key,
                        std::vector<KVPair> &values){
        rocksdb::Status s;
        for(KVPair &p:values){
            s = db_->Put(rocksdb::WriteOptions(),key,p.second);
            if(!s.ok()){
                cerr<<"insert error\n"<<endl;
                exit(0);
            }
        }
        return DB::kOK;
    }

    int RocksDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        return Insert(table,key,values);
    }

    int RocksDB::Delete(const std::string &table, const std::string &key) {
        vector<DB::KVPair> values;
        return Insert(table,key,values);
    }

    void RocksDB::printStats() {
        string stats;
        db_->GetProperty("rocksdb.stats",&stats);
        cout<<stats<<endl;
    }

    RocksDB::~RocksDB() {
        delete db_;
    }
}*/
