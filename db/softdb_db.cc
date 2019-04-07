//
// Created by lingo on 19-4-6.
//


#include "softdb_db.h"
#include <iostream>

using namespace std;

namespace ycsbc {
    SoftDB::SoftDB(const char *dbfilename) :noResult(0){
        //get softdb config
        //ConfigLevelDB config = ConfigLevelDB();
        //int bloomBits = config.getBloomBits();
        //size_t blockCache = config.getBlockCache();
        //bool seekCompaction = config.getSeekCompaction();
        //bool compression = config.getCompression();
        //bool directIO = config.getDirectIO();
        //set options
        softdb::Options options;
        options.create_if_missing = true;
        //if(!compression)
        //    options.compression = softdb::kNoCompression;
        //if(bloomBits>0)
        //    options.filter_policy = softdb::NewBloomFilterPolicy(bloomBits);
        //options.exp_ops.seekCompaction = seekCompaction;
        //options.exp_ops.directIO = directIO;
        //options.block_cache = softdb::NewLRUCache(blockCache);

        softdb::Status s = softdb::DB::Open(options,dbfilename,&db_);
        if(!s.ok()){
            cerr<<"Can't open softdb "<<dbfilename<<endl;
            exit(0);
        }
        //cout<<"\nbloom bits:"<<bloomBits<<"bits\ndirectIO:"<<(bool)directIO<<"\nseekCompaction:"<<(bool)seekCompaction<<endl;
    }

    int SoftDB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                      std::vector<KVPair> &result) {
        string value;
        softdb::Status s = db_->Get(softdb::ReadOptions(),key,&value);
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


    int SoftDB::Scan(const std::string &table, const std::string &key, int len, const std::vector<std::string> *fields,
                      std::vector<std::vector<KVPair>> &result) {
        auto it=db_->NewIterator(softdb::ReadOptions());
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

    int SoftDB::Insert(const std::string &table, const std::string &key,
                        std::vector<KVPair> &values){
        softdb::Status s;
        for(KVPair &p:values){
            s = db_->Put(softdb::WriteOptions(),key,p.second);
            if(!s.ok()){
                cerr<<"insert error\n"<<endl;
                exit(0);
            }
        }
        return DB::kOK;
    }

    int SoftDB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
        return Insert(table,key,values);
    }

    int SoftDB::Delete(const std::string &table, const std::string &key) {
        vector<DB::KVPair> values;
        return Insert(table,key,values);
    }

    void SoftDB::printStats() {
        //string stats;
        //db_->GetProperty("softdb.stats",&stats);
        //cout<<stats<<endl;
        cout<<"softdb stats not implemented yet"<<endl;
    }

    SoftDB::~SoftDB() {
        delete db_;
    }
}
