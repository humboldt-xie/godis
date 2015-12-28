/*
 * =====================================================================================
 *
 *       Filename:  ledis.cpp
 *
 *    Description:  
 *
 *        Version:  1.0
 *        Created:  05/21/2014 07:17:58 PM
 *       Revision:  none
 *       Compiler:  gcc
 *
 *         Author:  YOUR NAME (), 
 *        Company:  
 *
 * =====================================================================================
 */
#include <assert.h>
#include "leveldb/db.h"
#include <leveldb/write_batch.h>
#include "stdlib.h"
#include "bob_hash.h"
#include <inttypes.h>  
#include <list>
#include "leveldb/comparator.h"
using namespace std;

class Db
{
public:
	leveldb::DB* m_db;
	leveldb::Options options;
	int m_bucket;
	string ToStr(int32_t v)
	{
		char buf[20];
		sprintf(buf,"%d",v);
		return buf;
	}
	string ToStr(int64_t v)
	{
		char buf[20];
		sprintf(buf,"%"PRId64,v);
		return buf;
	}

	leveldb::Status open(string path,int bucket)
	{
		m_db=NULL;
		options.create_if_missing = true;
		//options.comparator = &cmp;
		leveldb::Status status = leveldb::DB::Open(options, path.c_str(), &m_db);	
		assert(status.ok());
	}
	void hencode(const string &prev,const string &next,string &value)
	{
		
	}

	void GetPoint(const string &invalue,string &prev,string &next,string &value)
	{
			size_t pos=invalue.find_first_of("-");
			if(pos==string::npos){
				return;
			}
			size_t end=invalue.find_first_of("-",pos+1);
			if(end==string::npos){
				return;
			}
			prev=invalue.substr(0,pos);
			next=invalue.substr(pos+1,end-pos-1);
			value=invalue.substr(end+1);
			printf("%s %s %s\n",prev.c_str(),next.c_str(),value.c_str());
	}

	leveldb::Status HGET(string key,string subkey,string & value){
		leveldb::Status s = m_db->Get(leveldb::ReadOptions(), key+"-"+subkey, &value);
		return s;
	}

	leveldb::Status HGETALL(string key,list<string> &result){
		string next;
		leveldb::Status s = m_db->Get(leveldb::ReadOptions(), key, &next);
		while(next!=""){
			string prev,value;
			m_db->Get(leveldb::ReadOptions(), key+"-"+next, &value);
			result.push_back(next);
			next="";
			GetPoint(value,prev,next,value);
			result.push_back(value);
		}
		return s;
	}
	
	bool HSET(const string &key,const string &subkey,const string  &value){
		string first;
		string first_value;
		string cur_value,next;
		leveldb::WriteBatch write_batch;
		leveldb::Status s;
		s=m_db->Get(leveldb::ReadOptions(), key+"-"+subkey, &cur_value);
		if(!s.IsNotFound()){
			string prev,next,cvalue;
			GetPoint(cur_value,prev,next,cvalue);
			s=m_db->Put(leveldb::WriteOptions(),key+"-"+subkey,prev+"-"+next+"-"+value);
			return s.ok();
		}
		s=m_db->Get(leveldb::ReadOptions(), key, &first);
		if(first!=""){
			m_db->Get(leveldb::ReadOptions(), key+"-"+first, &first_value);
			string prev,cvalue;
			GetPoint(first_value,prev,next,cvalue);
			write_batch.Put(key+"-"+first,subkey+"-"+next+"-"+cvalue);
		}
		string encode_value="-"+first+"-"+value;
		write_batch.Put(key+"-"+subkey,encode_value);
		write_batch.Put(key,subkey);
		s=m_db->Write(leveldb::WriteOptions(),&write_batch);
		return s.ok();
	}

	void Seek(){
		leveldb::Iterator* it = m_db->NewIterator(leveldb::ReadOptions());
		for (it->SeekToFirst();
				it->Valid() ;
				it->Next()) {
			string key=it->key().ToString();
			string value=it->value().ToString();
			printf("seek:%s %s\n",key.c_str(),value.c_str());
		}
	}
};

int main()
{
	Db db;
	leveldb::Status s=db.open("./db/",10000);
	string key="a",subkey="b",value="b";
	do{
	string invalue="aa-bb-cccccccc",prev,next,value;
	db.GetPoint(invalue,prev,next,value);
	}while(0);
	//db.Put(key,value);
	//string k="a",v="";
	//db.Get(k,v);
	//printf("%s\n",v.c_str());
	//string tkey;
	//db.Tkey(tkey,key,0,0);
	//printf("%s\n",tkey.c_str());
	//db.Seek(7814);
	db.HSET(key,subkey,value);
	db.HSET("a","d","11j");
	db.HSET("a","e","122");
	db.HSET("a","f","13333");

	list<string> all;
	db.HGETALL("a",all);
	printf("%d\n",all.size());
	list<string>::iterator it=all.begin(),end=all.end();
	for(;it!=end; it++){
		printf("=%s\n",it->c_str());
	}
	db.Seek();
	while(1)sleep(1);
	return 0;
}
