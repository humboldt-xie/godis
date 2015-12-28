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

class TwoPartComparator : public leveldb::Comparator {
   public:
    // Three-way comparison function:
    //   if a < b: negative result
    //   if a > b: positive result
    //   else: zero result
    int Compare(const leveldb::Slice& a, const leveldb::Slice& b) const {
		const char *d1=a.data();
		const char *d2=b.data();
		if(d1){
			d1=strchr(d1,'-');
		}
		if(d1){
			d1=strchr(d1+1,'-');
		}
		if(d1){
			d1++;
		}
		if(d2){
			d2=strchr(d2,'-');
		}
		if(d2){
			d2=strchr(d2+1,'-');
		}
		if(d2){
			d2++;
		}
		//printf(d1);
		//d2=strchr(d2,'-');
		//d2=strchr(d2,'-');
		if(d1==NULL || d2==NULL){
			return a.compare(b);
		}
		int len1=a.size()-(d1-a.data());
		int len2=b.size()-(d2-b.data());
		int r=memcmp(d1,d2,min(len1,len2));
		if(r==0){
			if(len1<len2)return -1;
			if(len1>len2)return 1;
		}
		return r;
    }

    // Ignore the following methods for now:
    const char* Name() const { return "TwoPartComparator"; }
    void FindShortestSeparator(std::string*, const leveldb::Slice&) const { }
    void FindShortSuccessor(std::string*) const { }
};

class Db
{
public:
	leveldb::DB* m_db;
	leveldb::Options options;
	int m_bucket;
	TwoPartComparator cmp;
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
		string key="bucket",value="0";
		leveldb::Status s = m_db->Get(leveldb::ReadOptions(), key, &value);
		if(s.ok()){
			bucket=atoi(value.c_str());
		}
		m_bucket=bucket;
		value=ToStr(m_bucket);
		s = m_db->Put(leveldb::WriteOptions(), key, value );
		assert(s.ok());
	}

	std::string value;
	void Tkey(string &tkey,const string &key,int32_t node_version,int64_t data_version)
	{
		uint32_t hash=bob_hash(key.c_str(),key.size(),0);
		int32_t idx=hash%m_bucket;
		tkey=ToStr(node_version)+"-"+ToStr(data_version)+"-"+ToStr(idx)+"-"+key;
	}

	leveldb::Status HGET(string key,string subkey,string & value){
		leveldb::Status s = m_db->Get(leveldb::ReadOptions(), key+"-"+subkey, &value);
		return s;
	}

	void GetPoint(const string &invalue,string &prev,string &next,string &value)
	{
			size_t pos=invalue.find_first_of("-");
			printf("%d\n",pos);
			if(pos==string::npos){
				return;
			}
			size_t end=invalue.find_first_of("-",pos+1);
			printf("%d\n",end);
			if(end==string::npos){
				return;
			}
			prev=invalue.substr(0,pos);
			next=invalue.substr(pos+1,end-pos-1);
			value=invalue.substr(end+1);
			printf("%s %s %s\n",prev.c_str(),next.c_str(),value.c_str());
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
	
	int HSET(const string &key,const string &subkey,const string  &value){
		string first;
		string first_value;
		string cur_value,next;
		leveldb::WriteBatch write_batch;
		leveldb::Status s;
		s=m_db->Get(leveldb::ReadOptions(), key+"-"+subkey, &cur_value);
		if(!s.IsNotFound()){
			string prev,next,cvalue;
			GetPoint(cur_value,prev,next,cvalue);
			m_db->Put(leveldb::WriteOptions(),key+"-"+subkey,prev+"-"+next+"-"+value);
			return 1;
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
		m_db->Write(leveldb::WriteOptions(),&write_batch);
		return 1;
	}

	leveldb::Status Get(const string &key,string & value){
		string tkey;
		Tkey(tkey,key,0,0);
		leveldb::Status s = m_db->Get(leveldb::ReadOptions(), tkey, &value);
		return s;
	}

	leveldb::Status Put(const string &key,const string & value){
		string tkey;
		Tkey(tkey,key,0,0);
		leveldb::Status s =m_db->Put(leveldb::WriteOptions(), tkey, value);
		return s;
	}
	leveldb::Status Delete(const string &key){
		string tkey;
		Tkey(tkey,key,0,0);
		leveldb::Status s = m_db->Delete(leveldb::WriteOptions(), tkey);
		return s;
	}
	void Seek(int buck){
		leveldb::Iterator* it = m_db->NewIterator(leveldb::ReadOptions());
		for (it->Seek(string("1-1-")+ToStr(buck)+"-");
				it->Valid() ;
				it->Next()) {
			string key=it->key().ToString();
			printf("seek:%s\n",key.c_str());
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
	while(1)sleep(1);
	return 0;
}
