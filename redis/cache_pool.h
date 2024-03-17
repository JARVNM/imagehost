#ifndef CACHEPOOL_H_
#define CACHEPOOL_H_

#include <hiredis/hiredis.h>
#include <condition_variable>
#include <iostream>
#include <list>
#include <map>
#include <mutex>
#include <vector>
#include "../core/dlog.h"

#define REDIS_COMMAND_SIZE 300
#define FIELD_ID_SIZE 100
#define VALUES_ID_SIZE 1024

typedef char (*RFIELDS)[FIELD_ID_SIZE];
typedef char (*RVALUES)[VALUES_ID_SIZE];

class CachePool;

class CacheConn {

public:
    CacheConn(const char* server_ip, int server_port, int db_index, const char* password, const char* pool_name = "");
    CacheConn(CachePool* pCachepool);
    virtual ~CacheConn();

    int Init();
    void DeInit();
    const char* GetPoolName();

    bool IsExist(std::string& key);
    long Del(std::string key);

    std::string Get(std::string key);
    std::string Set(std::string key, std::string value);
    std::string SetEx(std::string key, int timeout, std::string value);

    bool MGet(const std::vector<std::string> &keys, std::map<std::string, std::string> &ret_value);
    int Incr(std::string key, int64_t& value);
    int Decr(std::string key, int64_t& value);

    //hash
    long Hdel(std::string key, std::string field);
    int Hget(std::string key, char* field, char* value);
    bool HgetAll(std::string key, std::map<std::string, std::string>& ret_value);
    long Hset(std::string key, std::string field, std::string value);

    long HincrBy(std::string key, std::string field, long value);
    long IncrBy(std::string key, long value);
    std::string Hmset(std::string key, std::map<std::string, std::string>& hash);
    bool Hmget(std::string key, std::list<std::string>& field, std::list<std::string>& ret_value);

    //list
    long Lpush(std::string key, std::string value);
    long Rpush(std::string key, std::string value);
    long Llen(std::string key);
    bool Lrange(std::string key, long start, long end, std::list<std::string> &ret_value);
    //zset
    int ZsetExit(std::string key, std::string member);
    int ZsetAdd(std::string key, long score, std::string member);
    int ZsetZrem(std::string, std::string member);
    int ZsetIncr(std::string key, std::string member);
    int ZsetZcard(std::string card);
    int ZsetZrevrange(std::string key, int from_pos, int end_pos, RVALUES values, int& get_num);
    int ZsetGetScore(std::string key, std::string member);

    bool FlushDb();

private:
    CachePool* cache_pool_;
    redisContext* context_;
    uint64_t last_connect_time_;
    uint16_t server_port_;
    std::string server_ip_;
    std::string password_;
    uint16_t db_index_;
    std::string pool_name_;
};

class CachePool {

public:
    CachePool(const char* pool_name, const char* server_ip, int server_port,
                int db_index, const char* password, int max_conn_cnt);

    virtual ~CachePool();

    int Init();

    //获取空闲的连接资源
    CacheConn* GetCacheConn(const int timeout = 0);

    void RelCacheConn(CacheConn* cache_conn);

    const char* GetPoolName();
    const char* GetServerIp();
    const char* GetPassword();
    int GetServerPort();
    int GetDBIndex();

private:
    std::string pool_name_;
    std::string server_ip_;
    std::string password_;
    int m_server_port;
    int db_index_;//mysql database name, redis db index

    int cur_conn_cnt_;
    int max_conn_cnt_;
    std::list<CacheConn*> free_list_;

    std::mutex m_mutex;
    std::condition_variable cond_var_;
    bool abort_request_ = false;
};

class CacheManager {

public:
    CacheManager();
    virtual ~CacheManager();
    static void SetConfPath(const char* conf_path);
    static CacheManager* getInstance();

    int Init();
    CacheConn* GetCacheConn(const char* pool_name);
    void RelCacheConn(CacheConn* cache_conn);

private:

    static CacheManager* s_cache_manager;
    std::map<std::string, CachePool*> m_cache_pool_map;
    static std::string conf_path_;
};

class AutoRelCacheConn {

public:
    AutoRelCacheConn(CacheManager* manager, CacheConn* conn)
        : manager_(manager), conn_(conn) {}

    ~AutoRelCacheConn() {
        if(manager_)
            manager_->RelCacheConn(conn_);
    }

private:

    CacheManager* manager_ = NULL;
    CacheConn* conn_ = NULL;
};

#define AUTO_REL_CACHECONN(m, c) AutoRelCacheConn autorelcacheconn(m, c)
#endif