#include "cache_pool.h"
#include <stdlib.h>
#include <string.h>

#define MIN_CACHE_CONN_CNT 2
#define MAX_CACHE_CONN_FAIL_NUM 10

#include "../core/config_file_reader.h"

CacheManager * CacheManager::s_cache_manager = NULL;
std::string CacheManager::conf_path_ = "ih_http_server.conf";
CacheConn::CacheConn(const char* server_ip, int server_port, int db_index, 
    const char* password, const char* pool_name) {

    server_ip_ = server_ip;
    server_port_ = server_port;
    db_index_ = db_index;
    password_ = password;
    pool_name_ = pool_name;
    context_ = NULL;
    last_connect_time_ = 0;
}

CacheConn::CacheConn(CachePool* pCachePool) {

    cache_pool_ = pCachePool;
    if(pCachePool) {

        server_ip_ = cache_pool_->GetServerIp();
        server_port_ = cache_pool_->GetServerPort();
        db_index_ = cache_pool_->GetDBIndex();
        password_ = cache_pool_->GetPassword();
        pool_name_ = cache_pool_->GetPoolName();
    }
    else {
        LogError("pCachePool is NULL\n");
    }

    context_ = NULL;
    last_connect_time_ = 0;
}

CacheConn::~CacheConn() {

    if(context_) {

        redisFree(context_);
        context_ = NULL;
    }
}


int CacheConn::Init() {

    if (context_) {
        return 0;
    }

    uint64_t cur_time = (uint64_t) time(NULL);
    if (cur_time < last_connect_time_ + 1) {

        printf("cur_time:%lu, m_last_connect_time:%lu\n", cur_time, last_connect_time_);
        return 1;
    }

    last_connect_time_ = cur_time;

    struct timeval timeout = {0, 1000000};

    context_ = redisConnectWithTimeout(server_ip_.c_str(), server_port_, timeout);

    if (!context_ || context_->err) {

        if (context_) {
            LogError("redisConnect failed: %s\n", context_->errstr);
            redisFree(context_);
            context_ = NULL;
        } else {
            LogError("redisConnect failed\n");
        }
        return 1;
    }

    redisReply* reply;
    if(!password_.empty()) {
        reply - (redisReply*)redisCommand(context_, "AUTH %s", password_.c_str());

        if(!reply || reply->type == REDIS_REPLY_ERROR) {
            LogError("Authentication failure:%p\n", reply);
            if(reply)
                freeReplyObject(reply);

            return -1;
        }
        else
            LogInfo("Authentication success\n");

        freeReplyObject(reply);
    }

    reply = (redisReply*)redisCommand(context_, "SELECT %d", 0);

    if(reply && (reply->type == REDIS_REPLY_STATUS) && (strncmp(reply->str, "OK", 2) == 0)) {
        freeReplyObject(reply);
        return 0;
    }
    else {
        if(reply)
            LogError("select cache db failed:%s\n", reply->str);

        return 2;
    }
 }

void CacheConn::DeInit() {

    if(context_) {
        redisFree(context_);
        context_ = NULL;
    }
}

const char* CacheConn::GetPoolName() {return pool_name_.c_str();}

std::string CacheConn::Get(std::string key) {

    std::string value;

    if(Init()) {
        return value;
    }

    redisReply* reply = (redisReply*)redisCommand(context_, "GET %s", key.c_str());
    if(!reply) {
        LogError("redisCommand failed:%s\n", context_->errstr);
        redisFree(context_);
        context_ = NULL;
        return value;
    }

    if(reply->type == REDIS_REPLY_STRING) {
        value.append(reply->str, reply->len);
    }

    freeReplyObject(reply);
    return value;
}

std::string CacheConn::Set(std::string key, std::string value) {

    std::string ret_value;
    if(Init()) {
        return ret_value;
    }

    redisReply* reply = (redisReply*)redisCommand(context_, "SET %s %s", key.c_str(), value.c_str());

    if(!reply) {
        LogError("redisCommand failed:%s\n", context_->errstr);
        redisFree(context_);
        context_ = NULL;
        return ret_value;
    }

    ret_value.append(reply->str, reply->len);
    freeReplyObject(reply);
    return ret_value;
}

std::string CacheConn::SetEx(std::string key, int timeout, std::string value) {
    std::string ret_value;

    if(Init()) {
        return ret_value;
    }

    redisReply* reply = (redisReply*)redisCommand(context_, "SETEX %s %d %s", key.c_str(), timeout, value.c_str());
    if(!reply) {
        LogError("redisCommand failed:%s", context_->errstr);
        redisFree(context_);
        context_ = NULL;
        return ret_value;
    }

    ret_value.append(reply->str, reply->len);
    freeReplyObject(reply);
    return ret_value;
}

bool CacheConn::MGet(const std::vector<std::string>& keys,
    std::map<std::string, std::string>& ret_value) {

    if(Init()) {
        return false;
    }

    if(keys.empty()) {
        return false;
    }

    std::string strKey;
    bool bFirst = true;
    for(std::vector<std::string>::const_iterator it = keys.begin();
        it != keys.end(); ++it) {
        if(bFirst) {
            bFirst = false;
            strKey = *it;
        }
        else {
            strKey += " " + *it;
        }
    }

    if(strKey.empty()) {
        return false;
    }

    strKey = "MGET" + strKey;
    redisReply* reply = (redisReply*)redisCommand(context_, strKey.c_str());
    if(!reply) {
        LogError("redisCommand failed: %s", context_->errstr);
        redisFree(context_);
        context_ = NULL;
        return false;
    }

    if(reply->type == REDIS_REPLY_ARRAY) {
        for(size_t i = 0; i < reply->elements; i++) {
            redisReply* child_reply = reply->element[i];
            if(child_reply->type == REDIS_REPLY_STRING)
                ret_value[keys[i]] = child_reply->str;
        }
    }

    freeReplyObject(reply);
    return true;
}

bool CacheConn::IsExist(std::string& key) {
    if(Init()) {
        return false;
    }

    redisReply* reply = (redisReply*)redisCommand(context_, "EXISTS  %s", key.c_str());
    if(!reply) {
        LogError("redisCommand failed: %s", reply->str);
        redisFree(context_);
        context_ = NULL;
        return false;
    }

    long ret_value = reply->integer;
    freeReplyObject(reply);
    if(0 == ret_value)
        return false;
    else
        return true;
}

long CacheConn::Del(std::string key) {
    if(Init()) {
        return 0;
    }

    redisReply* reply = (redisReply*)redisCommand(context_, "DEL %s", key.c_str());
    if(!reply) {
        LogError("redisCommand failed: %s", context_->errstr);
        redisFree(context_);
        context_ = NULL;
        return -1;
    }

    long ret_val  = reply->integer;
    freeReplyObject(reply);
    return ret_val;
}

long CacheConn::Hdel(std::string key, std::string field) {

    if(Init()) {
        return 0;
    }

    redisReply* reply = (redisReply*)redisCommand(context_, "HDEL %s %s", key.c_str(), field.c_str());
    if(!reply) {
        LogError("redisCommand failed: %s", context_->errstr);
        redisFree(context_);
        context_ = NULL;
        return -1;
    }
    
    long ret_value = reply->integer;
    freeReplyObject(reply);
    return ret_value;
}

int CacheConn::Hget(std::string key, char* field, char* value) {

    int retn = 0;
    int len = 0;

    if(Init()) {
        return -1;
    }

    redisReply* reply = (redisReply*)redisCommand(context_, "HGET %s %s", key.c_str(), field);
    if(reply == NULL || reply->type != REDIS_REPLY_STATUS) {
        LogError("hget %s %s error %s\n", key.c_str(), field, context_->errstr);
        retn = -1;
        goto END;
    }

    len = reply->len > VALUES_ID_SIZE ? VALUES_ID_SIZE : reply->len;
    strncpy(value, reply->str, len);
    value[len] = '\0';
END:
    freeReplyObject(reply);
    return retn;
}

bool CacheConn::HgetAll(std::string key, std::map<std::string, std::string>& ret_value) {

    if(Init()) {
        return false;
    }

    redisReply* reply = (redisReply*)redisCommand(context_, "HGETALL %s", key.c_str());
    if(!reply) {
        LogError("redisCommand failed: %s", context_->errstr);
        redisFree(context_);
        context_ = NULL;
        return false;
    }

    if((reply->type == REDIS_REPLY_ARRAY) || (reply->elements %2 == 0)) {
        for(size_t i = 0; i < reply->elements; i += 2) {
            redisReply* field_reply = reply->element[i];
            redisReply* value_reply = reply->element[i + 1];

            std::string field(field_reply->str, field_reply->len);
            std::string value(value_reply->str, field_reply->len);
            ret_value.insert(std::make_pair(field, value));
        }
    }

    freeReplyObject(reply);
    return true;
}

long  CacheConn::Hset(std::string key, std::string field, std::string value) {

    if(Init()) {
        return -1;
    }

    redisReply* reply = (redisReply*)redisCommand(
        context_, "HSET %s %s %s", key.c_str(), field.c_str(), value.c_str());

    if(!reply) {
        LogError("redisCommand failed %s %s %s", key.c_str(), field.c_str(), value.c_str());
        redisFree(context_);
        context_ = NULL;
        return -1;
    }

    long ret_value = reply->integer;
    freeReplyObject(reply);
    return ret_value;
}

bool CacheConn::HgetAll(std::string key, std::map<std::string, std::string>& ret_value) {
    if(Init())
        return false;

    redisReply* reply = (redisReply*)redisCommand(context_, "HGETALL %s", key.c_str());
    if(!reply) {
        LogError("redisCommand failed: %s", context_->errstr);
        redisFree(context_);
        context_ = NULL;
        return false;
    }

    if((reply->type == REDIS_REPLY_ARRAY) && (reply->elements % 2 == 0)) {
        for(size_t i = 0; i < reply->elements; i += 2) {
            redisReply* field_reply = reply->element[i];
            redisReply* value_reply = reply->element[i + 1];
            
            std::string field(field_reply->str, field_reply->len);
            std::string value(value_reply->str, value_reply->len);
            ret_value.insert(std::make_pair(field, value));
        }
    }

    freeReplyObject(reply);
    return true;
}

long CacheConn::Hset(std::string key, std::string field, std::string value) {
    if(Init())
        return -1;

    redisReply* reply = (redisReply*) redisCommand(context_, "HSET %s %s %s",
                                                   key.c_str(), field.c_str(), value.c_str());
    if(!reply) {
        LogError("redisCommand failed: %s", context_->errstr);
        redisFree(context_);
        context_ = NULL;
        return -1;
    }

    long ret_value = reply->integer;
    freeReplyObject(reply);
    return ret_value;
}

long CacheConn::HincrBy(std::string key, std::string field, long value) {
    if(Init())
        return -1;

    redisReply* reply = (redisReply*) redisCommand(context_, "HINCRBY %s %s %ld",
                                                   key.c_str(), field.c_str(), value);
    if(!reply) {
        LogError("redisCommand failed: %s", context_->errstr);
        redisFree(context_);
        context_ = NULL;
        return -1;
    }

    long ret_value = reply->integer;
    freeReplyObject(reply);
    return ret_value;
}

long CacheConn::IncrBy(std::string key, long value) {
    if(Init())
        return -1;

    redisReply* reply = (redisReply*) redisCommand(context_, "INCRBY %s %ld",
                                                   key.c_str(), value);
    if(!reply) {
        LogError("redisCommand failed: %s", context_->errstr);
        redisFree(context_);
        context_ = NULL;
        return -1;
    }

    long ret_long = reply->integer;
    freeReplyObject(reply);
    return ret_long;
}

std::string CacheConn::Hmset(std::string key, std::map <std::string, std::string> &hash) {
    std::string ret_value;
    if(Init())
        return ret_value;

    int argc = hash.size() * 2 + 2;
    const char **argv = new const char* [argc];
    if(!argv)
        return ret_value;

    argv[0] = "HMSET";
    argv[1] = key.c_str();
    int i = 2;
    for(std::map<std::string, std::string>::iterator it = hash.begin(); it !=  hash.end(); it++) {
        argv[i++] = it->first.c_str();
        argv[i++] = it->second.c_str();
    }

    redisReply* reply = (redisReply*) redisCommandArgv(context_, argc, argv, NULL);
    if(!reply) {
        LogError("redisCommand failed: %s", context_->errstr);
        delete[]  argv;
        redisFree(context_);
        context_ = NULL;
        return ret_value;
    }

    ret_value.append(reply->str, reply->len);
    delete argv;
    freeReplyObject(reply);
    return ret_value;
}

bool CacheConn::Hmget(std::string key, std::list <std::string> &field, std::list <std::string> &ret_value) {
    if(Init())
        return false;

    int argc = field.size() + 2;
    const char** argv = new const char* [argc];
    if(!argv) {
        return false;
    }

    argv[0] = "HMGET";
    argv[1] = key.c_str();
    int i = 2;
    for(std::list<std::string>::iterator it = field.begin(); it != field.end(); it++) {
        argv[i++] = it->c_str();
    }

    redisReply* reply = (redisReply*) redisCommandArgv(context_, argc, (const char**)argv, NULL);
    if(!reply) {
        LogError("redisCommand failed: %s", context_->errstr);
        delete[] argv;
        redisFree(context_);
        context_ = NULL;
        return false;
    }

    if(reply->type == REDIS_REPLY_ARRAY) {
        for(size_t i = 0; i < reply->elements; i++) {
            redisReply* value_reply = reply->element[i];
            std::string value(value_reply->str, value_reply->len);
            ret_value.push_back(value);
        }
    }
    delete[] argv;
    freeReplyObject(reply);
    return true;
}

int CacheConn::Incr(std::string key, int64_t &value) {
    if(Init())
        return -1;

    redisReply* reply = (redisReply*)redisCommand(context_, "INCR %s", key.c_str());
    if(!reply) {
        LogError("redisCommand failed: %s", context_->errstr);
        redisFree(context_);
        context_ = NULL;
        return -1;
    }

    value = reply->integer;
    freeReplyObject(reply);
    return 0;
}

int CacheConn::Decr(std::string key, int64_t &value) {
    if(Init())
        return -1;

    redisReply* reply = (redisReply*)(redisReply*)redisCommand(context_, "DECR %s", key.c_str());
    if(!reply) {
        LogError("redisCommand failed: %s", context_->errstr);
        redisFree(context_);
        context_ = NULL;
        return -1;
    }

    value = reply->integer;
    freeReplyObject(reply);
    return value;
}

long CacheConn::Lpush(std::string key, std::string value) {
    if (Init()) {
        return -1;
    }

    redisReply *reply = (redisReply *)redisCommand(context_, "LPUSH %s %s",
                                                   key.c_str(), value.c_str());
    if (!reply) {
        LogError("redisCommand failed:%s\n", context_->errstr);
        redisFree(context_);
        context_ = NULL;
        return -1;
    }

    long ret_value = reply->integer;
    freeReplyObject(reply);
    return ret_value;
}

long CacheConn::Rpush(std::string key, std::string value) {
    if (Init()) {
        return -1;
    }

    redisReply *reply = (redisReply *)redisCommand(context_, "RPUSH %s %s",
                                                   key.c_str(), value.c_str());
    if (!reply) {
        LogError("redisCommand failed:%s\n", context_->errstr);
        redisFree(context_);
        context_ = NULL;
        return -1;
    }

    long ret_value = reply->integer;
    freeReplyObject(reply);
    return ret_value;
}

long CacheConn::Llen(std::string key) {
    if (Init()) {
        return -1;
    }

    redisReply *reply =
            (redisReply *)redisCommand(context_, "LLEN %s", key.c_str());
    if (!reply) {
        LogError("redisCommand failed:%s\n", context_->errstr);
        redisFree(context_);
        context_ = NULL;
        return -1;
    }

    long ret_value = reply->integer;
    freeReplyObject(reply);
    return ret_value;
}

bool CacheConn::Lrange(std::string key, long start, long end,
                       std::list<std::string> &ret_value) {
    if (Init()) {
        return false;
    }

    redisReply *reply = (redisReply *)redisCommand(context_, "LRANGE %s %d %d",
                                                   key.c_str(), start, end);
    if (!reply) {
        LogError("redisCommand failed:%s\n", context_->errstr);
        redisFree(context_);
        context_ = NULL;
        return false;
    }

    if (reply->type == REDIS_REPLY_ARRAY) {
        for (size_t i = 0; i < reply->elements; i++) {
            redisReply *value_reply = reply->element[i];
            std::string value(value_reply->str, value_reply->len);
            ret_value.push_back(value);
        }
    }

    freeReplyObject(reply);
    return true;
}

int CacheConn::ZsetExit(std::string key, std::string member) {
    int retn = 0;
    redisReply *reply = NULL;
    if (Init()) {
        return -1;
    }

    //执行命令
    reply =
            (redisReply *)redisCommand(context_, "zlexcount %s [%s [%s",
                                       key.c_str(), member.c_str(), member.c_str());

    if (reply->type != REDIS_REPLY_INTEGER) {
        LogError("zlexcount: %s,member: %s Error:%s,%s\n", key.c_str(),
                  member.c_str(), reply->str, context_->errstr);
        retn = -1;
        goto END;
    }

    retn = reply->integer;

    END:

    freeReplyObject(reply);
    return retn;
}

int CacheConn::ZsetAdd(std::string key, long score, std::string member) {
    int retn = 0;
    redisReply *reply = NULL;
    if (Init()) {
        LogError("Init() -> failed");
        return -1;
    }

    //执行命令, reply->integer成功返回1，reply->integer失败返回0
    reply = (redisReply *)redisCommand(context_, "ZADD %s %ld %s", key.c_str(),
                                       score, member.c_str());
    // rop_test_reply_type(reply);

    if (reply->type != REDIS_REPLY_INTEGER) {
        printf("ZADD: %s,member: %s Error:%s,%s, reply->integer:%lld, %d\n",
               key.c_str(), member.c_str(), reply->str, context_->errstr,
               reply->integer, reply->type);
        retn = -1;
        goto END;
    }

    END:

    freeReplyObject(reply);
    return retn;
}

int CacheConn::ZsetZrem(std::string key, std::string member) {
    int retn = 0;
    redisReply *reply = NULL;
    if (Init()) {
        LogError("Init() -> failed");
        return -1;
    }

    //执行命令, reply->integer成功返回1，reply->integer失败返回0
    reply = (redisReply *)redisCommand(context_, "ZREM %s %s", key.c_str(),
                                       member.c_str());
    if (reply->type != REDIS_REPLY_INTEGER) {
        printf("ZREM: %s,member: %s Error:%s,%s\n", key.c_str(), member.c_str(),
               reply->str, context_->errstr);
        retn = -1;
        goto END;
    }
    END:

    freeReplyObject(reply);
    return retn;
}
int CacheConn::ZsetIncr(std::string key, std::string member) {
    int retn = 0;
    redisReply *reply = NULL;
    if (Init()) {
        return false;
    }

    reply = (redisReply *)redisCommand(context_, "ZINCRBY %s 1 %s", key.c_str(),
                                       member.c_str());
    // rop_test_reply_type(reply);
    if (strcmp(reply->str, "OK") != 0) {
        printf("Add or increment table: %s,member: %s Error:%s,%s\n",
               key.c_str(), member.c_str(), reply->str, context_->errstr);

        retn = -1;
        goto END;
    }

    END:
    freeReplyObject(reply);
    return retn;
}

int CacheConn::ZsetZcard(std::string key) {
    redisReply *reply = NULL;
    if (Init()) {
        return -1;
    }

    int cnt = 0;

    reply = (redisReply *)redisCommand(context_, "ZCARD %s", key.c_str());
    if (reply->type != REDIS_REPLY_INTEGER) {
        printf("ZCARD %s error %s\n", key.c_str(), context_->errstr);
        cnt = -1;
        goto END;
    }

    cnt = reply->integer;

    END:
    freeReplyObject(reply);
    return cnt;
}
int CacheConn::ZsetZrevrange(std::string key, int from_pos, int end_pos,
                             RVALUES values, int &get_num) {
    int retn = 0;
    redisReply *reply = NULL;
    if (Init()) {
        return -1;
    }
    int i = 0;
    int max_count = 0;

    int count = end_pos - from_pos + 1; //请求元素个数

    //降序获取有序集合的元素
    reply = (redisReply *)redisCommand(context_, "ZREVRANGE %s %d %d",
                                       key.c_str(), from_pos, end_pos);
    if (reply->type != REDIS_REPLY_ARRAY) //如果返回不是数组
    {
        printf("ZREVRANGE %s  error!%s\n", key.c_str(), context_->errstr);
        retn = -1;
        goto END;
    }

    //返回一个数组，查看elements的值(数组个数)
    //通过element[index] 的方式访问数组元素
    //每个数组元素是一个redisReply对象的指针

    max_count = (reply->elements > count) ? count : reply->elements;
    get_num = max_count; //得到结果value的个数

    for (i = 0; i < max_count; ++i) {
        strncpy(values[i], reply->element[i]->str, VALUES_ID_SIZE - 1);
        values[i][VALUES_ID_SIZE - 1] = 0; //结束符
    }

    END:
    if (reply != NULL) {
        freeReplyObject(reply);
    }

    return retn;
}

int CacheConn::ZsetGetScore(std::string key, std::string member) {
    if (Init()) {
        return -1;
    }

    int score = 0;

    redisReply *reply = NULL;

    reply = (redisReply *)redisCommand(context_, "ZSCORE %s %s", key.c_str(),
                                       member.c_str());

    if (reply->type != REDIS_REPLY_STRING) {
        printf("[-][GMS_REDIS]ZSCORE %s %s error %s\n", key.c_str(),
               member.c_str(), context_->errstr);
        score = -1;
        goto END;
    }
    score = atoi(reply->str);

    END:
    freeReplyObject(reply);

    return score;
}

bool CacheConn::FlushDb() {
    bool ret =  false;
    if(Init())
        return false;

    redisReply* reply = (redisReply*)redisCommand(context_, "FLUSHDB");
    if(!reply) {
        LogError("redisCommand failed: %s", context_->errstr);
        redisFree(context_);
        context_ = NULL;
        return false;
    }

    if(reply->type ==  REDIS_REPLY_STRING && strncmp(reply->str, "OK", 2) == 0)
        ret = true;

    freeReplyObject(reply);

    return ret;
}

CachePool::CachePool(const char* pool_name, const char* server_ip, int server_port,
                int db_index, const char* password, int max_conn_cnt) {

    pool_name_ = pool_name;
    server_ip_ = server_ip;
    m_server_port = server_port;
    db_index_ = db_index;
    password_ = password;
    max_conn_cnt_ = max_conn_cnt;
    cur_conn_cnt_ = MIN_CACHE_CONN_CNT;
}

CachePool::~CachePool() {

    {
        std::lock_guard<std::mutex> lock(m_mutex);
        abort_request_ = true;
        cond_var_.notify_all();
    }

    {
        std::lock_guard<std::mutex> lock(m_mutex);
        for(std::list<CacheConn*>::iterator  it = free_list_.begin();
            it != free_list_.end(); it++) {
            CacheConn* pConn = *it;
            delete pConn;
        }
    }

    free_list_.clear();
    cur_conn_cnt_ = 0;
}

int CachePool::Init() {
    for(int i = 0; i < cur_conn_cnt_; i++) {
        CacheConn* pConn = new CacheConn(server_ip_.c_str(), m_server_port, db_index_,
            password_.c_str(), pool_name_.c_str());
        if(pConn->Init()) {
            delete pConn;
            return 1;
        }

        free_list_.push_back(pConn);
    }

    LogInfo("cache pool: %s, list size: %lu\n", pool_name_.c_str(), free_list_.size());
    return 0;
}

CacheConn* CachePool::GetCacheConn(const int timeout) {
    std::unique_lock<std::mutex> lock(m_mutex);
    if(abort_request_) {
        LogInfo("have abort");
        return NULL;
    }

    if(free_list_.empty()) {
        if(cur_conn_cnt_ >= max_conn_cnt_) {
            if(timeout <= 0) {
                LogInfo("wait ms: %d", timeout);
                cond_var_.wait(lock, [this]{
                    return (!free_list_.empty()) | abort_request_;
                });
            }
            else {
                cond_var_.wait_for(lock, std::chrono::milliseconds(timeout),
                    [this]{return (!free_list_.empty()) | abort_request_;});
                if(free_list_.empty())
                    return NULL;
            }

            if(abort_request_)
                LogWarn("have abort");
                return NULL;
        }
        else {
            CacheConn* db_conn = new CacheConn(server_ip_.c_str(), m_server_port, db_index_, 
                                                password_.c_str(), pool_name_.c_str());
            int ret = db_conn->Init();
            if(ret) {
                LogError("Init DBConnection failed");
                delete db_conn;
                return NULL;
            }
            else {
                free_list_.push_back(db_conn);
                cur_conn_cnt_++;
            }
        }
    }

    CacheConn* pConn = free_list_.front();
    free_list_.pop_front();

    return pConn;
}

void CachePool::RelCacheConn(CacheConn* p_cache_conn) {
    std::lock_guard<std::mutex> lock(m_mutex);

    std::list<CacheConn*>::iterator it = free_list_.begin();
    for(; it != free_list_.end(); it++) {
        if(*it == p_cache_conn)
            break;
    }

    if(it == free_list_.end()) {
        free_list_.push_back(p_cache_conn);
        cond_var_.notify_one();
    }
    else {
        LogError("RelDBConn failed");
    }
}

const char* CachePool::GetPoolName() {
    return pool_name_.c_str();
}

const char* CachePool::GetServerIp() {
    return server_ip_.c_str();
}

const char* CachePool::GetPassword() {
    password_.c_str();
}

int CachePool::GetServerPort() {
    return m_server_port;
}

int CachePool::GetDBIndex() {
    return db_index_;
}

CacheManager::CacheManager() {}
CacheManager::~CacheManager() {}

void CacheManager::SetConfPath(const char* conf_path) {
    conf_path_ = conf_path;
}

CacheManager* CacheManager::getInstance() {
    if(!s_cache_manager) {
        s_cache_manager = new CacheManager();
        if(s_cache_manager->Init()) {
            delete s_cache_manager;
            s_cache_manager = NULL;
        }
    }

    return s_cache_manager;
}

int CacheManager::Init() {
    CConfigFileReader config_file(conf_path_.c_str());

    char* cache_instances = config_file.GetConfigName("CacheInstances");
    if(!cache_instances) {
        LogError("not configure CacheIntance");
        return 1;
    }

    char host[64];
    char port[64];
    char db[64];
    char maxconncnt[64];
    CStrExplode instances_name(cache_instances, ',');
    for(uint32_t i = 0; i < instances_name.GetItemCnt(); i++) {
        char* pool_name = instances_name.GetItem(i);
        snprintf(host, 64, "%s_host", pool_name);
        snprintf(port, 64, "%s_port", pool_name);
        snprintf(db, 64, "%s_db", pool_name);
        snprintf(maxconncnt, 64, "%s_maxconncnt", pool_name);

        char* cache_host = config_file.GetConfigName(host);
        char* str_cache_port = config_file.GetConfigName(port);
        char* str_cache_db = config_file.GetConfigName(db);
        char* str_max_conn_cnt = config_file.GetConfigName(maxconncnt);
        if(!cache_host || !str_cache_port || !str_cache_db || !maxconncnt) {
            if(!cache_host)
                LogError("net configure cache instance: {}, cache_host is null", pool_name);
            if(!str_cache_port)
                LogError("net configure cache instance: {}, str_cache_port is null", pool_name);
            if(!str_cache_db)
                LogError("net configure cache instance: {}, str_cache_db is null", pool_name);
            if(!str_max_conn_cnt)
                LogError("net configure cache instance: {}, str_max_conn_cnt is null", pool_name);
            return 2;
        }

        CachePool* pCachePool = new CachePool(pool_name, cache_host, atoi(str_cache_port), atoi(str_cache_db), "", atoi(str_max_conn_cnt));
        if(pCachePool->Init())
            LogError("Init cache pool failed");
            return 3;

        m_cache_pool_map.insert(make_pair(pool_name, pCachePool));
    }
    
    return 0;
}

CacheConn* CacheManager::GetCacheConn(const char* pool_name) {
    std::map<std::string, CachePool*>::iterator  it = m_cache_pool_map.find(pool_name);
    if(it != m_cache_pool_map.end()) {
        return it->second->GetCacheConn();
    }
    else
        return NULL;
}

void CacheManager::RelCacheConn(CacheConn* cache_conn) {
    if(!cache_conn)
        return;

    std::map<std::string, CachePool*>::iterator it = m_cache_pool_map.find(cache_conn->GetPoolName());
    if(it != m_cache_pool_map.end())
        return it->second->RelCacheConn(cache_conn);
}