#include "kvstore.h"
#include <string>
#include <fcntl.h>
#include <queue>
#include <cassert>

struct kvspair
{
    std::pair<key_type, value_type> kvpair;//键值对
    uint64_t stamp;//时间戳
    int i;//索引
};

//运算符重载 <
bool operator < (kvspair a, kvspair b)
{
    //如果两个 kvspair 的键相同，则比较它们的时间戳，时间戳小的排在前面
    if(a.kvpair.first == b.kvpair.first) {
        return a.stamp < b.stamp;
    }
    else {
        //否则，比较两个 kvspair 的键，键大的排在前面
        return a.kvpair.first > b.kvpair.first;
    }
}

struct kvsopair
{
    key_type key;//键
    uint64_t valueLen;//值的长度
    uint64_t stamp;//时间戳
    off_t offset;//偏移量
    int i;//索引
};

//运算符重载 <
bool operator < (kvsopair a, kvsopair b)
{
    if(a.key == b.key) {
        return a.stamp < b.stamp;
    }
    else {
        return a.key > b.key;
    }
}

struct lipair
{
    int level;
    int id;
    std::string file;
};

//运算符重载 <
bool operator < (lipair a, lipair b)
{
    if(a.level == b.level)
        return a.id > b.id;
    else return a.level > b.level;
}

KVStore::KVStore(const std::string &dir, const std::string &vlog) : KVStoreAPI(dir, vlog)
{
    this->memTable = new MemTable<key_type, value_type>(0.5, 8192);
    this->dir_path = dir;
    this->vlog_path = vlog;
    this->stamp = 0;
    this->head = 0;
    this->tail = 0;
    this->test_mode = 0;
    this->bloomSize = 8192;
    //定义一个优先队列 heap，用于存储 lipair 对象
    std::priority_queue<lipair> heap;
    //定义一个字符串向量 files，用于存储目录中的文件名
    std::vector <std::string> files;
    //扫描目录 dir_path，将文件名存储到 files 向量中
    utils::scanDir(dir_path, files);
    if(utils::fileExists(vlog_path)) {
        //获取vlog文件的尾部和头部偏移量
        tail = utils::seek_data_block(vlog_path);
        head = utils::get_end_offset(vlog_path);
        //打开vlog文件
        int fd = open(vlog_path.c_str(), O_RDWR, 0644);
        //将文件指针移动到尾部偏移量
        lseek(fd, tail, SEEK_SET);
        char buf[1024 * 48 + 5];
        while(tail < head) {
            //读取一个字节，检查是否为 MAGIC 标志
            read(fd, buf, 1);
            while(buf[0] != (char)MAGIC) {
                tail ++;
                read(fd, buf, 1);
            }
            //
            //读取日志前缀，获取校验和、键和值的长度
            read(fd, buf, VLOGPADDING - 1);
            uint16_t crc = *(uint16_t*)buf;
            uint64_t key = *(uint64_t*)(buf + 2);
            uint32_t vlen = *(uint32_t*)(buf + 10);
            //读取值并计算校验和，验证数据完整性
            read(fd, buf, vlen);
            std::string value(buf);
            uint16_t check_sum = utils::generate_checksum(key, vlen, value);
            if(check_sum == crc) {
                break;
            }
            //更新尾部偏移量 tail
            tail += VLOGPADDING + vlen;
        }
        close(fd);
    }
    //处理目录中的文件
    for(int i = 0; i < files.size(); i ++){
        //检查文件名是否包含 .
        if(~ files[i].find('.')) {
            //去掉文件扩展名
            std::string file = files[i].substr(0, files[i].find('.'));
            //获取层级和 id
            int level = atoi(file.substr(0, file.find('-')).c_str());
            int id = atoi(file.substr(file.find('-') + 1, file.length()).c_str());
            //将文件信息压入优先队列
            heap.push(lipair{level, id, files[i]});
        }
    }
    //从优先队列中取出文件信息
    layers.push_back(std::vector<SSTable<key_type, value_type>*>());
    while(!heap.empty()) {
        lipair p = heap.top();
        heap.pop();
        while(layers.size() <= p.level) {
            layers.push_back(std::vector<SSTable<key_type, value_type> *>());
        }
        //将文件信息添加到相应层级的 SSTable 向量中
        layers[p.level].push_back(new SSTable<key_type, value_type>(p.level, p.id, p.file, dir, vlog, bloomSize));
        //更新时间戳
        stamp = std::max(layers[p.level].back() -> getStamp() + 1, stamp);
    }
}

KVStore::~KVStore()
{
    //检查内存中的跳表 memTable 是否包含键值对
    if(memTable -> get_numkv()) {
        //将 memTable 转换为 SSTable 并添加到第 0 层
        layers[0].push_back(memTable -> convertSSTable(layers[0].size(), stamp ++, dir_path, vlog_path));
    }
    //释放 memTable 占用的内存
    delete memTable;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s)
{
    if(memTable -> size() >= SSTABLESIZE)
    {
        layers[0].push_back(memTable -> convertSSTable(layers[0].size(), stamp ++, dir_path, vlog_path));
        delete memTable;
        for(int i = 0; i < layers.size() && layers[i].size() > (1 << i + 2); i ++)
            compaction(i);
        memTable = new MemTable<key_type, value_type>(0.5, bloomSize);
    }
    memTable -> put(key, s);
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key)
{
    std::string val = memTable -> get(key);
    if (val == "~DELETED~")
        return "";
    else if(val != "")
        return val;
    for(int i = 0; i < layers.size(); i ++)
        for(int j = layers[i].size() - 1; j >= 0; j --)
            if(test_mode == 1 || test_mode == 2 || layers[i][j] -> query(key))
            {
                if(test_mode == 2) val = layers[i][j] -> get_fromdisk(key);
                else val = layers[i][j] -> get(key);
                if(val == "~DELETED~") return "";
                else if(val != "") return val;
            }
    return "";
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key)
{
    if(get(key) != "")
    {
        put(key, "~DELETED~");
        return true;
    }
    return false;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset()
{
    for(int i = 0; i < layers.size(); i ++)
        for(int j = layers[i].size() - 1; j >= 0; j --)
        {
            layers[i][j] -> delete_disk();
            delete layers[i][j];
            layers[i].pop_back();
        }
    delete memTable;
    utils :: rmfile(vlog_path);
    std::vector <std::string> files;
    utils :: scanDir(dir_path, files);
    for(int i = 0; i < files.size(); i ++)
        utils :: rmfile(files[i]);
    memTable = new MemTable<key_type, value_type>(0.5, bloomSize);
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list)
{
    std::priority_queue<kvspair> heap;
    std::vector<std::vector<std::pair<key_type, value_type>>> parts;
    std::vector<int> iters;
    parts.push_back(memTable -> scan(key1, key2));
    iters.push_back(0);
    if(iters.back() != parts.back().size())
    {
        heap.push(kvspair{parts.back()[0], stamp, 0});
        iters.back() ++;
    }
    for(int i = 0, sum_s = 0; i < layers.size(); sum_s += layers[i].size(), i ++)
        for(int j = 0; j < layers[i].size(); j ++)
        {
            parts.push_back(layers[i][j] -> scan(key1, key2));
            iters.push_back(0);
            if(iters.back() != parts.back().size())
            {
                heap.push(kvspair{parts.back()[0], layers[i][j] -> getStamp(), sum_s + j + 1});
                iters.back() ++;
            }
        }
    key_type last_delete = -0x7ffffff;
    while(!heap.empty())
    {
        kvspair p = heap.top();
        heap.pop();
        if(last_delete != p.kvpair.first)
        {
            if(p.kvpair.second == "~DELETED~")
                last_delete = p.kvpair.first;
            else
                list.push_back(p.kvpair);
        }
        if(iters[p.i] != parts[p.i].size())
        {
            heap.push(kvspair{parts[p.i][iters[p.i]], p.stamp, p.i});
            iters[p.i] ++;
        }
    }
}

/**
 * This reclaims space from vLog by moving valid value and discarding invalid value.
 * chunk_size is the size in byte you should AT LEAST recycle.
 */
void KVStore::gc(uint64_t chunk_size)
{
    int fd = open(vlog_path.c_str(), O_RDWR, 0644);
    lseek(fd, tail, SEEK_SET);
    char buf[1024 * 48 + 5];
    read(fd, buf, 1);
    uint64_t read_len = 0, vlen;
    key_type key;
    while(read_len < chunk_size && buf[0] == (char)MAGIC)
    {
        read(fd, buf, VLOGPADDING - 1);
        key = *(uint64_t*)(buf + 2);
        vlen = *(uint32_t*)(buf + 10);
        read(fd, buf, vlen);
        bool flag = true;
        if(memTable -> get(key) == "")
            for(int i = 0; i < layers.size() && flag; i ++)
                for(int j = layers[i].size() - 1; j >= 0 && flag; j --)
                    if(layers[i][j] -> query(key))
                    {
                        off_t off = layers[i][j] -> get_offset(key);
                        if(off != 1)
                        {
                            buf[vlen] = 0;
                            if(off != 2 && off == read_len + tail)
                                put(key, buf);
                            flag = false;
                        }
                    }
        read_len += VLOGPADDING + vlen;
        read(fd, buf, 1);
    }
    close(fd);
    layers[0].push_back(memTable -> convertSSTable(layers[0].size(), stamp ++, dir_path, vlog_path));
    delete memTable;
    for(int i = 0; i < layers.size() && layers[i].size() > (1 << i + 2); i ++)
        compaction(i);
    memTable = new MemTable<key_type, value_type>(0.5, bloomSize);
    utils::de_alloc_file(vlog_path, tail, read_len);
    tail = read_len + tail;
}

void KVStore::compaction(int level)
{
    key_type min_k = 0x7fffffff, max_k = 0;
    int compact_size = level ? layers[level].size() / 2 : layers[level].size();
    uint64_t max_step = layers[level][compact_size - 1] -> getStamp();
    while(compact_size < layers[level].size() && layers[level][compact_size] -> getStamp() <= max_step)
        compact_size ++;
    for(int i = 0; i < compact_size; i ++)
    {
        max_k = std::max(max_k, layers[level][i] -> get_maxk());
        min_k = std::min(min_k, layers[level][i] -> get_mink());
    }
    if(level + 1 == layers.size())
        layers.push_back(std::vector<SSTable<key_type, value_type>*>());
    std::vector<int> idxes, iters;
    std::priority_queue<kvsopair> heap;
    for(int i = 0; i < layers[level + 1].size(); i ++)
        if(layers[level + 1][i] -> get_mink() <= max_k && layers[level + 1][i] -> get_maxk() >= min_k)
        {
            idxes.push_back(i);
            iters.push_back(0);
        }
    for(int i = idxes.size() - 1; i >= 0; i --)
        if(iters[i] != layers[level + 1][idxes[i]] -> get_numkv())
        {
            SSTable<key_type, value_type> *sst = layers[level + 1][idxes[i]];
            heap.push(kvsopair{sst -> get_keys()[0], sst -> get_valueLens()[0], sst -> getStamp(), sst -> get_offsets()[0], i});
            iters[i] ++;
        }
    for(int i = 0; i < compact_size; i ++)
    {
        iters.push_back(0);
        if(iters.back() != layers[level][i] -> get_numkv())
        {
            SSTable<key_type, value_type> *sst = layers[level][i];
            heap.push(kvsopair{sst -> get_keys()[0], sst -> get_valueLens()[0], sst -> getStamp(), sst -> get_offsets()[0], i + idxes.size()});
            iters.back() ++;
        }
    }
    std::vector<kvsopair> list;
    while(!heap.empty())
    {
        kvsopair p = heap.top();
        heap.pop();
        if(list.empty() || p.key != list.back().key)
            list.push_back(p);
        else
            assert(list.back().stamp >= p.stamp);
        if(p.i >= idxes.size())
        {
            int i = p.i - idxes.size();
            assert(layers[level][i] -> get_numkv() == layers[level][i] -> get_keys().size());
            if(iters[p.i] != layers[level][i] -> get_numkv())
            {
                SSTable<key_type, value_type> *sst = layers[level][i];
                heap.push(kvsopair{sst -> get_keys()[iters[p.i]], sst -> get_valueLens()[iters[p.i]], p.stamp, sst -> get_offsets()[iters[p.i]], p.i});
                iters[p.i] ++;
            }
        }
        else
        if(iters[p.i] != layers[level + 1][idxes[p.i]] -> get_numkv())
        {
            SSTable<key_type, value_type> *sst = layers[level + 1][idxes[p.i]];
            heap.push(kvsopair{sst -> get_keys()[iters[p.i]], sst -> get_valueLens()[iters[p.i]], p.stamp, sst -> get_offsets()[iters[p.i]], p.i});
            iters[p.i] ++;
        }
    }
    for(int i = idxes.size() - 1; i >= 0; i --)
    {
        std::vector<SSTable<key_type, value_type>*>::iterator iter = layers[level + 1].begin() + idxes[i];
        layers[level + 1][idxes[i]] -> delete_disk();
        delete layers[level + 1][idxes[i]];
        layers[level + 1].erase(iter);
    }
    for(int i = compact_size - 1; i >= 0; i --)
    {
        std::vector<SSTable<key_type, value_type>*>::iterator iter = layers[level].begin() + i;
        layers[level][i] -> delete_disk();
        delete layers[level][i];
        layers[level].erase(iter);
    }
    for(int i = 0; i < layers[level].size(); i ++)
        layers[level][i] -> set_id(i);
    for(int i = 0; i < layers[level + 1].size(); i ++)
        layers[level + 1][i] -> set_id(i);
    int max_numkv = (SSTABLESIZE - bloomSize - 32) / 20;
    for(int i = 0; i < list.size(); i += max_numkv)
    {
        key_type max_k = 0, min_k = 0x7fffffff;
        uint64_t new_step = 0, num_kv = std::min(max_numkv, (int)list.size() - i);
        std::vector<key_type> keys;
        std::vector<uint64_t> offsets, valueLens;
        bloomFilter * bloom_p = new bloomFilter(bloomSize, 3);
        for(int j = i; j < std::min(i + max_numkv, (int)list.size()); j ++ )
        {
            max_k = std::max(max_k, list[j].key);
            min_k = std::min(min_k, list[j].key);
            new_step = std::max(new_step, list[j].stamp);
            keys.push_back(list[j].key);
            offsets.push_back(list[j].offset);
            valueLens.push_back(list[j].valueLen);
            bloom_p -> insert(list[j].key);
        }
        SSTable<key_type, value_type> *sst = new SSTable<key_type, value_type>({new_step, num_kv, max_k, min_k}, level + 1, layers[level + 1].size(), bloom_p, keys, offsets, valueLens, dir_path, vlog_path);
        sst -> write_disk();
        layers[level + 1].push_back(sst);
    }
}