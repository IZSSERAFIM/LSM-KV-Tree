#include "kvstore.h"
#include <string>
#include <fcntl.h>
#include <queue>
#include <cassert>

//运算符重载 <
bool operator<(kv a, kv b) {
    //如果两个 kv 的键相同，则比较它们的时间戳，时间戳小的排在前面
    if (a.kv_pair.first == b.kv_pair.first) {
        return a.stamp < b.stamp;
    } else {
        //否则，比较两个 kv 的键，键大的排在前面
        return a.kv_pair.first > b.kv_pair.first;
    }
}

//运算符重载 <
bool operator<(kv_info a, kv_info b) {
    if (a.key == b.key) {
        return a.stamp < b.stamp;
    } else {
        return a.key > b.key;
    }
}

//运算符重载 <
bool operator<(sst_info a, sst_info b) {
    if (a.level == b.level)
        return a.id > b.id;
    else return a.level > b.level;
}

void KVStore::process_vlog() {
    if (utils::fileExists(vlog_path)) {
        tail = utils::seek_data_block(vlog_path);
        head = utils::get_end_offset(vlog_path);
        int fd = open(vlog_path.c_str(), O_RDWR, 0644);
        lseek(fd, tail, SEEK_SET);
        char buf[BUFFER_SIZE];
        while (tail < head) {
            read(fd, buf, 1);
            while (buf[0] != (char) MAGIC) {
                tail++;
                read(fd, buf, 1);
            }
            read(fd, buf, VLOGPADDING - 1);
            uint16_t checkSum = *(uint16_t *) buf;
            uint64_t key = *(uint64_t * )(buf + 2);
            uint32_t vlen = *(uint32_t * )(buf + 10);
            read(fd, buf, vlen);
            std::string value(buf);
            uint16_t check_sum = utils::generate_checksum(key, vlen, value);
            if (check_sum == checkSum) {
                break;
            }
            tail = tail + VLOGPADDING + vlen;
        }
        close(fd);
    }
}

void KVStore::process_sst(std::vector <std::string> &files, std::priority_queue <sst_info> &sstables) {
    for (int i = 0; i < files.size(); i++) {
        if (files[i].find('.') != -1) {
            std::string file = files[i].substr(0, files[i].find('.'));
            int level = atoi(file.substr(0, file.find('-')).c_str());
            int id = atoi(file.substr(file.find('-') + 1, file.length()).c_str());
            sstables.push(sst_info{level, id, files[i]});
        }
    }
}

void KVStore::write_sst(std::priority_queue <sst_info> &sstables) {
    layers.push_back(std::vector<SSTable *>());
    while (!sstables.empty()) {
        sst_info sst = sstables.top();
        sstables.pop();
        while (layers.size() <= sst.level) {
            layers.push_back(std::vector<SSTable *>());
        }
        layers[sst.level].push_back(new SSTable(sst.level, sst.id, sst.file, dir_path, vlog_path, bloomSize));
        stamp = std::max(layers[sst.level].back()->getStamp() + 1, stamp);
    }
}

/**
 * Demo consturctor used for test
 * !! cannot persistence !!
 * @param bloomSize bloomFilter size
*/
KVStore::KVStore(const std::string &dir, const std::string &vlog) : KVStoreAPI(dir, vlog) {
    this->memTable = new MemTable(0.5, BLOOMSIZE);
    this->dir_path = dir;
    this->vlog_path = vlog;
    this->stamp = 0;
    this->head = 0;
    this->tail = 0;
    this->bloomSize = BLOOMSIZE;
    std::priority_queue <sst_info> sstables;
    std::vector <std::string> files;
    utils::scanDir(dir_path, files);
    process_vlog();
    process_sst(files, sstables);
    write_sst(sstables);
}

KVStore::~KVStore() {
    //检查内存中的跳表 memTable 是否包含键值对
    if (memTable->get_numkv()) {
        //将 memTable 转换为 SSTable 并添加到第 0 层
        layers[0].push_back(memTable->convertSSTable(layers[0].size(), stamp++, dir_path, vlog_path));
    }
    //释放 memTable 占用的内存
    delete memTable;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s) {
    //检查 memTable 的大小是否超过 SSTABLESIZE
    if (memTable->size() >= SSTABLESIZE) {
        //将 memTable 转换为 SSTable 并添加到第 0 层
        layers[0].push_back(memTable->convertSSTable(layers[0].size(), stamp++, dir_path, vlog_path));
        delete memTable;
        //进行compaction, Level n层的文件数量为2^(𝑛+1)
        for (int i = 0; i < layers.size() && layers[i].size() > (1 << i + 2); i++) {
            compaction(i);
        }
        //创建一个新的 memTable
        memTable = new MemTable(0.5, bloomSize);
    }
    //将键值对插入 memTable
    memTable->put(key, s);
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key) {
    //从内存中的跳表 memTable 获取值
    std::string val = memTable->get(key);
    if (val == "~DELETED~") {
        return "";
    } else if (val != "") {
        return val;
    }
    //从 SSTable 获取值
    for (int i = 0; i < layers.size(); i++) {
        //从后向前遍历每层 SSTable
        for (int j = layers[i].size() - 1; j >= 0; j--) {
            //如果测试模式为 1 或 2，或者布隆过滤器判断键可能存在于 SSTable 中，则继续获取值
            if (layers[i][j]->query(key)) {
                //从 SSTable 中获取值
                val = layers[i][j]->get(key);
                if (val == "~DELETED~") {
                    return "";
                } else if (val != "") {
                    return val;
                }
            }
        }
    }
    return "";
}

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key) {
    if (get(key) != "") {
        put(key, "~DELETED~");
        return true;
    }
    return false;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {
    //删除所有 SSTable 文件
    for (int i = 0; i < layers.size(); i++) {
        for (int j = layers[i].size() - 1; j >= 0; j--) {
            //删除 SSTable 文件
            layers[i][j]->delete_disk();
            //释放 SSTable 对象
            delete layers[i][j];
            //从层中移除 SSTable
            layers[i].pop_back();
        }
    }
    //删除 memTable
    delete memTable;
    //删除 vlog 文件
    utils::rmfile(vlog_path);
    //删除存储目录中的所有文件
    std::vector <std::string> files;
    //获取存储目录中的所有文件名，并存储到 files 向量中
    utils::scanDir(dir_path, files);
    for (int i = 0; i < files.size(); i++) {
        utils::rmfile(files[i]);
    }
    //重新初始化 memTable
    memTable = new MemTable(0.5, bloomSize);
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list <std::pair<uint64_t, std::string>> &list) {
    //定义一个优先队列 kvs，用于存储 kv 对象
    std::priority_queue <kv> kvs;
    //定义一个向量 scanRes，用于存储各层次的扫描结果
    std::vector < std::vector < std::pair < uint64_t, std::string>>> scanRes;
    //定义一个迭代器 it
    std::vector<int> it;
    //先在 memTable 中查找
    //将 memTable 中指定范围的键值对添加到 scanRes 中
    scanRes.push_back(memTable->scan(key1, key2));
    //初始化迭代器
    it.push_back(0);
    if (it.back() != scanRes.back().size()) {
        //将第一个键值对及其时间戳和索引添加到优先队列
        kvs.push(kv{scanRes.back()[0], stamp, 0});
        //更新迭代器
        it.back()++;
    }
    //再在 SSTable 中查找
    //遍历 SSTable 所有层
    for (int i = 0, sstSum = 0; i < layers.size(); sstSum += layers[i].size(), i++) {
        for (int j = 0; j < layers[i].size(); j++) {
            scanRes.push_back(layers[i][j]->scan(key1, key2));
            //初始化迭代器
            it.push_back(0);
            if (it.back() != scanRes.back().size()) {
                kvs.push(kv{scanRes.back()[0], layers[i][j]->getStamp(), sstSum + j + 1});
                it.back()++;
            }
        }
    }
    //剔除已经删除的kv
    //初始化删除标记
    uint64_t last_delete = HEAD;
    while (!kvs.empty()) {
        //从优先队列中取出最小的键值对
        kv min_kv = kvs.top();
        kvs.pop();
        //如果当前键不等于上一个被删除的键
        if (last_delete != min_kv.kv_pair.first) {
            if (min_kv.kv_pair.second == "~DELETED~") {
                //如果值为删除标记, 更新删除标记
                last_delete = min_kv.kv_pair.first;
            } else {
                list.push_back(min_kv.kv_pair);
            }
        }
        //
        if (it[min_kv.i] != scanRes[min_kv.i].size()) {
            kvs.push(kv{scanRes[min_kv.i][it[min_kv.i]], min_kv.stamp, min_kv.i});
            it[min_kv.i]++;
        }
    }
}

/**
 * This reclaims space from vLog by moving valid value and discarding invalid value.
 * chunk_size is the size in byte you should AT LEAST recycle.
 */
void KVStore::gc(uint64_t chunk_size) {
    //打开 vlog 文件
    int fd = open(vlog_path.c_str(), O_RDWR, 0644);
    //从vlog 的 tail 位置开始读有效数据
    lseek(fd, tail, SEEK_SET);
    char buf[BUFFER_SIZE];
    read(fd, buf, 1);
    uint64_t read_len = 0;
    uint64_t vlen;
    uint64_t key;
    //读取 vlog 文件，将有效数据重新写入 memTable
    while (read_len < chunk_size && buf[0] == (char) MAGIC) {
        //读取日志前缀，获取键和值的长度
        read(fd, buf, VLOGPADDING - 1);
        key = *(uint64_t * )(buf + 2);
        vlen = *(uint32_t * )(buf + 10);
        //读value
        read(fd, buf, vlen);
        bool isNewest = true;
        //如果键不存在于 memTable 中，检查所有层的 SSTable 中最新的记录
        if (memTable->get(key) == "") {
            for (int i = 0; i < layers.size() && isNewest; i++) {
                for (int j = layers[i].size() - 1; j >= 0 && isNewest; j--) {
                    if (layers[i][j]->query(key)) {
                        off_t offset = layers[i][j]->get_offset(key);
                        //如果偏移量不是1，即不是MAGIC
                        if (offset != 1) {
                            //设置buf的结尾
                            buf[vlen] = 0;
                            //offset 指向该vLog entry
                            if (offset != 2 && offset == read_len + tail) {
                                //将该vLog entry 重新插入到MemTable 中
                                put(key, buf);
                            }
                                //vLog entry 记录的是过期的数据，不做处理
                            else {
                                isNewest = false;
                            }
                        }
                    }
                }
            }
        }
            //如果键存在于 memTable 中，说明是最新的数据，直接跳过这个vlog entry
        else {
            //更新读取长度vlog entry的长度
            read_len = read_len + VLOGPADDING + vlen;
            //读取下一个字节
            read(fd, buf, 1);
        }
    }
    close(fd);
    //将 memTable 转换为 SSTable 并添加到第 0 层
    layers[0].push_back(memTable->convertSSTable(layers[0].size(), stamp++, dir_path, vlog_path));
    delete memTable;
    //进行compaction
    for (int i = 0; i < layers.size() && layers[i].size() > (1 << i + 2); i++) {
        compaction(i);
    }
    memTable = new MemTable(0.5, bloomSize);
    //使用de_alloc_file() 帮助函数对扫描过的vLog 文件区域从tail开始访问过的read_len长度打洞
    utils::de_alloc_file(vlog_path, tail, read_len);
    //更新tail
    tail = read_len + tail;
}

void KVStore::compaction(int level) {
    uint64_t min_key = MINKEY;
    uint64_t max_key = 0;
    //确定压缩的 SSTable 数量，如果是第 0 层，压缩所有 SSTable；否则，压缩一半的 SSTable
    int compact_size = level ? layers[level].size() / 2 : layers[level].size();
    uint64_t max_stamp = layers[level][compact_size - 1]->getStamp();
    //确保压缩的 SSTable 数量包含所有时间戳小于等于 max_stamp 的 SSTable
    while (compact_size < layers[level].size() && layers[level][compact_size]->getStamp() <= max_stamp) {
        compact_size++;
    }
    //遍历所有需要压缩的 SSTable，更新 min_key 和 max_key
    for (int i = 0; i < compact_size; i++) {
        max_key = std::max(max_key, layers[level][i]->get_maxkey());
        min_key = std::min(min_key, layers[level][i]->get_minkey());
    }
    //准备下一层
    if (level + 1 == layers.size()) {
        layers.push_back(std::vector<SSTable *>());
    }
    //下一层 SSTable 的索引
    std::vector<int> index;
    std::vector<int> it;
    std::priority_queue <kv_info> kvs;
    //遍历下一层的所有 SSTable，收集键范围与当前层压缩范围重叠的 SSTable，将这些 SSTable 的索引添加到 index 中，并初始化迭代器
    for (int i = 0; i < layers[level + 1].size(); i++) {
        if (layers[level + 1][i]->get_minkey() <= max_key && layers[level + 1][i]->get_maxkey() >= min_key) {
            index.push_back(i);
            it.push_back(0);
        }
    }
    //将下一层的键值对添加到优先队列 kvs 中
    for (int i = index.size() - 1; i >= 0; i--) {
        if (it[i] != layers[level + 1][index[i]]->get_numkv()) {
            SSTable *sst = layers[level + 1][index[i]];
            kvs.push(kv_info{sst->get_keys()[0], sst->get_valueLens()[0], sst->getStamp(), sst->get_offsets()[0], i});
            it[i]++;
        }
    }
    //将当前层的键值对添加到优先队列 kvs 中
    for (int i = 0; i < compact_size; i++) {
        it.push_back(0);
        if (it.back() != layers[level][i]->get_numkv()) {
            SSTable *sst = layers[level][i];
            kvs.push(kv_info{sst->get_keys()[0], sst->get_valueLens()[0], sst->getStamp(), sst->get_offsets()[0],
                             i + index.size()});
            it.back()++;
        }
    }
    std::vector <kv_info> kv_list;
    while (!kvs.empty()) {
        //从优先队列中取出最小的键值对
        kv_info min_kv = kvs.top();
        kvs.pop();
        //检查是否为重复键
        if (kv_list.empty() || min_kv.key != kv_list.back().key) {
            //如果不是重复键，将键值对添加到 kv_list 中
            kv_list.push_back(min_kv);
        } else {
            //如果是重复键，确保时间戳递减
            assert(kv_list.back().stamp >= min_kv.stamp);
        }
        //更新优先队列kvs
        //如果是当前层的键值对
        if (min_kv.i >= index.size()) {
            int i = min_kv.i - index.size();
            //检查是否有更多键值对
            assert(layers[level][i]->get_numkv() == layers[level][i]->get_keys().size());
            //如果有更多键值对，将键值对添加到优先队列 kvs 中
            if (it[min_kv.i] != layers[level][i]->get_numkv()) {
                SSTable *sst = layers[level][i];
                kvs.push(kv_info{sst->get_keys()[it[min_kv.i]], sst->get_valueLens()[it[min_kv.i]], min_kv.stamp,
                                 sst->get_offsets()[it[min_kv.i]], min_kv.i});
                it[min_kv.i]++;
            }
        }
            //如果是下一层的键值对
        else if (it[min_kv.i] != layers[level + 1][index[min_kv.i]]->get_numkv()) {
            //获取对应的 SSTable，将下一个键值对添加到优先队列 kvs 中
            SSTable *sst = layers[level + 1][index[min_kv.i]];
            kvs.push(kv_info{sst->get_keys()[it[min_kv.i]], sst->get_valueLens()[it[min_kv.i]], min_kv.stamp,
                             sst->get_offsets()[it[min_kv.i]], min_kv.i});
            it[min_kv.i]++;
        }
    }
    //删除下一层的旧 SSTable
    for (int i = index.size() - 1; i >= 0; i--) {
        std::vector<SSTable *>::iterator iter = layers[level + 1].begin() + index[i];
        layers[level + 1][index[i]]->delete_disk();
        delete layers[level + 1][index[i]];
        layers[level + 1].erase(iter);
    }
    //删除当前层的旧 SSTable
    for (int i = compact_size - 1; i >= 0; i--) {
        std::vector<SSTable *>::iterator iter = layers[level].begin() + i;
        layers[level][i]->delete_disk();
        delete layers[level][i];
        layers[level].erase(iter);
    }
    //更新当前层的 SSTable 索引
    for (int i = 0; i < layers[level].size(); i++) {
        layers[level][i]->set_id(i);
    }
    //更新下一层的 SSTable 索引
    for (int i = 0; i < layers[level + 1].size(); i++) {
        layers[level + 1][i]->set_id(i);
    }
    //计算每个 SSTable 的最大键值对数量
    int max_kvnum = (SSTABLESIZE - bloomSize - HEADERSIZE) / 20;
    for (int i = 0; i < kv_list.size(); i += max_kvnum) {
        uint64_t max_key = 0;
        uint64_t min_key = MINKEY;
        uint64_t new_step = 0;
        uint64_t kv_num = std::min(max_kvnum, (int) kv_list.size() - i);
        std::vector <uint64_t> keys;
        std::vector <uint64_t> offsets, valueLens;
        bloomFilter *bloom_p = new bloomFilter(bloomSize, 3);
        for (int j = i; j < std::min(i + max_kvnum, (int) kv_list.size()); j++) {
            max_key = std::max(max_key, kv_list[j].key);
            min_key = std::min(min_key, kv_list[j].key);
            new_step = std::max(new_step, kv_list[j].stamp);
            keys.push_back(kv_list[j].key);
            offsets.push_back(kv_list[j].offset);
            valueLens.push_back(kv_list[j].valueLen);
            bloom_p->insert(kv_list[j].key);
        }
        SSTable *sst = new SSTable({new_step, kv_num, max_key, min_key}, level + 1, layers[level + 1].size(), bloom_p,
                                   keys, offsets, valueLens, dir_path, vlog_path);
        sst->write_disk();
        layers[level + 1].push_back(sst);
    }
}