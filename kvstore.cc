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

KVStore::KVStore(const std::string &dir, const std::string &vlog) : KVStoreAPI(dir, vlog) {
    this->memTable = new MemTable(0.5, BLOOMSIZE);
    this->dir_path = dir;
    this->vlog_path = vlog;
    this->stamp = 0;
    this->head = 0;
    this->tail = 0;
    this->test_type = 0;
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

void KVStore::checkAndConvertMemTable() {
    if (memTable->size() >= SSTABLESIZE) {
        layers[0].push_back(memTable->convertSSTable(layers[0].size(), stamp++, dir_path, vlog_path));
        delete memTable;
        memTable = new MemTable(0.5, bloomSize);
    }
}

void KVStore::doCompaction() {
    for (int i = 0; i < layers.size() && layers[i].size() > (1 << i + 2); i++) {
        compaction(i);
    }
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s) {
    checkAndConvertMemTable();
    doCompaction();
    memTable->put(key, s);
}

std::string KVStore::getValueFromMemTable(uint64_t key) {
    std::string val = memTable->get(key);
    if (val == "~DELETED~") {
        return "";
    } else if (val != "") {
        return val;
    }
    return "";
}

std::string KVStore::getValueFromSSTable(uint64_t key) {
    std::string val = "";
    for (int i = 0; i < layers.size(); i++) {
        for (int j = layers[i].size() - 1; j >= 0; j--) {
            if (test_type == 1 || test_type == 2 || layers[i][j]->query(key)) {
                if (test_type == 2) {
                    val = layers[i][j]->get_fromdisk(key);
                } else {
                    val = layers[i][j]->get(key);
                }
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
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key) {
    std::string val = getValueFromMemTable(key);
    if (val != "") {
        return val;
    }
    val = getValueFromSSTable(key);
    return val;
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

void KVStore::deleteAllSSTables() {
    for (int i = 0; i < layers.size(); i++) {
        for (int j = layers[i].size() - 1; j >= 0; j--) {
            layers[i][j]->delete_disk();
            delete layers[i][j];
            layers[i].pop_back();
        }
    }
}

void KVStore::deleteAllFilesInDir() {
    std::vector <std::string> files;
    utils::scanDir(dir_path, files);
    for (int i = 0; i < files.size(); i++) {
        utils::rmfile(files[i]);
    }
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {
    deleteAllSSTables();
    delete memTable;
    utils::rmfile(vlog_path);
    deleteAllFilesInDir();
    memTable = new MemTable(0.5, bloomSize);
}

void
KVStore::getPairsFromMemTable(uint64_t key1, uint64_t key2, std::vector <std::vector<std::pair < uint64_t, std::string>>

>& scanRes,
std::vector<int> &it, std::priority_queue<kv>
& kvs) {
scanRes.
push_back(memTable
->
scan(key1, key2
));
it.push_back(0);
if(it.

back()

!= scanRes.

back()

.

size()

) {
kvs.
push(kv{scanRes.back()[0], stamp, 0}
);
it.

back()

++;
}
}

void
KVStore::getPairsFromSSTable(uint64_t key1, uint64_t key2, std::vector <std::vector<std::pair < uint64_t, std::string>>

>& scanRes,
std::vector<int> &it, std::priority_queue<kv>
& kvs) {
for(
int i = 0, sstSum = 0;
i<layers.

size();

sstSum += layers[i].

size(), i

++) {
for (
int j = 0;
j<layers[i].

size();

j++) {
scanRes.
push_back(layers[i][j]
->
scan(key1, key2
));
it.push_back(0);
if (it.

back()

!= scanRes.

back()

.

size()

) {
kvs.
push(kv{scanRes.back()[0], layers[i][j]->getStamp(), sstSum + j + 1}
);
it.

back()

++;
}
}
}
}

void KVStore::removeDeletedPairs(std::list <std::pair<uint64_t, std::string>> &list,
                                 std::vector <std::vector<std::pair < uint64_t, std::string>>

>& scanRes,
std::vector<int> &it, std::priority_queue<kv>
& kvs) {
uint64_t last_delete = HEAD;
while(!kvs.

empty()

) {
kv min_kv = kvs.top();
kvs.

pop();

if(last_delete != min_kv.kv_pair.first) {
if(min_kv.kv_pair.second == "~DELETED~") {
last_delete = min_kv.kv_pair.first;
}
else {
list.
push_back(min_kv
.kv_pair);
}
}
if(it[min_kv.i] != scanRes[min_kv.i].

size()

) {
kvs.
push(kv{scanRes[min_kv.i][it[min_kv.i]], min_kv.stamp, min_kv.i}
);
it[min_kv.i] ++;
}
}
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list <std::pair<uint64_t, std::string>> &list) {
    std::priority_queue <kv> kvs;
    std::vector < std::vector < std::pair < uint64_t, std::string>>> scanRes;
    std::vector<int> it;
    getPairsFromMemTable(key1, key2, scanRes, it, kvs);
    getPairsFromSSTable(key1, key2, scanRes, it, kvs);
    removeDeletedPairs(list, scanRes, it, kvs);
}

uint64_t KVStore::readVlogAndWriteToMemTable(uint64_t chunk_size, int fd, char *buf, uint64_t &read_len) {
    uint64_t vlen;
    uint64_t key;
    bool isNewest = true;
    while (read_len < chunk_size && buf[0] == (char) MAGIC) {
        read(fd, buf, VLOGPADDING - 1);
        key = *(uint64_t * )(buf + 2);
        vlen = *(uint32_t * )(buf + 10);
        read(fd, buf, vlen);
        if (memTable->get(key) == "") {
            for (int i = 0; i < layers.size() && isNewest; i++) {
                for (int j = layers[i].size() - 1; j >= 0 && isNewest; j--) {
                    if (layers[i][j]->query(key)) {
                        off_t offset = layers[i][j]->get_offset(key);
                        if (offset != 1) {
                            buf[vlen] = 0;
                            if (offset != 2 && offset == read_len + tail) {
                                put(key, buf);
                            } else {
                                isNewest = false;
                            }
                        }
                    }
                }
            }
        } else {
            read_len = read_len + VLOGPADDING + vlen;
            read(fd, buf, 1);
        }
    }
    return read_len;
}

void KVStore::convertMemTableToSSTable() {
    layers[0].push_back(memTable->convertSSTable(layers[0].size(), stamp++, dir_path, vlog_path));
    delete memTable;
    memTable = new MemTable(0.5, bloomSize);
}

/**
 * This reclaims space from vLog by moving valid value and discarding invalid value.
 * chunk_size is the size in byte you should AT LEAST recycle.
 */
void KVStore::gc(uint64_t chunk_size) {
    int fd = open(vlog_path.c_str(), O_RDWR, 0644);
    lseek(fd, tail, SEEK_SET);
    char buf[BUFFER_SIZE];
    read(fd, buf, 1);
    uint64_t read_len = 0;
    read_len = readVlogAndWriteToMemTable(chunk_size, fd, buf, read_len);
    close(fd);
    convertMemTableToSSTable();
    for (int i = 0; i < layers.size() && layers[i].size() > (1 << i + 2); i++) {
        compaction(i);
    }
    utils::de_alloc_file(vlog_path, tail, read_len);
    tail = read_len + tail;
}

int KVStore::determineCompactSize(int level) {
    int compact_size = level ? layers[level].size() / 2 : layers[level].size();
    uint64_t max_stamp = layers[level][compact_size - 1]->getStamp();
    while (compact_size < layers[level].size() && layers[level][compact_size]->getStamp() <= max_stamp) {
        compact_size++;
    }
    return compact_size;
}

void KVStore::updateMinMaxKeys(int compact_size, uint64_t &min_key, uint64_t &max_key, int level) {
    for (int i = 0; i < compact_size; i++) {
        max_key = std::max(max_key, layers[level][i]->get_maxkey());
        min_key = std::min(min_key, layers[level][i]->get_minkey());
    }
}

void KVStore::prepareNextLayer(int level) {
    if (level + 1 == layers.size()) {
        layers.push_back(std::vector<SSTable *>());
    }
}

void KVStore::collectOverlappingSSTables(int level, uint64_t min_key, uint64_t max_key, std::vector<int> &index,
                                         std::vector<int> &it) {
    for (int i = 0; i < layers[level + 1].size(); i++) {
        if (layers[level + 1][i]->get_minkey() <= max_key && layers[level + 1][i]->get_maxkey() >= min_key) {
            index.push_back(i);
            it.push_back(0);
        }
    }
}

void
KVStore::addKVToPriorityQueue(int level, int compact_size, std::vector<int> &it, std::priority_queue <kv_info> &kvs,
                              std::vector<int> &index) {
    for (int i = index.size() - 1; i >= 0; i--) {
        if (it[i] != layers[level + 1][index[i]]->get_numkv()) {
            SSTable *sst = layers[level + 1][index[i]];
            kvs.push(kv_info{sst->get_keys()[0], sst->get_valueLens()[0], sst->getStamp(), sst->get_offsets()[0], i});
            it[i]++;
        }
    }
    for (int i = 0; i < compact_size; i++) {
        it.push_back(0);
        if (it.back() != layers[level][i]->get_numkv()) {
            SSTable *sst = layers[level][i];
            kvs.push(kv_info{sst->get_keys()[0], sst->get_valueLens()[0], sst->getStamp(), sst->get_offsets()[0],
                             i + index.size()});
            it.back()++;
        }
    }
}

std::vector <kv_info>
KVStore::collectKVList(std::vector<int> &it, std::priority_queue <kv_info> &kvs, int level, std::vector<int> &index,
                       int compact_size) {
    std::vector <kv_info> kv_list;
    while (!kvs.empty()) {
        kv_info min_kv = kvs.top();
        kvs.pop();
        if (kv_list.empty() || min_kv.key != kv_list.back().key) {
            kv_list.push_back(min_kv);
        } else {
            assert(kv_list.back().stamp >= min_kv.stamp);
        }
        if (min_kv.i >= index.size()) {
            int i = min_kv.i - index.size();
            assert(layers[level][i]->get_numkv() == layers[level][i]->get_keys().size());
            if (it[min_kv.i] != layers[level][i]->get_numkv()) {
                SSTable *sst = layers[level][i];
                kvs.push(kv_info{sst->get_keys()[it[min_kv.i]], sst->get_valueLens()[it[min_kv.i]], min_kv.stamp,
                                 sst->get_offsets()[it[min_kv.i]], min_kv.i});
                it[min_kv.i]++;
            }
        } else if (it[min_kv.i] != layers[level + 1][index[min_kv.i]]->get_numkv()) {
            SSTable *sst = layers[level + 1][index[min_kv.i]];
            kvs.push(kv_info{sst->get_keys()[it[min_kv.i]], sst->get_valueLens()[it[min_kv.i]], min_kv.stamp,
                             sst->get_offsets()[it[min_kv.i]], min_kv.i});
            it[min_kv.i]++;
        }
    }
    return kv_list;
}

void KVStore::deleteOldSSTables(int level, std::vector<int> &index, int compact_size) {
    for (int i = index.size() - 1; i >= 0; i--) {
        std::vector<SSTable *>::iterator iter = layers[level + 1].begin() + index[i];
        layers[level + 1][index[i]]->delete_disk();
        delete layers[level + 1][index[i]];
        layers[level + 1].erase(iter);
    }
    for (int i = compact_size - 1; i >= 0; i--) {
        std::vector<SSTable *>::iterator iter = layers[level].begin() + i;
        layers[level][i]->delete_disk();
        delete layers[level][i];
        layers[level].erase(iter);
    }
}

void KVStore::updateSSTableIndices(int level) {
    for (int i = 0; i < layers[level].size(); i++) {
        layers[level][i]->set_id(i);
    }
    for (int i = 0; i < layers[level + 1].size(); i++) {
        layers[level + 1][i]->set_id(i);
    }
}

void KVStore::createNewSSTables(int level, std::vector <kv_info> &kv_list) {
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

void KVStore::compaction(int level) {
    uint64_t min_key = MINKEY;
    uint64_t max_key = 0;
    int compact_size = determineCompactSize(level);
    updateMinMaxKeys(compact_size, min_key, max_key, level);
    prepareNextLayer(level);
    std::vector<int> index;
    std::vector<int> it;
    collectOverlappingSSTables(level, min_key, max_key, index, it);
    std::priority_queue <kv_info> kvs;
    addKVToPriorityQueue(level, compact_size, it, kvs, index);
    std::vector <kv_info> kv_list = collectKVList(it, kvs, level, index, compact_size);
    deleteOldSSTables(level, index, compact_size);
    updateSSTableIndices(level);
    createNewSSTables(level, kv_list);
}