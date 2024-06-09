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
    if (a.level == b.level) {
        return a.id > b.id;
    } else {
        return a.level > b.level;
    }
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
    for (const auto &file: files) {
        if (file.find('.') != -1) {
            std::string fileName = file.substr(0, file.find('.'));
            int level = atoi(fileName.substr(0, fileName.find('-')).c_str());
            int id = atoi(fileName.substr(fileName.find('-') + 1, fileName.length()).c_str());
            sstables.push(sst_info{level, id, file});
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
    for (const auto &layer: layers) {
        for (auto it = layer.rbegin(); it != layer.rend(); ++it) {
            if ((*it)->query(key)) {
                val = (*it)->get(key);
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

void KVStore::deleteAllSSTables() {
    for (auto &layer: layers) {
        while (!layer.empty()) {
            layer.back()->delete_disk();
            delete layer.back();
            layer.pop_back();
        }
    }
}

void KVStore::deleteAllFilesInDir() {
    std::vector <std::string> files;
    utils::scanDir(dir_path, files);
    for (const auto &file: files) {
        utils::rmfile(file);
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

> & scanRes,
std::vector<int> &it, std::priority_queue<kv>
& kvs )
{
scanRes.
push_back( memTable
->
scan( key1, key2
));
it.push_back( 0 );
if ( it.

back()

!= scanRes.

back()

.

size()

)
{
kvs.
push( kv{scanRes.back()[0], stamp, 0}
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
int sstSum = 0;
for (
auto &layer
: layers) {
for (
auto &sstable
: layer) {
scanRes.
push_back(sstable
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
push(kv{scanRes.back()[0], sstable->getStamp(), sstSum + (&sstable - &layer[0]) + 1}
);
it.

back()

++;
}
}
sstSum += layer.

size();

}
}


void KVStore::removeDeletedPairs(std::list <std::pair<uint64_t, std::string>> &list,
                                 std::vector <std::vector<std::pair < uint64_t, std::string>>

> & scanRes,
std::vector<int> &it, std::priority_queue<kv>
& kvs )
{
uint64_t last_delete = HEAD;
while ( !kvs.

empty()

)
{
kv min_kv = kvs.top();
kvs.

pop();

if ( last_delete != min_kv.kv_pair.first )
{
if ( min_kv.kv_pair.second == "~DELETED~" )
{
last_delete = min_kv.kv_pair.first;
}else  {
list.
push_back( min_kv
.kv_pair );
}
}
if ( it[min_kv.i] != scanRes[min_kv.i].

size()

)
{
kvs.
push( kv{scanRes[min_kv.i][it[min_kv.i]], min_kv.stamp, min_kv.i}
);
it[min_kv.i]++;
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
            for (const auto &layer: layers) {
                if (!isNewest) break;
                for (auto it = layer.rbegin(); it != layer.rend(); ++it) {
                    if ((*it)->query(key)) {
                        off_t offset = (*it)->get_offset(key);
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
    int i = 0;
    for (const auto &layer: layers[level]) {
        if (i >= compact_size) break;
        max_key = std::max(max_key, layer->get_maxkey());
        min_key = std::min(min_key, layer->get_minkey());
        i++;
    }
}

void KVStore::prepareNextLayer(int level) {
    if (level + 1 == layers.size()) {
        layers.push_back(std::vector<SSTable *>());
    }
}

void KVStore::collectOverlappingSSTables(int level, uint64_t min_key, uint64_t max_key, std::vector<int> &index,
                                         std::vector<int> &it) {
    int i = 0;
    for (const auto &layer: layers[level + 1]) {
        if (layer->get_minkey() <= max_key && layer->get_maxkey() >= min_key) {
            index.push_back(i);
            it.push_back(0);
        }
        i++;
    }
}

void
KVStore::addKVToPriorityQueue(int level, int compact_size, std::vector<int> &it, std::priority_queue <kv_info> &kvs,
                              std::vector<int> &index) {
    int i = index.size();
    for (auto idx = index.rbegin(); idx != index.rend(); ++idx, --i) {
        if (it[i - 1] != layers[level + 1][*idx]->get_numkv()) {
            SSTable *sst = layers[level + 1][*idx];
            kvs.push(kv_info{sst->get_keys()[0], sst->get_valueLens()[0], sst->getStamp(), sst->get_offsets()[0],
                             i - 1});
            it[i - 1]++;
        }
    }
    i = 0;
    for (auto &layer: layers[level]) {
        if (i >= compact_size) break;
        it.push_back(0);
        if (it.back() != layer->get_numkv()) {
            kvs.push(
                    kv_info{layer->get_keys()[0], layer->get_valueLens()[0], layer->getStamp(), layer->get_offsets()[0],
                            i + index.size()});
            it.back()++;
        }
        i++;
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
    int i = index.size();
    for (auto idx = index.rbegin(); idx != index.rend(); ++idx, --i) {
        layers[level + 1][*idx]->delete_disk();
        delete layers[level + 1][*idx];
        layers[level + 1].erase(layers[level + 1].begin() + *idx);
    }
    i = compact_size;
    for (auto iter = layers[level].rbegin(); iter != layers[level].rbegin() + compact_size; ++iter, --i) {
        (*iter)->delete_disk();
        delete *iter;
        layers[level].erase(layers[level].begin() + i - 1);
    }
}

void KVStore::updateSSTableIndices(int level) {
    int i = 0;
    for (auto &layer: layers[level]) {
        layer->set_id(i++);
    }
    i = 0;
    for (auto &layer: layers[level + 1]) {
        layer->set_id(i++);
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