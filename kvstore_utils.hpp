#pragma once

#include "kvstore.h"

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

bool KVStore::isMemTableFull() const {
    return memTable->size() >= SSTABLESIZE;
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

int KVStore::determineCompactSize(int level) {
    int compact_size = level ? layers[level].size() / 2 : layers[level].size();
    uint64_t max_stamp = layers[level][compact_size - 1]->getStamp();
    while (compact_size < layers[level].size() && layers[level][compact_size]->getStamp() <= max_stamp) {
        compact_size++;
    }
    return compact_size;
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