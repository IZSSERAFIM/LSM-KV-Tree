#pragma once

#include "memtable.h"

void MemTable::initializeConversion(const std::string &vlog, off_t &offset, int &fd, std::vector<uint64_t> &keys, std::vector<uint64_t> &offsets, std::vector<uint64_t> &valueLens, bloomFilter *&bloom_p) {
    offset = (off_t) utils::get_end_offset(vlog);
    bloom_p = new bloomFilter(bloomSize, 3);
    fd = open(vlog.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
    lseek(fd, offset, SEEK_SET);
}

void MemTable::processNodes(int fd, off_t &offset, std::vector<uint64_t> &keys, std::vector<uint64_t> &offsets, std::vector<uint64_t> &valueLens, bloomFilter *bloom_p, uint64_t &max_k, uint64_t &min_k) {
    MemTable::Node *ptr = head[0];
    while (ptr->next) {
        MemTable::Node *p = ptr->next;
        bloom_p->insert(p->key);
        keys.push_back(p->key);
        offsets.push_back(offset);
        if (p->value != "~DELETED~") {
            if (p->key > max_k) {
                max_k = p->key;
            }
            if (p->key < min_k) {
                min_k = p->key;
            }
            valueLens.push_back(p->value.length());
            write_vlog(p, offset, fd);
        } else {
            valueLens.push_back(0);
        }
        ptr = ptr->next;
    }
}

void MemTable::finalizeConversion(int fd, SSTable *&sst, int id, uint64_t stamp, const std::string &dir, const std::string &vlog, bloomFilter *bloom_p, const std::vector<uint64_t> &keys, const std::vector<uint64_t> &offsets, const std::vector<uint64_t> &valueLens, uint64_t max_k, uint64_t min_k) {
    close(fd);
    sst = new SSTable({stamp, num_kv, max_k, min_k}, 0, id, bloom_p, keys, offsets, valueLens, dir, vlog);
    sst->write_disk();
}

void MemTable::prepareBuffer(Node *p, char* buf, size_t vlog_len) {
    strcpy(buf + VLOGPADDING, p->value.c_str());
    buf[0] = MAGIC;
    buf[vlog_len] = 0;
    *(uint16_t * )(buf + 1) = generateChecksum(p->key, p->value);
    *(uint64_t * )(buf + 3) = p->key;
    *(uint32_t * )(buf + 11) = (uint32_t) p->value.length();
}

void MemTable::writeBuffer(int fd, char* buf, size_t vlog_len) { // 修改参数类型为 char*
    utils::write_file(fd, vlog_len, static_cast<void*>(buf)); // 显式类型转换为 void*
}

uint16_t MemTable::generateChecksum(uint64_t key, const std::string &value) {
    return utils::generate_checksum(key, value.length(), value);
}