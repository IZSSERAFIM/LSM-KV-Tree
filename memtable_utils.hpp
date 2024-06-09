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

void MemTable::findAndUpdate(uint64_t key, const std::string &val, Node *former[]) {
    Node *ptr = head[max_layer - 1];
    for (int layer = max_layer; layer; layer--) {
        while (ptr->next && ptr->next->key <= key) {
            ptr = ptr->next;
        }
        former[layer - 1] = ptr;
        if (ptr->key == key) {
            while (ptr) {
                ptr->value = val;
                ptr = ptr->down;
            }
            return;
        } else if (ptr->down) {
            ptr = ptr->down;
        }
    }
}

void MemTable::insertNewNodes(uint64_t key, const std::string &val, Node *former[], int new_layer) {
    Node *ptr = NULL;
    for (int layer = 1; layer <= std::min(max_layer, new_layer); layer++) {
        ptr = former[layer - 1]->next = new Node(key, val, ptr, former[layer - 1]->next);
    }
}

void MemTable::updateHeadNodes(uint64_t key, const std::string &val, int new_layer) {
    Node *ptr = NULL;
    for (int layer = max_layer + 1; layer <= new_layer; layer++) {
        Node *new_head = new Node(HEAD, "NONE", head.back(), NULL);
        ptr = new_head->next = new Node(key, val, ptr, NULL);
        head.push_back(new_head);
    }
}

void MemTable::deleteLayerNodes(Node* head) {
    Node *ptr = head->next;
    Node *now_p;
    while (ptr) {
        now_p = ptr;
        ptr = ptr->next;
        delete now_p;
    }
}

void MemTable::deleteAllNodes() {
    for (auto it = head.rbegin(); it != head.rend(); ++it) {
        deleteLayerNodes(*it);
    }
}

MemTable::Node* MemTable::findStartPosition(uint64_t key1) const {
    Node *ptr = head[max_layer - 1];
    while (true) {
        while (ptr->next && ptr->next->key < key1) {
            ptr = ptr->next;
        }
        if (ptr->down) {
            ptr = ptr->down;
        } else {
            break;
        }
    }
    return ptr->next;
}

std::vector<std::pair<uint64_t, std::string>> MemTable::collectRange(Node* start, uint64_t key1, uint64_t key2) const {
    std::vector<std::pair<uint64_t, std::string>> result;
    Node* ptr = start;
    while (ptr && ptr->key >= key1 && ptr->key <= key2) {
        result.push_back(std::make_pair(ptr->key, ptr->value));
        ptr = ptr->next;
    }
    return result;
}