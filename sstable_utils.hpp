#pragma once

#include "sstable.h"
#include "utils.h"

void SSTable::readHeader(int fd) {
    char buf[32] = {0};
    utils::read_file(fd, -1, 32, buf);
    head.stamp = *(uint64_t *)buf;
    head.num_kv = *(uint64_t *)(buf + 8);
    head.min_key = *(uint64_t *)(buf + 16);
    head.max_key = *(uint64_t *)(buf + 24);
}

void SSTable::initializeBloomFilter(int fd, uint64_t bloomSize) {
    bloomfilter = new bloomFilter(bloomSize, 3);
    utils::read_file(fd, -1, bloomfilter->getM(), bloomfilter->getSet());
}

void SSTable::readKeyValuePairs(int fd) {
    char buf[20] = {0};
    uint64_t key;
    uint64_t offset;
    uint32_t valueLen;
    for (uint64_t i = 0; i < head.num_kv; ++i) {
        utils::read_file(fd, -1, 20, buf);
        key = *(uint64_t *)buf;
        offset = *(uint64_t *)(buf + 8);
        valueLen = *(uint32_t *)(buf + 16);
        keys.push_back(key);
        offsets.push_back(offset);
        valueLens.push_back(valueLen);
    }
}

std::vector<uint64_t>::const_iterator SSTable::findKey(uint64_t key) const {
    return std::lower_bound(keys.begin(), keys.end(), key);
}

std::string SSTable::readValueFromVlog(off_t offset, size_t size) const {
    char buf[VLOGPADDING + size + 5] = {0};
    utils::read_file(vlog_path, offset, VLOGPADDING + size, buf);
    return std::string(buf + VLOGPADDING);
}

std::string SSTable::getSSTFilename() const {
    return dir_path + "/" + std::to_string(level) + "-" + std::to_string(id) + ".sst";
}

std::string SSTable::getSSTFilename(int id) const {
    return dir_path + "/" + std::to_string(level) + "-" + std::to_string(id) + ".sst";
}

bool SSTable::findKeyInDisk(int fd, uint64_t key, off_t& offset, size_t& valueLen) const {
    char buf[20] = {0};
    uint64_t now_key;
    for (uint64_t i = 0; i < head.num_kv; ++i) {
        utils::read_file(fd, -1, 20, buf);
        now_key = *(uint64_t *)buf;
        offset = *(uint64_t *)(buf + 8);
        valueLen = *(uint32_t *)(buf + 16);
        if (now_key == key) {
            return true;
        }
    }
    return false;
}

int SSTable::getKeyIndex(uint64_t key) const {
    auto iter = findKey(key);
    if (iter != keys.end() && *iter == key) {
        return int(iter - keys.begin());
    }
    return -1;
}

bool SSTable::isKeyDeleted(int index) const {
    return valueLens[index] == 0;
}


std::vector<std::pair<uint64_t, std::string>> SSTable::readRangeFromVlog(int index1, int index2) const {
    std::vector<std::pair<uint64_t, std::string>> list;
    int fd = open(vlog_path.c_str(), O_RDWR, 0644);
    if (fd == -1) {
        throw std::runtime_error("Failed to open VLOG file: " + vlog_path);
    }

    for (int i = index1; i < index2; ++i) {
        off_t offset = offsets[i];
        size_t size = valueLens[i];
        if (size) {
            char buf[VLOGPADDING + size + 5] = {0};
            utils::read_file(fd, offset, size + VLOGPADDING, buf);
            list.push_back(std::make_pair(keys[i], std::string(buf + VLOGPADDING)));
        } else {
            list.push_back(std::make_pair(keys[i], std::string("~DELETED~")));
        }
    }

    close(fd);
    return list;
}

void SSTable::writeHeader(std::string sstFilename) const {
    char buf[32] = {0};
    *(uint64_t *)buf = head.stamp;
    *(uint64_t *)(buf + 8) = head.num_kv;
    *(uint64_t *)(buf + 16) = head.min_key;
    *(uint64_t *)(buf + 24) = head.max_key;
    utils::write_file(sstFilename, -1, 32, buf);
}

void SSTable::writeBloomFilter(std::string sstFilename) const {
    utils::write_file(sstFilename, -1, bloomfilter->getM(), bloomfilter->getSet());
}

void SSTable::writeKeyValuePairs(std::string sstFilename) const {
    char buf[20] = {0};
    for (size_t i = 0; i < keys.size(); ++i) {
        *(uint64_t *)buf = keys[i];
        *(uint64_t *)(buf + 8) = offsets[i];
        *(uint32_t *)(buf + 16) = valueLens[i];
        utils::write_file(sstFilename, -1, 20, buf);
    }
}

void SSTable::assertFileExists(const std::string& filename) const {
    assert(utils::fileExists(filename));
}

void SSTable::removeFile(const std::string& filename) const {
    utils::rmfile(filename);
}

void SSTable::renameFile(const std::string& oldFilename, const std::string& newFilename) const {
    std::rename(oldFilename.c_str(), newFilename.c_str());
}