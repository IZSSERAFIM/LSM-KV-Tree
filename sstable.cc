#include "sstable.h"
#include "sstable_utils.hpp"

SSTable::SSTable(head_type head, int level, int id, bloomFilter *bloomFilter, std::vector <uint64_t> keys,
                 std::vector <uint64_t> offsets, std::vector <uint64_t> valueLens, std::string dir_path,
                 std::string vlog_path) {
    this->head = head;
    this->level = level;
    this->id = id;
    this->bloomfilter = bloomFilter;
    this->keys = keys;
    this->offsets = offsets;
    this->valueLens = valueLens;
    this->dir_path = dir_path;
    this->vlog_path = vlog_path;
}


SSTable::SSTable(int level, int id, std::string sstFilename, std::string dir_path, std::string vlog_path, uint64_t bloomSize) {
    this->level = level;
    this->id = id;
    this->dir_path = dir_path;
    this->vlog_path = vlog_path;
    sstFilename = dir_path + "/" + sstFilename;

    // Open the file
    int fd = open(sstFilename.c_str(), O_RDWR, 0644);
    if (fd == -1) {
        // Handle error
        throw std::runtime_error("Failed to open file: " + sstFilename);
    }

    // Read header
    readHeader(fd);

    // Initialize bloom filter
    initializeBloomFilter(fd, bloomSize);

    // Read key-value pairs
    readKeyValuePairs(fd);

    // Close the file
    close(fd);
}


SSTable::~SSTable() {
    //析构函数只析构 bloomfilter，因为 bloomfilter 是动态分配的内存(bloomfilter = new bloomFilter(bloomSize, 3))，需要显式释放。而其他成员变量要么是栈上分配的，要么是标准库类型，会自动管理其内部资源，不需要显式析构
    delete bloomfilter;
}


std::string SSTable::get(uint64_t key) const {
    auto iter = findKey(key);
    if (iter != keys.end() && *iter == key) {
        int index = int(iter - keys.begin());
        off_t offset = offsets[index];
        size_t size = valueLens[index];
        if (size) {
            return readValueFromVlog(offset, size);
        } else {
            return std::string("~DELETED~");
        }
    }
    return std::string("");
}


std::string SSTable::get_fromdisk(uint64_t key) const {
    std::string sstFilename = getSSTFilename();
    int fd = open(sstFilename.c_str(), O_RDWR, 0644);
    if (fd == -1) {
        throw std::runtime_error("Failed to open file: " + sstFilename);
    }

    char buf[HEADERSIZE] = {0};
    utils::read_file(fd, 0, HEADERSIZE, buf);
    uint64_t num_kv = *(uint64_t *)(buf + 8);

    lseek(fd, bloomfilter->getM() + 32, SEEK_SET);

    off_t offset;
    size_t valueLen;
    if (findKeyInDisk(fd, key, offset, valueLen)) {
        close(fd);
        if (valueLen) {
            return readValueFromVlog(offset, valueLen);
        } else {
            return std::string("~DELETED~");
        }
    }

    close(fd);
    return std::string("");
}


uint64_t SSTable::get_offset(uint64_t key) const {
    int index = getKeyIndex(key);
    if (index != -1) {
        if (isKeyDeleted(index)) {
            return 2;
        } else {
            return offsets[index];
        }
    }
    return 1;
}



std::vector<std::pair<uint64_t, std::string>> SSTable::scan(uint64_t key1, uint64_t key2) {
    std::vector<std::pair<uint64_t, std::string>> list;
    if (key1 > key2) {
        return list;
    }

    auto iter1 = findKey(key1);
    auto iter2 = findKey(key2);
    if (iter2 != keys.end() && *iter2 == key2) {
        iter2++;
    }

    int index1 = int(iter1 - keys.begin());
    int index2 = int(iter2 - keys.begin());

    return readRangeFromVlog(index1, index2);
}


bool SSTable::query(uint64_t key) {
    return bloomfilter->query(key);
}


void SSTable::write_disk() const {
    write_sst();
}


void SSTable::delete_disk() const {
    std::string sstFilename = getSSTFilename();
    assertFileExists(sstFilename);
    removeFile(sstFilename);
}


void SSTable::set_id(int new_id) {
    std::string old_sst = getSSTFilename();
    std::string new_sst = getSSTFilename(new_id);
    id = new_id;
    assertFileExists(old_sst);
    renameFile(old_sst, new_sst);
}


void SSTable::write_sst() const {
    std::string sstFilename = getSSTFilename();
    int fd = open(sstFilename.c_str(), O_RDWR | O_CREAT, 0644);
    if (fd == -1) {
        throw std::runtime_error("Failed to open file: " + sstFilename);
    }

    writeHeader(sstFilename);
    writeBloomFilter(sstFilename);
    writeKeyValuePairs(sstFilename);

    close(fd);
}


uint64_t SSTable::get_numkv() const {
    return head.num_kv;
}


uint64_t SSTable::getStamp() const {
    return head.stamp;
}


uint64_t SSTable::get_maxkey() const {
    return head.max_key;
}


uint64_t SSTable::get_minkey() const {
    return head.min_key;
}


std::vector <uint64_t> SSTable::get_keys() const {
    return keys;
}


std::vector <uint64_t> SSTable::get_offsets() const {
    return offsets;
}


std::vector <uint64_t> SSTable::get_valueLens() const {
    return valueLens;
}
