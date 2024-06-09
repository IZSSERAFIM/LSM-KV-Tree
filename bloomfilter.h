#ifndef BLOOMFILTER_H
#define BLOOMFILTER_H

#include <bitset>
#include <string>
#include "MurmurHash3.h"

class bloomFilter {

private:
    int m; //哈希数组的大小
    int k; //hash函数的个数
    bool *set;

public:
    bloomFilter(int m, int k);
    ~bloomFilter();
    void insert(const uint64_t s);
    bool query(const uint64_t s);
    bool* getSet();
    int getM();
};

#endif //BLOOMFILTER_H