#ifndef BLOOMFILTER_H
#define BLOOMFILTER_H

#include <bitset>
#include <string>
#include <iostream>
#include <fstream>
#include "MurmurHash3.h"

class bloomFilter {

private:
    int m;//哈希数组的大小
    int k;//hash函数的个数
    bool *set;

public:
    bloomFilter(int m, int k) : m(m), k(k) {
        set = new bool[m];
        for (int i = 0; i < m; i++) {
            set[i] = false;
        }
    }

    ~bloomFilter() {
        delete[] set;
    }

    void insert(const uint64_t s) {
        uint64_t hash[4] = {0};
        for (int i = 0; i < k; i++) {
            MurmurHash3_x64_128(&s, sizeof(s), i, hash);
            set[hash[i] % m] = true;
        }
    }

    bool query(const uint64_t s) {
        uint64_t hash[4] = {0};
        for (int i = 0; i < k; i++) {
            MurmurHash3_x64_128(&s, sizeof(s), i, hash);
            if (!set[hash[i] % m]) {
                return false;
            }
        }
        return true;
    }

    bool* getSet() {
        return set;
    }

    int getM() {
        return m;
    }
};


#endif //BLOOMFILTER_H