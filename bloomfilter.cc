#include "bloomfilter.h"

#include <iostream>

bloomFilter::bloomFilter(int m, int k) : m(m), k(k) {
    set = new bool[m];
    for (int i = 0; i < m; i++) {
        set[i] = false;
    }
}

bloomFilter::~bloomFilter() {
    delete[] set;
}

void bloomFilter::insert(const uint64_t s) {
    uint64_t hash[4] = {0};
    for (int i = 0; i < k; i++) {
        MurmurHash3_x64_128(&s, sizeof(s), i, hash);
        set[hash[0] % m] = true; // 注意这里只用hash[0]，因为你可能不需要4个哈希值
    }
}

bool bloomFilter::query(const uint64_t s) {
    uint64_t hash[4] = {0};
    for (int i = 0; i < k; i++) {
        MurmurHash3_x64_128(&s, sizeof(s), i, hash);
        if (!set[hash[0] % m]) { // 注意这里只用hash[0]
            return false;
        }
    }
    return true;
}

bool* bloomFilter::getSet() {
    return set;
}

int bloomFilter::getM() {
    return m;
}