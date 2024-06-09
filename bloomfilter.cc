#include "bloomfilter.hpp"
#include "MurmurHash3.h"
#include <cstring>

bloomFilter::bloomFilter(int m, int k) : m(m), k(k) {
    set = new bool[m];
    std::memset(set, 0, m * sizeof(bool));
}

bloomFilter::~bloomFilter() {
    delete[] set;
}

void bloomFilter::insert(const uint64_t s) {
    uint64_t hash[4] = {0};
    for (int i = 0; i < k; i++) {
        MurmurHash3_x64_128(&s, sizeof(s), i, hash);
        set[hash[i] % m] = true;
    }
}

bool bloomFilter::query(const uint64_t s) {
    uint64_t hash[4] = {0};
    for (int i = 0; i < k; i++) {
        MurmurHash3_x64_128(&s, sizeof(s), i, hash);
        if (!set[hash[i] % m]) {
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
