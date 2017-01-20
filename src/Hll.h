#include <iostream>
#include <cstring>
#include <cmath>
#include <stdint.h>

#define HASH_SEED 27072015

#ifndef _HLL_H_
#define _HLL_H_

template <typename T>
class Hash {
  public:
    virtual uint64_t operator()(T value) const = 0;
};

template<typename T>
class MurMurHash : public Hash<T> {};

template<>
class MurMurHash<uint64_t> : public Hash<uint64_t>{
  public:
    uint64_t operator()(uint64_t value) const override {
      const uint64_t m = 0xc6a4a7935bd1e995;
      unsigned int seed = HASH_SEED;
      const int r = 47;
      uint64_t h = seed ^ (sizeof(uint64_t) * m);
      {
        uint64_t k = value;
        k *= m;
        k ^= k >> r;
        k *= m;
        h ^= k;
        h *= m;
      }
      const unsigned char * data2 = (const unsigned char*)&value;
      switch(sizeof(uint64_t) & 7)
      {
        case 7: h ^= uint64_t(data2[6]) << 48;
        case 6: h ^= uint64_t(data2[5]) << 40;
        case 5: h ^= uint64_t(data2[4]) << 32;
        case 4: h ^= uint64_t(data2[3]) << 24;
        case 3: h ^= uint64_t(data2[2]) << 16;
        case 2: h ^= uint64_t(data2[1]) << 8;
        case 1: h ^= uint64_t(data2[0]);
        h *= m;
      };
      h ^= h >> r;
      h *= m;
      h ^= h >> r;
      return h;
    }
};

class Hll {
  private:
    uint8_t bucketBits;
    uint8_t valueBits;
    uint64_t numberOfBuckets;
    uint64_t bucketSize;
    uint64_t bucketMask;
    uint64_t valueMask;
    uint8_t* synopsis;

    uint8_t leftMostSetBit(uint64_t hash) const;
    uint64_t murmurHash(const void * key, int len, unsigned int seed) const;
    void init(uint8_t bucketBits);

  public:
  	uint8_t* getCurrentSynopsis();
    uint64_t getSynopsisSize() const;
    uint64_t getNumberOfBuckets() const;
    uint64_t approximateCountDistinct();
    void add(uint64_t value);
    void add(const uint8_t otherSynopsis[]);
    uint64_t bucket(uint64_t hash);

    Hll(uint8_t bucketBits);
    Hll(const uint8_t *synopsis, uint8_t bucketBits);
    virtual ~Hll();

};


#endif
