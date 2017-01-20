#include <iostream>
#include <cstring>
#include <cmath>
#include <stdint.h>

#define HASH_SEED 27072015
#define ZERO_VALUE 48

#ifndef _HLL_H_
#define _HLL_H_

class Hll {
  private:
    uint8_t bucketBits;
    uint64_t synopsisSize;
    uint8_t shift;
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
    Hll(uint8_t *synopsis, uint8_t bucketBits);
    virtual ~Hll();

};


#endif
