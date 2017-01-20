#include <cstring>
#include <cmath>
#include <iostream>
#include <stdint.h>

#include "Hll.h"

uint64_t Hll::murmurHash( const void * key, int len, unsigned int seed ) const
{
  const uint64_t m = 0xc6a4a7935bd1e995;
  const int r = 47;
  uint64_t h = seed ^ (len * m);
  const uint64_t * data = (const uint64_t *)key;
  const uint64_t * end = data + (len/8);
  while(data != end)
  {
    uint64_t k = *data++;
    k *= m;
    k ^= k >> r;
    k *= m;
    h ^= k;
    h *= m;
  }
  const unsigned char * data2 = (const unsigned char*)data;
  switch(len & 7)
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

uint64_t Hll::bucket(uint64_t hash) {
  // Get the most significant bits and shift that
  // For example on a 16-bit platform, with a 4 bit bucketMask (shift = 12)
  // The value 0110 0111 0101 0001 is first bit masked to 0110 0000 0000 0000
  // And right shifted by 12 bits, which gives 0110, bucket 6.
  return (hash & bucketMask) >> shift;
}

uint8_t Hll::leftMostSetBit(uint64_t hash) const {
  // We set the bucket bits to 0
  if (hash == 0)
    return 0;
  else
    return ZERO_VALUE + __builtin_clzll(hash & valueMask) + 1 - bucketBits;
}

void Hll::init(uint8_t bucketBits) {
  this -> bucketBits = bucketBits;
  this -> synopsisSize = 1UL << bucketBits;
  this -> shift = 64 - bucketBits;

  // Ex: with a 4 bits bucket on a 2 bytes size_t we want 1111 0000 0000 0000
  // So that's 10000 minus 1 shifted with 12 zeroes 1111 0000 0000 0000
  this -> bucketMask = (( 1UL << bucketBits ) - 1UL) << shift;

  // Ex: with a 4 bits bucket on a 16 bit size_t  we want 0000 1111 1111 1111
  // So that's 1 with 12 ( 16 - 4 ) zeroes 0001 0000 0000 0000, minus one = 0000 1111 1111 1111
  this -> valueMask = (1UL << shift ) - 1UL;

  this -> synopsis = new uint8_t[synopsisSize];
}

Hll::Hll(uint8_t bucketBits) {
  init(bucketBits);
  memset( synopsis, ZERO_VALUE, synopsisSize );
}

Hll::Hll(uint8_t *synopsis, uint8_t bucketBits) {
  init(bucketBits);
  for(uint64_t i = 0; i < synopsisSize; i++) {
    this -> synopsis[i] = synopsis[i];
  }
}

Hll::~Hll() {
  delete[] synopsis;
}

void Hll::add(uint64_t value) {
  uint64_t hash = murmurHash(&value, 8, HASH_SEED);
  // We store in the synopsis for the the biggest leftmost one
  synopsis[bucket(hash)] = std::max(synopsis[bucket(hash)], leftMostSetBit(hash));
}

void Hll::add(const uint8_t otherSynopsis[]) {
  for (uint64_t i = 0; i < synopsisSize; i++) {
    synopsis[i] = std::max(synopsis[i], otherSynopsis[i]);
  }
}

uint8_t* Hll::getCurrentSynopsis() {
  return this -> synopsis;
}

uint64_t Hll::getSynopsisSize() {
  return this -> synopsisSize;
}

uint64_t Hll::approximateCountDistinct() {
  double harmonicMean = 0.0;
  double alpha;
  // Alpha computation (see paper)
  switch (bucketBits) {
    case 4:
      alpha = 0.673;
      break;
    case 5:
      alpha = 0.697;
      break;
    case 6:
      alpha = 0.709;
    default:
      alpha = (0.7213 / (1.0 + (1.079 / static_cast<double>(synopsisSize))));
  }

  for (uint64_t i = 0; i < synopsisSize; i++)
  {
      harmonicMean += 1.0 / (1 << (synopsis[i] - ZERO_VALUE));
  }
  harmonicMean = synopsisSize / harmonicMean;
  return std::round(0.5 + alpha * harmonicMean * synopsisSize);
}
