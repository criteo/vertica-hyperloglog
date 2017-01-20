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
      uint64_t k = value;
      k *= m;
      k ^= k >> r;
      k *= m;
      h ^= k;
      h *= m;
      h *= m;
      h ^= h >> r;
      h *= m;
      h ^= h >> r;
      return h;
    }
};

template<>
class MurMurHash<uint32_t> : public Hash<uint32_t>{
  public:
    uint64_t operator()(uint32_t value) const override {
      return MurMurHash<uint64_t>()(static_cast<uint64_t>(value));
    }
};

template<typename T, typename H = MurMurHash<T> >
class Hll {
  private:
    uint8_t bucketBits;
    uint8_t valueBits;
    uint64_t numberOfBuckets;
    uint64_t bucketSize;
    uint64_t bucketMask;
    uint64_t valueMask;
    uint8_t* synopsis;


  uint8_t leftMostSetBit(uint64_t hash) const {
    // We set the bucket bits to 0
    if (hash == 0)
      return 0;
    else
      /**
       * clz returns number of leading zero bits couting from the MSB
       * we have to add 1 to count the set bit
       * then we subtract bucketBits, since they are zeroed by valueMask
       */
      return (uint8_t)__builtin_clzll(hash & valueMask) + 1 - bucketBits;
  }

  void init(uint8_t bucketBits) {
    this -> bucketBits = bucketBits;
    this -> valueBits = 64 - bucketBits;
    this -> numberOfBuckets = 1UL << bucketBits;
    this -> bucketSize = 1;

    // Ex: with a 4 bits bucket on a 2 bytes size_t we want 1111 0000 0000 0000
    // So that's 10000 minus 1 shifted with 12 zeroes 1111 0000 0000 0000
    this -> bucketMask = (( 1UL << bucketBits ) - 1UL) << valueBits;

    // Ex: with a 4 bits bucket on a 16 bit size_t  we want 0000 1111 1111 1111
    // So that's 1 with 12 ( 16 - 4 ) zeroes 0001 0000 0000 0000, minus one = 0000 1111 1111 1111
    this -> valueMask = (1UL << valueBits ) - 1UL;

    this -> synopsis = new uint8_t[numberOfBuckets];
  }
    public:

  uint64_t bucket(uint64_t hash) {
    // Get the most significant bits and shift that
    // For example on a 16-bit platform, with a 4 bit bucketMask (valueBits = 12)
    // The value 0110 0111 0101 0001 is first bit masked to 0110 0000 0000 0000
    // And right shifted by 12 bits, which gives 0110, bucket 6.
    return (hash & bucketMask) >> valueBits;
  }


  Hll(uint8_t bucketBits) {
    init(bucketBits);
    //TODO: This will not work when buckets won't be a single byte
    memset( synopsis, 0, numberOfBuckets );
  }

  Hll(const uint8_t* synopsis, uint8_t bucketBits) {
    init(bucketBits);
    for(uint64_t i = 0; i < numberOfBuckets; i++) {
      this -> synopsis[i] = synopsis[i];
    }
  }

  /** Copy constructor */
  Hll(const Hll& other) : bucketBits(other.bucketBits),
    valueBits(other.valueBits),
    numberOfBuckets(other.numberOfBuckets),
    bucketSize(other.bucketSize),
    bucketMask(other.bucketMask),
    valueMask(other.valueMask),
    synopsis(new uint8_t[other.numberOfBuckets]) {
    memcpy(synopsis, other.synopsis, numberOfBuckets);
  }

  /** Move constructor */
  Hll(Hll&& other) noexcept : bucketBits(other.bucketBits),
    valueBits(other.valueBits),
    numberOfBuckets(other.numberOfBuckets),
    bucketSize(other.bucketSize),
    bucketMask(other.bucketMask),
    valueMask(other.valueMask) {
      std::swap(synopsis, other.synopsis);
    }

  /** Copy assignment operator */
  Hll& operator=(const Hll& other) {
    Hll tmp(other);
    *this = std::move(other);
    return *this;
  }

  /** Move assignment operator */
  Hll& operator=(Hll&& other) noexcept {
    std::swap(other);
    return *this;
  }

  ~Hll() {
    delete[] synopsis;
  }

  void add(T value) {
    H hashFunction;
    uint64_t hashValue = hashFunction(value);
    // We store in the synopsis for the the biggest leftmost one
    synopsis[bucket(hashValue)] = std::max(synopsis[bucket(hashValue)], leftMostSetBit(hashValue));
  }

  void add(const uint8_t otherSynopsis[]) {
    for (uint64_t i = 0; i < numberOfBuckets; i++) {
      synopsis[i] = std::max(synopsis[i], otherSynopsis[i]);
    }
  }

  void add(const Hll<T>& other) {
    assert(this->numberOfBuckets == other.numberOfBuckets);
    this->add(other.synopsis); 
  }

  uint8_t* getCurrentSynopsis() {
    return this -> synopsis;
  }

  uint64_t getNumberOfBuckets() const {
    return this -> numberOfBuckets;
  }

  /**
   * @return Size of synopsis in bytes
   */
  uint64_t getSynopsisSize() const {
    return this -> numberOfBuckets * bucketSize;
  }

  uint64_t approximateCountDistinct() {
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
      case 14:
        alpha = (0.7213 / (1.0 + (1.079 / (1<<14)))); //0.721253
      default:
        alpha = (0.7213 / (1.0 + (1.079 / static_cast<double>(numberOfBuckets))));
    }
    for (uint64_t i = 0; i < numberOfBuckets; i++)
    {
        harmonicMean += 1.0 / (1 << (synopsis[i]));
    }
    harmonicMean = numberOfBuckets / harmonicMean;
    return std::llround(0.5 + alpha * harmonicMean * numberOfBuckets);
  }

};

#endif
