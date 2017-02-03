#include <iostream>
#include <cstring>
#include <cmath>
#include <memory>
#include <stdint.h>
#include <type_traits>
#include <utility>
#include <cassert>
#include "bias_corrected_estimate.hpp"
#include "linear_counting.hpp"
#include "murmur_hash.hpp"


const uint8_t LINEAR_COUNTING_BITS = 10;

#ifndef _HLL_H_
#define _HLL_H_

enum class Format {SPARSE, DENSE};

/**
 * T is the hashable type. The class can be used to count various types.
 * H is a class deriving from Hash<T>
 */
template<typename T, typename H = MurMurHash<T> >
class Hll {
/**
 * The line below is the fanciest thing I've ever done.
 * This enforces in compile time that the H parameter is a subclass of Hash<T>.
 *
 * Weirdly, static_assert is not a member of std::
 */
  static_assert(std::is_base_of<Hash<T>, H>::value,
    "Hll's H parameter has to be a subclass of Hash<T>");
private:
  /**
   * The below member variables' order aims at minimizing the memory fooprint
   * Please don't change it if it's not 100% necessary.
   */
  uint64_t numberOfBuckets;
  uint64_t bucketSize;
  uint64_t bucketMask;
  uint64_t valueMask;

  LinearCounting linearCounting;

  uint32_t biasCorrectedThreshold;

  uint8_t bucketBits;
  uint8_t valueBits;
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

  /**
   * This function is a direct implementation of the formula presented in
   * Flajolet's paper. According to it the cardinality estimation is calculated
   * as follows:
   *
   * E = \alpha_m * m^2 * sum_{j=1}^m{2^{-M[j]}}
   *
   * where:
   *    E is the estimation
   *    \alpha_m is a constant derived from another formula
   *    m is number of buckets
   *    M[j] is count from the j-th bucket
   *
   */
  uint64_t hllEstimate() const {
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
      // since 14 is our most common case, we make it computable in compile time
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
    // std::llround returns a long long
    // note: other rounding functions return a floating point or shorter types
    uint64_t hllEstimate = std::llround(0.5 + alpha * harmonicMean * numberOfBuckets);
    return hllEstimate;
  }


  void init(uint8_t bucketBits) {
    this -> bucketBits = bucketBits;
    this -> valueBits = 64 - bucketBits;
    this -> numberOfBuckets = 1UL << bucketBits;

    // Google's paper suggests to set the threshold to this value 
    this -> biasCorrectedThreshold = numberOfBuckets*5;

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

  /**
   * Below we implement the C++11's rule of five.
   * Since this class uses manual memory allocation, we have to to define:
   * - copy constructor,
   * - move constructor,
   * - copy assignment,
   * - move assignmnet,
   * - destructor.
   *
   * See: https://en.wikipedia.org/wiki/Rule_of_three_(C%2B%2B_programming)
   */
  Hll(uint8_t bucketBits) : linearCounting(LINEAR_COUNTING_BITS) {
    init(bucketBits);
    //TODO: This will not work when buckets won't be a single byte
    memset( synopsis, 0, numberOfBuckets );
  }

  Hll(const uint8_t* synopsis, uint8_t bucketBits) :
      linearCounting(LINEAR_COUNTING_BITS) {
    init(bucketBits);
    for(uint64_t i = 0; i < numberOfBuckets; i++) {
      this -> synopsis[i] = synopsis[i];
    }
  }

  /** Copy constructor */
  Hll(const Hll& other) : numberOfBuckets(other.numberOfBuckets),
    bucketSize(other.bucketSize),
    bucketMask(other.bucketMask),
    valueMask(other.valueMask),
    linearCounting(other.linearCounting),
    biasCorrectedThreshold(other.biasCorrectedThreshold),
    bucketBits(other.bucketBits),
    valueBits(other.valueBits),
    synopsis(new uint8_t[other.numberOfBuckets]) {
    memcpy(synopsis, other.synopsis, numberOfBuckets);
  }

  /** Move constructor */
  Hll(Hll&& other) noexcept : numberOfBuckets(other.numberOfBuckets),
    bucketSize(other.bucketSize),
    bucketMask(other.bucketMask),
    valueMask(other.valueMask),
    linearCounting(other.linearCounting),
    biasCorrectedThreshold(other.biasCorrectedThreshold),
    bucketBits(other.bucketBits),
    valueBits(other.valueBits) {
      // we swap the values, so that we get the synopsis from the temporary
      // object, and the current synopsis will be destroyed together with
      // the temporary object.
      std::swap(synopsis, other.synopsis);
    }

  /** Copy assignment operator */
  Hll& operator=(const Hll& other) {
    Hll tmp(other);
    *this = std::move(tmp);
    return *this;
  }

  /** Move assignment operator */
  Hll& operator=(Hll&& other) noexcept {
    delete[] synopsis;
    synopsis = other.synopsis;
    other.synopsis = nullptr;
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
    linearCounting.add(hashValue);

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
 * Hll's error becomes significant for small cardinalities. For instance, when
 * the cardinality is 0, HLL(p=14) estimates it to ~11k.
 * To circumvent 
 */
  uint64_t approximateCountDistinct() {
    return hllEstimate();
    // uint64_t hllEstimate = this->hllEstimate();
    // uint64_t linearCountingEstimate = this->linearCounting->estimate();

    // if(hllEstimate < biasCorrectedThreshold) {
    //     if(linearCountingEstimate < linearCountingThreshold) {
    //       return this->linearCounting->estimate();
    //     } else {
    //       return BiasCorrectedEstimate::estimate(hllEstimate, bucketBits);
    //     }
    // } else {
    //   return hllEstimate;
    // }
  }

void deserialize(const char* byteArray, Format format) {
  if(format == Format::SPARSE) {
    for(uint32_t i=0; i<getNumberOfBuckets(); ++i) {
      synopsis[i] = byteArray[i];
    }
  } else if (format == Format::DENSE) {
    deserializeDense(byteArray);
  }
}


void serialize(char* byteArray, Format format) const {
  if(format == Format::SPARSE) {
    for(uint32_t i=0; i<getNumberOfBuckets(); ++i) {
      byteArray[i] = synopsis[i];
    }
  } else if (format == Format::DENSE) {
    serializeDense(byteArray);
  } else {
    //TODO: replace it with an exception or sth more meaningful
    assert(0);
  }
}

uint32_t getSynopsisSize(Format format) {
  if(format == Format::SPARSE) {
    return numberOfBuckets * bucketSize;

  } else if(format == Format::DENSE) {
    uint8_t outputBucketSizeBits = static_cast<uint8_t>(std::log2(valueBits) + 0.5); //6
    uint32_t outputArraySizeBits = numberOfBuckets * outputBucketSizeBits;
    uint32_t outputArraySize = outputArraySizeBits >> 3; //12288
    return outputArraySize;
  } else {
    //TODO: replace it with an exception or sth more meaningful
    assert(0);
  }
}

/**
 * We use dense representation for storing the buckets.
 * In the Hll class we store the synopsis as an array of 2^14 bytes. In fact,
 * since the counters use only 6 bits, when storing the synopsis on a permanent
 * storage we can compress it to 2^14 * 6 bits =  12288 bytes.
 * The idea is to split the buckets into groups of 4 and to store them in 3 bytes
 * (since 4 * 6 bits = 24 bits = 3 bytes).
 *
 * Hence, we use the following representation:
 *
 * +--------+--------+--------+---//
 * |00000011|11112222|22333333|4444
 * +--------+--------+--------+---//
 *
 * In order to deserialize an array of bytes into buckets, we iterate over
 * groups of three bytes which get stored in four buckets.
 * Likewise, to serialize the buckets, we iterate over groups of 4 buckets
 * which get stored in three bytes.
 *
 * In the functions below it's essential to set value of a single byte in only
 * one assignment. Otherwise we risk encountering a write after write hazard,
 * which could make the operation significantly slower.
 */

  void deserializeDense(const char* byteArray1) {
    //bgidx stands for bucket group index
    //
    const unsigned char* byteArray = reinterpret_cast<const unsigned char*>(byteArray1);
    for(uint32_t bgidx = 0; bgidx < getNumberOfBuckets()/4; ++bgidx) {
      synopsis[bgidx*4] = byteArray[bgidx*3] >> 2;
      synopsis[bgidx*4+1] = ((byteArray[bgidx*3] & 0x3) << 4) | (byteArray[bgidx*3+1] >> 4);
      synopsis[bgidx*4+2] = ((byteArray[bgidx*3+1] & 0xF) << 2) | (byteArray[bgidx*3+2] >> 6);
      synopsis[bgidx*4+3] = (byteArray[bgidx*3+2] & 0x3F);
    }
  }

  void serializeDense(char* byteArray1) const {
    //bgidx stands for bucket group index
    unsigned char* byteArray = reinterpret_cast<unsigned char*>(byteArray1);
    for(uint32_t bgidx = 0; bgidx < getNumberOfBuckets()/4; ++bgidx) {
      byteArray[bgidx*3]   = (synopsis[bgidx*4] << 2)    | (synopsis[bgidx*4+1] >> 4);
      byteArray[bgidx*3+1] = (synopsis[bgidx*4+1] << 4)  | (synopsis[bgidx*4+2] >> 2);
      byteArray[bgidx*3+2] = (synopsis[bgidx*4+2] << 6 ) | synopsis[bgidx*4+3];
    }
  }

};

#endif
