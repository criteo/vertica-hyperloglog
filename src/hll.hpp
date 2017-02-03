#include "hll_raw.hpp"

#ifndef _HLL_H_
#define _HLL_H_


template<typename T, typename H = MurMurHash<T> >
class Hll {

  HllRaw<T, H> hll;
  LinearCounting linearCounting;
  uint32_t lcThreshold;
  uint32_t biasCorrectedThreshold;

public:
  Hll(uint8_t bucketBits, uint8_t lcBits) : 
      hll(bucketBits),
      linearCounting(lcBits) {
    // Google's paper suggests to set the threshold to this value 
   this -> biasCorrectedThreshold = hll.getNumberOfBuckets()*5;
   this -> lcThreshold = linearCounting.getLinearCountingThreshold(bucketBits);
  }

  Hll(uint8_t bucketBits) : Hll(bucketBits, bucketBits-4) {}

  void deserialize(const char* byteArray, Format format) {
    if(format == Format::SPARSE) {
      hll.deserializeSparse(byteArray);
    } else if (format == Format::DENSE) {
      hll.deserializeDense(byteArray);
    } else {
      assert(0);
    }
  }

  void serialize(char* byteArray, Format format) const {
    if(format == Format::SPARSE) {
      hll.serializeSparse(byteArray);
    } else if (format == Format::DENSE) {
      hll.serializeDense(byteArray);
    } else {
      //TODO: replace it with an exception or sth more meaningful
      assert(0);
    }
  }

  void add(const Hll& other) {
    this->hll.add(other.hll); 
  }

  void add(T value) {
    hll.add(value);
    linearCounting.add(H()(value));
  }

  // TODO: this function return just size of raw HLL synopsis
  // TODO: there is no header, no linearCounting
  uint32_t getSynopsisSize(Format format) {
    return hll.getSynopsisSize(format);
  }

/**
 * Hll's error becomes significant for small cardinalities. For instance, when
 * the cardinality is 0, HLL(p=14) estimates it to ~11k.
 * To circumvent 
 */
  uint64_t approximateCountDistinct() {
    return hll.estimate();
  //   uint64_t hllEstimate = this->hll.estimate();
  //   uint64_t lcEstimate = this->linearCounting.estimate();

  //   if(hllEstimate < biasCorrectedThreshold) {
  //       if(lcEstimate < lcThreshold) {
  //         return lcEstimate;
  //       } else {
  //         return BiasCorrectedEstimate::estimate(hllEstimate, bucketBits);
  //       }
  //   } else {
  //     return hllEstimate;
  //   }
  }

};

#endif
