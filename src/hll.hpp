#include "hll_raw.hpp"

#ifndef _HLL_H_
#define _HLL_H_


struct HLLHdr {
  char magic[2] = {'H','L'};
  char format;
  uint8_t bucketBase;
  char padding[4] = {'\0','\0','\0','\0'}; // padding to reach 8 bytes in length
};


template<typename T, typename H = MurMurHash<T> >
class Hll {

  HllRaw<T, H> hll;
  LinearCounting linearCounting;
  uint32_t lcThreshold;
  uint32_t biasCorrectedThreshold;

  static uint8_t formatToCode(Format format) {
    if(format == Format::NORMAL) return 0x01;
    else if(format == Format::COMPACT_6BITS) return 0x02;
    else if(format == Format::COMPACT_5BITS) return 0x04;
    else if(format == Format::COMPACT_4BITS) return 0x08;
    else assert(0);
  }

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
    // for the time being we skip the header
    HLLHdr hdr = *(reinterpret_cast<const HLLHdr*>(byteArray));
    const char* byteArrayHll = byteArray + sizeof(HLLHdr);
    if(format == Format::NORMAL) {
      hll.deserialize8Bits(byteArrayHll);

    } else if (format == Format::COMPACT_6BITS) {
      hll.deserialize6Bits(byteArrayHll);

    } else if (format == Format::COMPACT_5BITS) {
      uint8_t base = hdr.bucketBase;
      hll.deserialize5BitsWithBase(byteArrayHll, base);

    } else if (format == Format::COMPACT_4BITS) {
      uint8_t base = hdr.bucketBase;
      hll.deserialize4BitsWithBase(byteArrayHll, base);

    } else {
      assert(0);
    }
  }

  void serialize(char* byteArray, Format format) const {
    // for the time being we skip the header
    HLLHdr hdr;
    char* byteArrayHll = byteArray + sizeof(HLLHdr);
    if(format == Format::NORMAL) {
      hdr.format = formatToCode(format);
      memcpy(byteArray, reinterpret_cast<char*>(&hdr), sizeof(HLLHdr));

      hll.serialize8Bits(byteArrayHll);
    } else if (format == Format::COMPACT_6BITS) {
      hdr.format = formatToCode(format);
      memcpy(byteArray, reinterpret_cast<char*>(&hdr), sizeof(HLLHdr));

      hll.serialize6Bits(byteArrayHll);
    } else if (format == Format::COMPACT_5BITS) {
      uint8_t base = hll.serialize5BitsWithBase(byteArrayHll);
      hdr.bucketBase = base;
      hdr.format = formatToCode(format);

      memcpy(byteArray, reinterpret_cast<char*>(&hdr), sizeof(HLLHdr));
    } else if (format == Format::COMPACT_4BITS) {
      uint8_t base = hll.serialize4BitsWithBase(byteArrayHll);
      hdr.bucketBase = base;
      hdr.format = formatToCode(format);

      memcpy(byteArray, reinterpret_cast<char*>(&hdr), sizeof(HLLHdr));
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
    return hll.getSynopsisSize(format) + sizeof(HLLHdr);
  }

/**
 * Hll's error becomes significant for small cardinalities. For instance, when
 * the cardinality is 0, HLL(p=14) estimates it to ~11k.
 * To circumvent 
 */
  uint64_t approximateCountDistinct() {
    uint64_t e = this->hll.estimate();
    uint64_t ee;

    if(e <= biasCorrectedThreshold) {
      ee = BiasCorrectedEstimate::estimate(e, hll.getBucketBits());
    } else {
      ee = e;
    }

    uint64_t h;
    if(this->hll.emptyBucketsCount() != 0) {
      double v = static_cast<double>(hll.getNumberOfBuckets())/static_cast<double>(hll.emptyBucketsCount());
      h = hll.getNumberOfBuckets() * log(v);
    } else {
      h = ee;
    }

    if(h <= lcThreshold) {
      return h;
    } else {
      return ee;
    }
  }

};

#endif
