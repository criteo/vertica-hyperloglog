/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

#include "hll_raw.hpp"
#include <bitset>

#ifndef _HLL_H_
#define _HLL_H_


struct HLLHdr {
  char magic[2] = {'H','L'};
  char format;
  uint8_t bucketBase;
  uint16_t bucketSparseCount; // Only meaning if format is sparse - only maintained at serialization
  char padding[2] = {'\0','\0'}; // padding to reach 8 bytes in length
};

typedef std::pair<std::unique_ptr<uint8_t[]>, size_t> SizedBuffer;

template<typename T, typename H = MurMurHash<T> >
class Hll {

  HllRaw<T, H> hll;
  const HLLHdr *header;
  LinearCounting linearCounting;

  static uint8_t formatToCode(Format format) {
    uint8_t ret;
    if(format == Format::NORMAL) ret = 0x01;
    else if(format == Format::COMPACT_6BITS) ret = 0x02;
    else if(format == Format::COMPACT_5BITS) ret = 0x04;
    else if(format == Format::COMPACT_4BITS) ret = 0x08;
    else if(format == Format::SPARSE) ret = 0x10;
    else throw SerializationError("Unknown format parameter in formatToCode");
    return ret;
  }

public:
  Hll(uint8_t bucketBits, uint8_t* payload, uint32_t hashSeed = MURMURHASH_DEFAULT_SEED) :
    hll(bucketBits, payload + sizeof(HLLHdr), hashSeed),
    header(reinterpret_cast<HLLHdr*>(payload)),
    linearCounting(bucketBits - 4) {}

  static Hll wrapRawBuffer(uint8_t bucketBits, uint8_t* payload, size_t length, uint32_t hashSeed = MURMURHASH_DEFAULT_SEED) {
      return Hll(bucketBits, payload, hashSeed);
  }

  void reset() {
    hll.reset();
    HLLHdr *hdr = const_cast<HLLHdr*>(header);
    hdr->magic[0] = 'H';
    hdr->magic[1] = 'L';
    hdr->bucketBase = 0;
    hdr->bucketSparseCount = 0;
    hdr->format = formatToCode(Format::NORMAL);
  }

  void fold(const uint8_t* byteArray, size_t length) {
    if (length < sizeof(HLLHdr)) {
      throw SerializationError("payload is not big enough to contain header");
    }
    length -= sizeof(HLLHdr);
    HLLHdr hdr = *(reinterpret_cast<const HLLHdr*>(byteArray));
    const uint8_t* byteArrayHll = byteArray + sizeof(HLLHdr);

    if(hdr.format == formatToCode(Format::SPARSE)) {
      hll.fold8BitsSparse(byteArrayHll, hdr.bucketSparseCount, length);
    } else if(hdr.format == formatToCode(Format::NORMAL)) {
      hll.fold8Bits(byteArrayHll, length);
    } else if (hdr.format == formatToCode(Format::COMPACT_6BITS)) {
      hll.fold6Bits(byteArrayHll, length);
    } else if (hdr.format == formatToCode(Format::COMPACT_5BITS)) {
      hll.fold5BitsWithBase(byteArrayHll, hdr.bucketBase, length);
    } else if (hdr.format == formatToCode(Format::COMPACT_4BITS)) {
      hll.fold4BitsWithBase(byteArrayHll, hdr.bucketBase, length);
    } else {
      throw SerializationError("Unknown format parameter in fold().");
    }
  }

  void serialize(uint8_t* byteArray, Format format) const {
    // for the time being we skip the header and serialize it once
    // the buckets are written down
    HLLHdr hdr;

    uint8_t* byteArrayHll = byteArray + sizeof(HLLHdr);
    uint8_t base = 0;
    if (format == Format::SPARSE) {
      hdr.bucketSparseCount = hll.serialize8BitsSparse(byteArrayHll);
    } else if(format == Format::NORMAL) {
      hll.serialize8Bits(byteArrayHll);
    } else if (format == Format::COMPACT_6BITS) {
      hll.serialize6Bits(byteArrayHll);
    } else if (format == Format::COMPACT_5BITS) {
      base = hll.serialize5BitsWithBase(byteArrayHll);
    } else if (format == Format::COMPACT_4BITS) {
      base = hll.serialize4BitsWithBase(byteArrayHll);
    } else {
      throw SerializationError("Unknown format parameter in serialize().");
    }
    // serialize the header as well
    hdr.bucketBase = base;
    hdr.format = formatToCode(format);
    *reinterpret_cast<HLLHdr*>(byteArray) = hdr;
  }

  void add(const Hll& other) {
    this->hll.add(other.hll);
  }

  void add(T value) {
    hll.add(value);
  }

  void printBuckets() const {
    hll.printBuckets();
  }

  uint64_t getSerializedBufferSize(Format format) const {
    return this->hll.getSerializedSynopsisSize(format) + sizeof(HLLHdr);
  }

  static uint64_t getMaxDeserializedBufferSize(uint8_t precision) {
      return HllRaw<T,H>::getDeserializedSynopsisSize(precision) + sizeof(HLLHdr);
  }

  static uint64_t getMaxSerializedBufferSize(Format format, uint8_t precision) {
    return HllRaw<T,H>::getMaxSerializedSynopsisSize(format, precision) + sizeof(HLLHdr);
  }

  static SizedBuffer makeDeserializedBuffer(uint8_t precision) {
    uint64_t length = getMaxDeserializedBufferSize(precision);
    uint8_t *buffer = new uint8_t[length];
    memset(buffer, 0, length);
    return std::make_pair(std::unique_ptr<uint8_t[]>(buffer), length);
  }

  static SizedBuffer makeSerializedBuffer(Format format, uint8_t precision) {
    uint64_t length = getMaxSerializedBufferSize(format, precision);
    uint8_t *buffer = new uint8_t[length];
    memset(buffer, 0, length);
    return std::make_pair(std::unique_ptr<uint8_t[]>(buffer), length);
  }

  bool isBetterSerializedSparse() const {
    return this->hll.getNumberOfSetBuckets() < 256;
  }

  /**
   * get cardinality estimation using LogLog-Beta algorithm
   */

  uint64_t approximateCountDistinct_beta() const {
    return this->hll.betaEstimate();
  }

/**
 * Hll's error becomes significant for small cardinalities. For instance, when
 * the cardinality is 0, HLL(p=14) estimates it to ~11k.
 * To circumvent
 */
  uint64_t approximateCountDistinct() const {
    uint64_t e = this->hll.estimate();
    uint64_t ee;
    // Google's paper suggests to set the threshold to this value
    uint64_t biasCorrectedThreshold = hll.getNumberOfBuckets()*5;
    uint64_t lcThreshold = linearCounting.getLinearCountingThreshold(this->hll.getBucketBits());

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
