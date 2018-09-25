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

#include <exception>
#include <algorithm>
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


#ifndef _HLL_RAW_H_
#define _HLL_RAW_H_

struct SerializationError : public virtual std::runtime_error {
  SerializationError(const char* message) : std::runtime_error(std::string(message)) {}
};

enum class Format {NORMAL, COMPACT_6BITS, COMPACT_5BITS, COMPACT_4BITS, SPARSE};

/**
 * T is the hashable type. The class can be used to count various types.
 * H is a class deriving from Hash<T>
 */
template<typename T, typename H = MurMurHash<T> >
class HllRaw {
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
  uint8_t bucketBits;
  uint8_t valueBits;
  uint8_t* synopsis;

  const uint32_t DEFAULT_HASH_SEED = 27072015;
  uint32_t hashSeed; // Ability to get different hashing for same data

  // 8 constant values per precision for polynom (taken from LogLog-beta paper and appendix)
  // Source : https://github.com/colings86/elasticsearch/blob/b0093fc059b615d9ca2136efec0fc880f2be1815/core/src/main/java/org/elasticsearch/search/aggregations/metrics/cardinality/HyperLogLogBeta.java#L56
  static const size_t nCoefficients = 8;
  const std::vector<std::vector<double>> betaConstants = {
    // precision 4
    { 129.811426122, -127.758849345, -144.856462515, 185.084979526, -13.2281686587, 43.5841078986, -383.603665383, 154.492845304 },
    // precision 5
    { -13.0055889181, 8.58672362771, 9.72695761533, 16.5156287003, -17.0875475369, -4.31703226621, 10.912981826, -3.12448718477 },
    // precision 6
    { 1733.13875391, -1699.65637955, -1001.35164911, -79.5001457157, -232.449115309, 48.0467680133, -13.4033856565, 0.0432949807375 },
    // precision 7
    { -683.172241152, 699.316157869, 275.507508944, 219.266866262, -57.9057954518, 44.5955453694, -8.46896092799, 1.1725158865 },
    // precision 8
    { -19.2122824148, 16.5377254144, 12.9159210689, 5.15486460551, -3.55567694845, 2.41367059785, -0.485452949344, 0.0512917786702 },
    // precision 9
    { -4.85617520421, 3.35826651543, 2.90853842731, 2.93901916626, -2.37054651785, 1.1737214086, -0.22118210602, 0.0191092511669 },
    // precision 10
    { -3.11898253134, 9.25125002906, -17.8005229174, 21.5341553715, -10.8362087112, 3.00000412385, -0.408463351115, 0.0245033071993 },
    // precision 11
    { -0.172965890626, -8.81246455315, 21.0409860425, -16.7375649792, 6.44544077588, -1.30921425783, 0.136002575029, -0.0058234826948 },
    // precision 12
    { -0.356378277813, 3.24074126277, -5.90931639379, 4.23324241571, -1.3182929368, 0.208792006071, -0.0152184183956, 0.000471786845185 },
    // precision 13
    { -0.382200101569, 1.80366843702, -2.96538207991, 2.36112694627, -0.822043918775, 0.158042001067, -0.0150086424267, 0.000708114274487 },
    // precision 14
    {-3.70393914146161e-01,7.04718232678681e-02,1.73936855679645e-01,1.63398393221669e-01,-9.23774466279541e-02,3.73802699931568e-02,-5.38415897770915e-03,4.24187633936774e-04},
    // precision 15
    { -0.560387006169, 59.8108631214, -120.370073477, 86.0699330472, -28.9537963009, 5.03900955483, -0.439967193352, 0.0157440364892 },
    // precision 16
    { -0.391416234743, 1.85229689725, -8.882746972, 7.48086624254, -2.80472962045, 0.568918604145, -0.0583909163033, 0.00261029795878 },
     // precision 17
     { -0.339120524001, -72.1994426957, 113.185471625, -62.8282169476, 16.6562758098, -2.26144354617, 0.150939847827, -0.0036642817302 },
     // precision 18
    { -0.372494978401, 39.9302213478, -69.8219564407, 43.7971215279, -13.1312309526, 2.0820456299, -0.1696126329, 0.00591592212173 }
  };

  size_t betaDataIndex() const {
    return bucketBits - 4;
  }

  uint8_t leftMostSetBit(uint64_t hash) const {
    // We set the bucket bits to 0
    if (hash == 0)
      return 0;
    else {
      // Ex: with a 4 bits bucket on a 16 bit size_t  we want 0000 1111 1111 1111
      // So that's 1 with 12 ( 16 - 4 ) zeroes 0001 0000 0000 0000, minus one = 0000 1111 1111 1111
      const uint64_t valueMask = (1UL << valueBits ) - 1UL;
      /**
       * clz returns number of leading zero bits couting from the MSB
       * we have to add 1 to count the set bit
       * then we subtract bucketBits, since they are zeroed by valueMask
       */
      return (uint8_t)__builtin_clzll(hash & valueMask) + 1 - bucketBits;
    }
  }

  static uint64_t countNumberOfBuckets(uint8_t precision) {
    return 1UL << precision;
  }

  void init(uint8_t bucketBits, uint8_t* synopsis, uint32_t hashSeed = MURMURHASH_DEFAULT_SEED) {
    this -> bucketBits = bucketBits;
    this -> valueBits = 64 - bucketBits;

    this -> synopsis = synopsis;
    this -> hashSeed = hashSeed;

    if (!(bucketBits >= 4 && bucketBits <= 18)) {
      throw SerializationError("precision has to be between 4 and 18");
    }
  }
public:



  uint64_t bucket(uint64_t hash) {
    // Ex: with a 4 bits bucket on a 2 bytes size_t we want 1111 0000 0000 0000
    // So that's 10000 minus 1 shifted with 12 zeroes 1111 0000 0000 0000
    const uint64_t bucketMask = (( 1UL << bucketBits ) - 1UL) << valueBits;

    // Get the most significant bits and shift that
    // For example on a 16-bit platform, with a 4 bit bucketMask (valueBits = 12)
    // The value 0110 0111 0101 0001 is first bit masked to 0110 0000 0000 0000
    // And right shifted by 12 bits, which gives 0110, bucket 6.
    return (hash & bucketMask) >> valueBits;
  }

  HllRaw(uint8_t bucketBits, uint8_t *synopsis, uint32_t newHashSeed = MURMURHASH_DEFAULT_SEED) {
    init(bucketBits, synopsis, newHashSeed);
  }

  void reset() {
    memset( this->synopsis, 0, this->getNumberOfBuckets() );
  }

  ~HllRaw() {}

  uint64_t add(T value) {
    H hashFunction;
    uint64_t hashValue = hashFunction(value, hashSeed);

    const uint32_t dstBucket = bucket(hashValue);
    // We store in the synopsis for the the biggest leftmost one
    synopsis[dstBucket] = std::max(synopsis[dstBucket], leftMostSetBit(hashValue));
    return hashValue;
  }

  void add(const uint8_t otherSynopsis[]) {
    const uint32_t numberOfBucketsConst = this->getNumberOfBuckets();
    uint8_t* __restrict__ synopsis_ = synopsis;

    // note: LOOP VECTORIZED
    for (uint64_t i = 0; i < numberOfBucketsConst; i++) {
      synopsis_[i] = std::max(synopsis_[i], otherSynopsis[i]);
    }
  }

  void add(const HllRaw<T>& other) {
    if (!(this->getNumberOfBuckets() == other.getNumberOfBuckets())) {
      throw SerializationError("Synopsis are in different format");
    }
    this->add(other.synopsis);
  }

  //TODO (pierre) to remove
  void setBucketValue(const uint32_t bucket_idx, const uint8_t val) {
    synopsis[bucket_idx] = val;
  }

  uint8_t getBucketBits() const {
    return this->bucketBits;
  }

  uint8_t* getCurrentSynopsis() {
    return this->synopsis;
  }

  uint64_t getNumberOfBuckets() const {
    return countNumberOfBuckets(bucketBits);
    //return this->numberOfBuckets;
  }

  uint64_t getNumberOfSetBuckets() const {
    uint64_t set = 0;
    for (uint64_t i = 0; i < this->getNumberOfBuckets(); i++) {
      set += (int)(this->synopsis[i] != 0);
    }
    return set;
  }

  uint64_t getDeserializedSynopsisSize() const {
    return HllRaw<T,H>::countNumberOfBuckets(bucketBits);
  }

  static uint64_t getDeserializedSynopsisSize(uint8_t precision) {
    return HllRaw<T,H>::countNumberOfBuckets(precision);
  }

  uint64_t getSerializedSynopsisSize(Format format) const {
    if (format == Format::SPARSE) {
      return getNumberOfSetBuckets() * 3;
    }
    return HllRaw<T,H>::getMaxSerializedSynopsisSize(format, bucketBits);
  }

  static uint64_t getMaxSerializedSynopsisSize(Format format, uint8_t precision) {
    uint8_t bucketBits = precision;
    uint8_t valueBits = 64 - bucketBits;
    uint64_t numberOfBuckets = 1UL << bucketBits;

    uint64_t ret = 0;

    if(format == Format::NORMAL) {
      ret = numberOfBuckets;
    } else if(format == Format::COMPACT_6BITS) {
      uint8_t outputBucketSizeBits = static_cast<uint8_t>(std::log2(valueBits) + 0.5); //6
      uint32_t outputArraySizeBits = numberOfBuckets * outputBucketSizeBits;
      uint32_t outputArraySize = outputArraySizeBits >> 3; //12288
      ret = outputArraySize;

    } else if(format == Format::COMPACT_5BITS) {
      // we need 5 bits for every bucket
      // also, these buckets are stored in bytes, so we divide by 8
      ret = (5 * numberOfBuckets) / 8;
    } else if(format == Format::COMPACT_4BITS) {
      ret = (4 * numberOfBuckets) / 8;
    } else if(format == Format::SPARSE) {
      ret = numberOfBuckets * 3;
    } else {
      throw SerializationError("Cannot get Maximum serialized size for format");
    }
    return ret;
  }

  uint32_t emptyBucketsCount() const {
    uint32_t emptyBuckets = 0;
    // note: LOOP VECTORIZED
    for (uint64_t i = 0; i < this->getNumberOfBuckets(); i++) {
      emptyBuckets += static_cast<uint32_t>(synopsis[i] == 0);
    }
    return emptyBuckets;
  }

  void printBuckets() const {
    for (uint64_t i = 0; i < this->getNumberOfBuckets(); i++)
    {
        std::cout << 0+synopsis[i] << " ";
    }
    std::cout << std::endl;
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
  uint64_t estimate() const {
    double harmonicMean = 0.0;
    double alpha = getAlpha();
    for (uint64_t i = 0; i < this->getNumberOfBuckets(); i++)
    {
        uint64_t numerator = 1UL << (synopsis[i]);
        harmonicMean += 1.0 / numerator;
    }
    harmonicMean = this->getNumberOfBuckets() / harmonicMean;
    // std::llround returns a long long
    // note: other rounding functions return a floating point or shorter types
    uint64_t hllEstimate = std::llround(/*0.5 + */alpha * harmonicMean * this->getNumberOfBuckets());
    return hllEstimate;
  }

  double getAlpha() const {
    // Alpha computation (see paper)
    switch (bucketBits) {
      case 4:
        return 0.673;
      case 5:
        return 0.697;
      case 6:
        return 0.709;
      // since 11 and 14 are most likely cases, we make it computable in compile time
      case 11:
        return (0.7213 / (1.0 + (1.079 / (1<<11)))); //0.72092
      case 14:
        return (0.7213 / (1.0 + (1.079 / (1<<14)))); //0.721253
      default:
        return (0.7213 / (1.0 + (1.079 / static_cast<double>(getNumberOfBuckets()))));
    }
  }

  double getBeta(uint64_t zInput) const {
    if (zInput == 0){
        return 0.0;
    }

    double result = betaConstants[betaDataIndex()][0] * zInput;
    double zl = std::log(zInput + 1);
    // Get sum of Polynom beta_i * zl^i
    for(size_t i = 1; i < nCoefficients; ++i){
      result += std::pow(zl, i) * betaConstants[betaDataIndex()][i];
    }
    return result;
  }

   /**
   * This function is implementation of the formula presented in
   * https://arxiv.org/abs/1612.02284
   * , it returns LogLog-Beta cardinality estimation
   */
  uint64_t betaEstimate() const {
    double harmonicMean = 0.0;
    double alpha = getAlpha();

    uint64_t numberOfZeroes = 0;
    for (uint64_t i = 0; i < this->getNumberOfBuckets(); i++)
    {
      if (synopsis[i] == 0){
        ++numberOfZeroes;
      }
      uint64_t numerator = 1UL << (synopsis[i]);
      harmonicMean += 1.0 / numerator;
    }
    harmonicMean = this->getNumberOfBuckets() / (harmonicMean + getBeta(numberOfZeroes));
    // std::llround returns a long long
    // note: other rounding functions return a floating point or shorter types
    uint64_t hllEstimate = std::llround(alpha * harmonicMean * (getNumberOfBuckets() - numberOfZeroes) );
    return hllEstimate;
  }

  // Deserialize and add in one pass
  void fold8BitsSparse(const uint8_t* __restrict__ byteArray, uint16_t setBuckets, size_t length) {
    uint8_t* __restrict__ synopsis_ = this->synopsis;
    if (length < setBuckets * 3) {
      throw SerializationError("Payload is not big enough for all advertised buckets");
    }

    while (setBuckets-- > 0) {
      uint16_t id = *reinterpret_cast<const uint16_t *>(byteArray + (3 * setBuckets));
      if (id >= this->getNumberOfBuckets()) {
        throw SerializationError("Bucket id is not valid when decoding sparse");
      }
      synopsis_[id] = std::max(synopsis_[id], byteArray[(3 * setBuckets) + 2]);
    }
  }

  uint16_t serialize8BitsSparse(uint8_t* __restrict__ byteArray) const {
    const uint32_t numberOfBucketsConst = this->getNumberOfBuckets();
    uint8_t* __restrict__ synopsis_ = this->synopsis;

    // note: LOOP VECTORIZED
    uint16_t setBuckets = 0;
    for(uint32_t i=0; i< numberOfBucketsConst; ++i) {
      if (synopsis_[i] != 0) {
        *reinterpret_cast<uint16_t *>(byteArray + (3 * setBuckets)) = (uint16_t) i;
        byteArray[(3 * setBuckets) + 2] = synopsis_[i];
        setBuckets++;
      }
    }
    return setBuckets;
  }


  // Deserialize and add in one pass
  void fold8Bits(const uint8_t* __restrict__ byteArray, size_t length) {
    uint8_t* __restrict__ synopsis_ = this->synopsis;
    const uint32_t numberOfBucketsConst = this->getNumberOfBuckets();
    if (length < numberOfBucketsConst) {
      throw SerializationError("Payload is not big enough for all advertised buckets");
    }
    //note: LOOP VECTORIZED
    for(uint32_t i = 0; i < numberOfBucketsConst; ++i) {
      synopsis_[i] = std::max(synopsis_[i], byteArray[i]);
    }
  }

  void serialize8Bits(uint8_t* __restrict__ byteArray) const {
    const uint32_t numberOfBucketsConst = this->getNumberOfBuckets();
    uint8_t* __restrict__ synopsis_ = this->synopsis;

    // note: LOOP VECTORIZED
    for(uint32_t i=0; i<numberOfBucketsConst; ++i) {
      byteArray[i] = synopsis_[i];
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

  // Deserialize and add in one pass
  void fold6Bits(const uint8_t* __restrict__ byteArray, size_t length) {
    uint8_t* __restrict__ synopsis_ = this->synopsis;
    const uint32_t numberOfBucketsConst = this->getNumberOfBuckets();
    const uint32_t maxExpectedSize = ((numberOfBucketsConst / 4) -1) * 3 + 2; // offset of highest 6 bit bucket packed
    if (length < maxExpectedSize) { //
      std::string err = std::string("Payload is not big enough for all advertised buckets [") + std::to_string(length) + " - " + std::to_string(maxExpectedSize) + "]";
      throw SerializationError(err.c_str());
    }

    //note: LOOP VECTORIZED
    for(uint32_t bgidx = 0; bgidx < numberOfBucketsConst/4; ++bgidx) {
      synopsis_[bgidx*4] = std::max(synopsis_[bgidx*4], (uint8_t)(byteArray[bgidx*3] >> 2));
      synopsis_[bgidx*4+1] = std::max(synopsis_[bgidx*4+1], (uint8_t)(((byteArray[bgidx*3] & 0x3) << 4) | (byteArray[bgidx*3+1] >> 4)));
      synopsis_[bgidx*4+2] = std::max(synopsis_[bgidx*4+2], (uint8_t)(((byteArray[bgidx*3+1] & 0xF) << 2) | (byteArray[bgidx*3+2] >> 6)));
      synopsis_[bgidx*4+3] = std::max(synopsis_[bgidx*4+3], (uint8_t)((byteArray[bgidx*3+2] & 0x3F)));
    }
  }

  void serialize6Bits(uint8_t* __restrict__ byteArray) const {
    //bgidx stands for bucket group index
    uint8_t* __restrict__ synopsis_ = this->synopsis;
    const uint32_t numberOfBucketsConst = this->getNumberOfBuckets();

    //note: LOOP VECTORIZED
    for(uint32_t bgidx = 0; bgidx < numberOfBucketsConst/4; ++bgidx) {
      byteArray[bgidx*3]   = (synopsis_[bgidx*4] << 2)    | (synopsis_[bgidx*4+1] >> 4);
      byteArray[bgidx*3+1] = (synopsis_[bgidx*4+1] << 4)  | (synopsis_[bgidx*4+2] >> 2);
      byteArray[bgidx*3+2] = (synopsis_[bgidx*4+2] << 6 ) | synopsis_[bgidx*4+3];
    }
  }

/**
 * Following functions serialize and deserialize the buckets using
 * 5 bits per bucket. To fit the buckets nicely in the array of bytes,
 * we have to split the buckets into groups of 8 and put them in 5
 * bytes as follows:
 *
 *   Byte 0   Byte 1   Byte 2   Byte 3   Byte 4
 * +--------+--------+--------+--------+--------+---//
 * +00000111|11222223|33334444|45555566|66677777|...
 * +--------+--------+--------+--------+--------+---//
 */

  // Deserialize and add in one pass
  void fold5BitsWithBase(const uint8_t* __restrict__ byteArray, uint8_t base, size_t length) {
    uint8_t* __restrict__ synopsis_ = synopsis;
    const uint32_t numberOfBucketsConst = this->getNumberOfBuckets();
    const uint32_t maxExpectedSize = ((numberOfBucketsConst / 8) - 1) * 5 + 4; // offset of highest 5 bit bucket packed
    if (length < maxExpectedSize) { //
      std::string err = std::string("Payload is not big enough for all advertised buckets [") + std::to_string(length) + " - " + std::to_string(maxExpectedSize) + "]";
      throw SerializationError(err.c_str());
    }
    // note: LOOP VECTORIZED
    for(uint32_t bgidx = 0; bgidx < numberOfBucketsConst/8; ++bgidx) {
      synopsis_[bgidx*8]   = std::max(synopsis_[bgidx*8], (uint8_t) (base +   (byteArray[bgidx*5] >> 3)));
      synopsis_[bgidx*8+1] = std::max(synopsis_[bgidx*8+1], (uint8_t) (base + (((byteArray[bgidx*5]   & 0x07) << 2) | (byteArray[bgidx*5+1] >> 6))));
      synopsis_[bgidx*8+2] = std::max(synopsis_[bgidx*8+2], (uint8_t) (base +  ((byteArray[bgidx*5+1] & 0x3E) >> 1)));
      synopsis_[bgidx*8+3] = std::max(synopsis_[bgidx*8+3], (uint8_t) (base + (((byteArray[bgidx*5+1] & 0x01) << 4) | (byteArray[bgidx*5+2] >> 4))));
      synopsis_[bgidx*8+4] = std::max(synopsis_[bgidx*8+4], (uint8_t) (base + (((byteArray[bgidx*5+2] & 0x0F) << 1) | (byteArray[bgidx*5+3] >> 7))));
      synopsis_[bgidx*8+5] = std::max(synopsis_[bgidx*8+5], (uint8_t) (base +  ((byteArray[bgidx*5+3] & 0x7C) >> 2)));
      synopsis_[bgidx*8+6] = std::max(synopsis_[bgidx*8+6], (uint8_t) (base + (((byteArray[bgidx*5+3] & 0x03) << 3) | (byteArray[bgidx*5+4] >> 5))));
      synopsis_[bgidx*8+7] = std::max(synopsis_[bgidx*8+7], (uint8_t) (base +   (byteArray[bgidx*5+4] & 0x1F)));
    }
  }

  uint8_t serialize5BitsWithBase(uint8_t* __restrict__ byteArray) const {
    uint8_t base = *std::min_element(synopsis, synopsis + this->getNumberOfBuckets());

    // we iterate over groups of 8 buckets
    const uint32_t numberOfBucketsConst = this->getNumberOfBuckets();
    uint8_t buckets[8];
    uint8_t* __restrict__ synopsis_ = this->synopsis;
    for(uint32_t bgidx = 0; bgidx < numberOfBucketsConst / 8; ++bgidx) {

      // normalize the buckets, i.e. subtract the base (min. value) and
      // make sure that the remainder fits into 5 bits (and cut off if not)
      for(uint32_t bidx = 0; bidx < 8; ++bidx) {
        const uint32_t bgidx_ = bgidx;
        uint8_t normBucket = synopsis_[bgidx_*8 + bidx]-base; // normalized bucket
        const uint8_t maxValIn5Bits = ((1<<5)-1); // max value fitting 5 bits
        buckets[bidx] = normBucket > maxValIn5Bits ? maxValIn5Bits : normBucket;
      }
      byteArray[bgidx*5]   = (buckets[0] << 3) | (buckets[1] >> 2);
      byteArray[bgidx*5+1] = (buckets[1] << 6) | (buckets[2] << 1) | (buckets[3] >> 4);
      byteArray[bgidx*5+2] = (buckets[3] << 4) | (buckets[4] >> 1);
      byteArray[bgidx*5+3] = (buckets[4] << 7) | (buckets[5] << 2) | (buckets[6] >> 3);
      byteArray[bgidx*5+4] = (buckets[6] << 5) | (buckets[7]);
    }
    return base;
  }

/*
 * +--------+--------+---//
 * +00001111|22223333|...
 * +--------+--------+---//
 */

  // Deserialize and add in one pass
  void fold4BitsWithBase(const uint8_t* __restrict__ byteArray, uint8_t base, size_t length) {
    uint8_t* __restrict__ synopsis_ = synopsis;
    const uint32_t numberOfBucketsConst = this->getNumberOfBuckets();
    const uint32_t maxExpectedSize = (numberOfBucketsConst/2); // offset of highest 4 bit bucket packed
    if (length < maxExpectedSize) { //
      std::string err = std::string("Payload is not big enough for all advertised buckets [") + std::to_string(length) + " - " + std::to_string(maxExpectedSize) + "]";
      throw SerializationError(err.c_str());
    }

    // note: LOOP VECTORIZED
    for(uint32_t bgidx = 0; bgidx < numberOfBucketsConst/2; ++bgidx) {
      synopsis_[bgidx*2]   = std::max(synopsis_[bgidx*2], (uint8_t) (base + (byteArray[bgidx] >> 4)));
      synopsis_[bgidx*2+1] = std::max(synopsis_[bgidx*2+1], (uint8_t) (base + (byteArray[bgidx] & 0x0f)));
    }
  }

  uint8_t serialize4BitsWithBase(uint8_t* __restrict__ byteArray) const {
    uint8_t base = *std::min_element(synopsis, synopsis + this->getNumberOfBuckets());

    const uint8_t maxValIn4Bits = ((1<<4)-1); // max value fitting 4 bits
    // we iterate over pairs of buckets

    uint8_t* __restrict__ synopsis_ = synopsis;
    const uint32_t numberOfBucketsConst = this->getNumberOfBuckets();

    // note: LOOP VECTORIZED
    for(uint32_t bgidx = 0; bgidx < numberOfBucketsConst/2; ++bgidx) {
      uint8_t normBucket1 = synopsis_[2*bgidx] - base;
      normBucket1 = (normBucket1 > maxValIn4Bits ? maxValIn4Bits : normBucket1);

      uint8_t normBucket2 = synopsis_[2*bgidx+1] - base;
      normBucket2 = (normBucket2 > maxValIn4Bits ? maxValIn4Bits : normBucket2);

      byteArray[bgidx] = (normBucket1 << 4) | normBucket2;
    }
    return base;
  }
};

#endif
