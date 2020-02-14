#include <cmath>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <stdint.h>

#include "../base_test.hpp"
#include "gtest/gtest.h"
#include "hll-criteo/hll.hpp"

using namespace std;

namespace {


class HllRawTest : public HllBaseTest {
 protected:
  std::ifstream data_file;

  HllRawTest() : HllBaseTest("ids.dat"),
    data_file{getInputPath(), std::ifstream::in} {}

  virtual ~HllRawTest() {
    // You can do clean-up work that doesn't throw exceptions here.
  }

  virtual void SetUp() {
    data_file.seekg(0, data_file.beg);
    // ignore the first line of the data file; it contains a comment
    data_file.ignore(256, '\n');
  }

  virtual void TearDown() {
    data_file.close();
  }

};

class DummyHash : public Hash<uint64_t> {
public:
  uint64_t operator()(uint64_t, uint32_t) const override {
    return 0x1;
  }
};

/**
 * Multiplicative hash function proposed by Donald Knuth. Looks weird, but
 * actually works. It's anyway just to test that HLL works with a hash function
 * different from murmur.
 */
class KnuthHash : public Hash<uint32_t> {
public:
  uint64_t operator()(uint32_t val, uint32_t seed) const override {
    uint64_t left_half  = ((static_cast<uint64_t>(val)*2654435761) % (1ULL << 32)) << 32;
    uint64_t right_half =  (static_cast<uint64_t>(val)*2654435761) % (1ULL << 32);

    return left_half | right_half;
  }
};


TEST_F(HllRawTest, TestEstimateIsCorrectForBigBuckets) {
  const uint8_t PRECISION = 12;
  SizedBuffer buffer = Hll<uint64_t>::makeDeserializedBuffer(PRECISION); // sizeof(HLLHdr) + synopsis
  SizedBuffer buffer2 = Hll<uint64_t>::makeDeserializedBuffer(PRECISION);

  HllRaw<uint64_t> hll(PRECISION, buffer.first.get());
  HllRaw<uint64_t> hll2(PRECISION, buffer2.first.get());

  //number of buckets is always divisible by 4
  for(uint32_t i=0; i<hll.getNumberOfBuckets(); i+=4) {
    hll.setBucketValue(i, 4);
    hll.setBucketValue(i+1, 5);
    hll.setBucketValue(i+2, 6);
    hll.setBucketValue(i+3, 7);

    hll2.setBucketValue(i, 4);
    hll2.setBucketValue(i+1, 5);
    hll2.setBucketValue(i+2, 6);
    hll2.setBucketValue(i+3, 7);
  }
  // 32 would overflow unsigned int.
  // This was the problem with Opera data.
  hll2.setBucketValue(0, 32);
  EXPECT_GE(hll2.estimate(), hll.estimate());
}

/**
 * This test checks serialization and deserialization in the 6-bits format, i.e.
 * where 4 buckets are compressed to 3 bytes. An instance of HllRaw is serialized
 * and then another instance of HllRaw is created and used as a target for
 * deserialization. The expected result is that both HllRaws give the same
 * estimation and are bitwise equal.
 */
TEST_F(HllRawTest, TestSerializeDeserialize6Bits) {
  const uint8_t PRECISION = 14;
  SizedBuffer buffer = Hll<uint64_t>::makeDeserializedBuffer(PRECISION); // sizeof(HLLHdr) + synopsis
  HllRaw<uint64_t> hll(PRECISION, buffer.first.get());

  for(uint64_t id; data_file >> id;) {
    hll.add(id);
  }
  SizedBuffer byte_array = Hll<uint64_t>::makeSerializedBuffer(Format::COMPACT_6BITS, PRECISION);
  hll.serialize6Bits(byte_array.first.get());
  /**
   * Maybe we modify this as the codebase matures, but for the time being
   * we expect his fixed length

   */
  const uint32_t ARRAY_LENGTH_8BYTES_BUCKETS_COMPRESSED_WITH_HDR = 12288U + sizeof(HLLHdr);
  EXPECT_EQ(byte_array.second, ARRAY_LENGTH_8BYTES_BUCKETS_COMPRESSED_WITH_HDR);

  SizedBuffer buffer2 = Hll<uint64_t>::makeDeserializedBuffer(PRECISION); // sizeof(HLLHdr) + synopsis
  HllRaw<uint64_t> deserialized_hll(PRECISION, buffer2.first.get());
  deserialized_hll.fold6Bits(byte_array.first.get(), buffer2.second);

  EXPECT_EQ(hll.estimate(), deserialized_hll.estimate());
  EXPECT_TRUE( 0 == std::memcmp(hll.getCurrentSynopsis(), deserialized_hll.getCurrentSynopsis(), byte_array.second));
}

/**
 * This test shares the logic with the one above.
 * However, it saves the data in a file and then reads it back in.
 */
TEST_F(HllRawTest, TestSerializeDeserialize6BitsToFile) {
  const uint8_t PRECISION = 14;
  SizedBuffer buffer = Hll<uint64_t>::makeDeserializedBuffer(PRECISION); // sizeof(HLLHdr) + synopsis
  HllRaw<uint64_t> hll(PRECISION, buffer.first.get());

  for(uint64_t id; data_file >> id;) {
    hll.add(id);
  }

  SizedBuffer byte_array = Hll<uint64_t>::makeSerializedBuffer(Format::COMPACT_6BITS, PRECISION);
  hll.serialize6Bits(byte_array.first.get());
  std::ofstream temp_file_out("tmp", std::ios::binary | std::ios::out);
  temp_file_out.write(reinterpret_cast<const char*>(byte_array.first.get()), byte_array.second);
  temp_file_out.close();

  std::ifstream temp_file_in("tmp", std::ios::binary | std::ios::in);
  SizedBuffer byte_array2 = Hll<uint64_t>::makeSerializedBuffer(Format::COMPACT_6BITS, PRECISION);
  temp_file_in.read(reinterpret_cast<char*>(byte_array2.first.get()), byte_array2.second);
  temp_file_in.close();
  unlink("tmp");


  SizedBuffer buffer2 = Hll<uint64_t>::makeDeserializedBuffer(PRECISION); // sizeof(HLLHdr) + synopsis
  HllRaw<uint64_t> deserialized_hll(PRECISION, buffer2.first.get());
  deserialized_hll.fold6Bits(byte_array.first.get(), buffer2.second);

  EXPECT_EQ(hll.estimate(), deserialized_hll.estimate());
  EXPECT_TRUE( 0 == std::memcmp(hll.getCurrentSynopsis(), deserialized_hll.getCurrentSynopsis(), byte_array.second));
}





/**
 * This test uses a non-trivial hash function, but different from murmur.
 * std::hash can't be used here, since in many implementations it's
 * not random or broken.
 */
TEST_F(HllRawTest, TestKnuthHash) {
  const uint8_t PRECISION = 4;
  SizedBuffer buffer = Hll<uint64_t>::makeDeserializedBuffer(PRECISION); // sizeof(HLLHdr) + synopsis
  HllRaw<uint32_t, KnuthHash> hll(PRECISION, buffer.first.get());

  for(uint32_t id; data_file >> id;) {
    hll.add(id);
  }

  const uint32_t realCardinality = 632055;
  EXPECT_LT(hll.estimate(), 1.02*realCardinality);
  EXPECT_GT(hll.estimate(), 0.98*realCardinality);
}


TEST_F(HllRawTest, TestErrorWithinRangeForDifferentBucketMasks) {
  /**
   * We create a bunch of different HLL synopsis with bucket masks spreading
   * from 8 to 16. We store them together with the precision parameter
   * in order to be able to estimate the relative error later on.
   */
  SizedBuffer buffer6 = Hll<uint64_t>::makeDeserializedBuffer(6); // sizeof(HLLHdr) + synopsis
  SizedBuffer buffer8 = Hll<uint64_t>::makeDeserializedBuffer(8); // sizeof(HLLHdr) + synopsis
  SizedBuffer buffer10 = Hll<uint64_t>::makeDeserializedBuffer(10); // sizeof(HLLHdr) + synopsis
  SizedBuffer buffer12 = Hll<uint64_t>::makeDeserializedBuffer(12); // sizeof(HLLHdr) + synopsis
  SizedBuffer buffer14 = Hll<uint64_t>::makeDeserializedBuffer(14); // sizeof(HLLHdr) + synopsis
  SizedBuffer buffer16 = Hll<uint64_t>::makeDeserializedBuffer(16); // sizeof(HLLHdr) + synopsis

  std::vector<std::pair<uint8_t, HllRaw<uint64_t>>> hlls = {
      {6, {6, buffer6.first.get()}},
      {8,  {8, buffer8.first.get()} },
      {10, {10, buffer10.first.get()}},
      {12, {12, buffer12.first.get()}},
      {14, {14, buffer14.first.get()}},
      {16, {16, buffer16.first.get()}}
    };

  for(uint64_t id; data_file >> id;) {
    for(auto& bits_hll: hlls)
      bits_hll.second.add(id);
  }


  const uint64_t real_cardinality = 632055;
  /**
   * TODO: For the time being this is just a magic value out of my hat,
   *       but once a better error correction method is implemented
   *       we can adjust it to better fit the estimate.
   */
  const double error_tolerance = 1.5;

  for(auto& bits_hll: hlls) {
    uint8_t basket_bits = bits_hll.first;
    HllRaw<uint64_t> hll = bits_hll.second;

    int64_t approximated_cardinality = static_cast<uint64_t>(hll.estimate());

    uint32_t m = 2 << basket_bits;
    /**
     * The paper says that the relative error is typically lower than
     * 1.04/sqrt(m)
     */
    double expected_error = 1.04/std::sqrt(m);
    double real_error = std::fabs(real_cardinality-approximated_cardinality)/real_cardinality;

    EXPECT_LT(real_error, expected_error*error_tolerance);
  }
}


/**
 * This test uses a non-standard hash function which yields 0x1 for every input
 * value. For such a hash function HllRaw should always return the same estimate,
 * no matter how many elements will be added.
 */
TEST_F(HllRawTest, TestDummyHashFunction) {
  const uint8_t PRECISION = 14;
  SizedBuffer buffer = Hll<uint64_t>::makeDeserializedBuffer(PRECISION); // sizeof(HLLHdr) + synopsis
  HllRaw<uint64_t, DummyHash> hll(PRECISION, buffer.first.get());
  hll.add(static_cast<uint64_t>(0));
  auto singleElementDummyCount = hll.estimate();
  for(uint32_t id; data_file >> id;) {
    hll.add(id);
  }
  auto millionElementsDummyCount = hll.estimate();
  ASSERT_EQ(singleElementDummyCount, millionElementsDummyCount);
}


/**
 * This test whether the serialized synopses have expected lengths
 * NOTE: Length of a synopsis depends on the used precision and chosen format.
 */
TEST_F(HllRawTest, TestNonStandardSynopsisSize) {
  for(uint8_t precision=4; precision<=18; ++precision) {
    SizedBuffer buffer = Hll<uint64_t>::makeDeserializedBuffer(precision); // sizeof(HLLHdr) + synopsis
    HllRaw<uint64_t> hll(precision, buffer.first.get());
    ASSERT_EQ(hll.getSerializedSynopsisSize(Format::NORMAL), 1<<precision);
    // 6 bit format is expected to go down by 3/4
    // since 8 bits become 6
    ASSERT_EQ(hll.getSerializedSynopsisSize(Format::COMPACT_6BITS), (1<<precision)*3/4);
  }
}


/**
 * This tests splits the input data set into two halves. The first half is added
 * to one HllRaw, the second half to another HllRaw. At the same time all the elements
 * are added to a third HllRaw. Assumption is that if we add to halfs of the input
 * data, we should get the same estimate as for having all the elements stored in
 * one HllRaw.
 */
TEST_F(HllRawTest, TestSynopsisAdditionAssociativity) {
  const uint8_t PRECISION = 14;
  SizedBuffer buffer1 = Hll<uint64_t>::makeDeserializedBuffer(PRECISION); // sizeof(HLLHdr) + synopsis
  SizedBuffer buffer2 = Hll<uint64_t>::makeDeserializedBuffer(PRECISION); // sizeof(HLLHdr) + synopsis
  SizedBuffer buffer1p2 = Hll<uint64_t>::makeDeserializedBuffer(PRECISION); // sizeof(HLLHdr) + synopsis


  HllRaw<uint64_t> hll1(PRECISION, buffer1.first.get()), hll2(PRECISION, buffer2.first.get()), hll1plus2(PRECISION, buffer1p2.first.get());
  std::vector<uint32_t> ids;
  ids.reserve(1000000);
  for(uint32_t id; data_file >> id;) {
    ids.push_back(id);
  }
  size_t i=0;
  for(; i<500000; ++i) {
    hll1.add(ids[i]);
    hll1plus2.add(ids[i]);
  }
  for(;i<1000000; ++i) {
    hll2.add(ids[i]);
    hll1plus2.add(ids[i]);
  }

  hll1.add(hll2);
  ASSERT_EQ(hll1.estimate(), hll1plus2.estimate());
}


/**
 * This test uses a hash function accepting 32 bit values. The target values are
 * 64 bits long, but this should work as well.
 */
TEST_F(HllRawTest, Test32BitHash) {
  const uint8_t PRECISION = 14;
  SizedBuffer buffer = Hll<uint64_t>::makeDeserializedBuffer(PRECISION); // sizeof(HLLHdr) + synopsis
  HllRaw<uint32_t> hll(PRECISION, buffer.first.get());
  for(uint32_t id; data_file >> id;) {
    hll.add(id);
  }
  const uint32_t realCardinality = 632055;
  EXPECT_LT(hll.estimate(), 1.01*realCardinality);
  EXPECT_GT(hll.estimate(), 0.99*realCardinality);
}


TEST_F(HllRawTest, Test32BitIds) {
  const uint8_t PRECISION = 14;
  SizedBuffer buffer = Hll<uint64_t>::makeDeserializedBuffer(PRECISION); // sizeof(HLLHdr) + synopsis
  HllRaw<uint32_t> hll(PRECISION, buffer.first.get());
  for(uint32_t id; data_file >> id;) {
    hll.add(id);
  }
  const uint32_t realCardinality = 632055;
  EXPECT_LT(hll.estimate(), 1.01*realCardinality);
  EXPECT_GT(hll.estimate(), 0.99*realCardinality);
}


/**
 * Since the estimate is proportional to a harmonic mean of bucketed values
 * growing monotonically, adding new elements should never make the estimate
 * lower.
 */
TEST_F(HllRawTest, TestEstimateGrowsMonotonically) {
  const uint8_t PRECISION = 14;
  SizedBuffer buffer = Hll<uint64_t>::makeDeserializedBuffer(PRECISION); // sizeof(HLLHdr) + synopsis
  HllRaw<uint32_t> hll(PRECISION, buffer.first.get());

  uint32_t counter = 0;
  uint64_t prevEstimate = 0;
  for(uint32_t id; data_file >> id;) {
    hll.add(id);
    ++counter;
    if(counter % 100000 == 0) { // check how the estimate behaves every 100k elements
      uint64_t curEstimate = hll.estimate();
      EXPECT_GE(curEstimate, prevEstimate);
      prevEstimate = curEstimate;
    }
  }

}


/**
 * Adding items already seen should never decrease the estimate. Since they
 * were already hashed and inserted into corresponding bucket, this bucket
 * either contains the same value or a larger one. If it's larger, adding
 * already seen value will not change it.
 */
TEST_F(HllRawTest, TestAlreadySeenItemsDontChangeEstimate) {
  const uint8_t PRECISION = 14;
  SizedBuffer buffer = Hll<uint64_t>::makeDeserializedBuffer(PRECISION); // sizeof(HLLHdr) + synopsis
  HllRaw<uint32_t> hll(PRECISION, buffer.first.get());

  for(uint32_t id; data_file >> id;) {
    hll.add(id);
  }

  auto firstEstimate = hll.estimate();

  data_file.seekg(0); // go back to the beginning
  data_file.ignore(256, '\n');

  for(uint32_t id; data_file >> id;) {
    hll.add(id);
  }

  EXPECT_EQ(firstEstimate, hll.estimate());
}


/**
 * Test whether an estimate for 1M numbers with cardinality=630k will fall
 * within 1% from the real value.
 */
TEST_F(HllRawTest, TestBigInput) {
  const uint8_t PRECISION = 14;
  SizedBuffer buffer = Hll<uint64_t>::makeDeserializedBuffer(PRECISION); // sizeof(HLLHdr) + synopsis
  HllRaw<uint32_t> hll(PRECISION, buffer.first.get());

  for(uint32_t id; data_file >> id;) {
    hll.add(id);
  }

  const uint32_t realCardinality = 632055;
  EXPECT_LT(hll.estimate(), 1.01*realCardinality);
  EXPECT_GT(hll.estimate(), 0.99*realCardinality);
}


TEST_F(HllRawTest, TestSmallInput) {
  const uint8_t PRECISION = 14;
  SizedBuffer buffer = Hll<uint64_t>::makeDeserializedBuffer(PRECISION); // sizeof(HLLHdr) + synopsis
  HllRaw<uint32_t> hll(PRECISION, buffer.first.get());

  size_t i=0;
  for(uint32_t id; data_file >> id && i < 100000; ++i) {
    hll.add(id);
  }

  const uint32_t realCardinality = 95245;
  EXPECT_LT(hll.estimate(), 1.01*realCardinality);
  EXPECT_GT(hll.estimate(), 0.99*realCardinality);
}

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
