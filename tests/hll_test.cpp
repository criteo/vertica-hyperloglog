#include <cmath>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <stdint.h>


#include "gtest/gtest.h"
#include "Hll.hpp"

using namespace std;

namespace {


class HllTest : public ::testing::Test {
 protected:
  const std::string ids_filepath = "../data/ids.dat";
  std::ifstream data_file;

  HllTest() : data_file{ids_filepath, std::ifstream::in} {
  }

  virtual ~HllTest() {
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
  uint64_t operator()(uint64_t) const override {
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
  uint64_t operator()(uint32_t val) const override {
    uint64_t left_half  = ((static_cast<uint64_t>(val)*2654435761) % (1ULL << 32)) << 32;
    uint64_t right_half =  (static_cast<uint64_t>(val)*2654435761) % (1ULL << 32);

    return left_half | right_half;
  }
};


/**
 * This test checks serialization and deserialization in the dense format, i.e.
 * where 4 buckets are compressed to 3 bytes. An instance of Hll is serialized 
 * and then another instance of Hll is created and used as a target for
 * deserialization. The expected result is that both Hlls give the same
 * estimation and are bitwise equal.
 */
TEST_F(HllTest, TestSerializeDeserializeDense) {
  Hll<uint64_t> hll(14);

  for(uint64_t id; data_file >> id;) {
    hll.add(id);
  }
  uint32_t length = hll.getSynopsisSize(Format::DENSE);

  std::unique_ptr<char[]> byte_array(new char[length]);
  hll.serialize(byte_array.get(), Format::DENSE);
  /**
   * Maybe we modify this as the codebase matures, but for the time being
   * we expect his fixed length

   */
  const uint32_t ARRAY_LENGTH_8BYTES_BUCKETS_COMPRESSED = 12288U;
  EXPECT_EQ(length, ARRAY_LENGTH_8BYTES_BUCKETS_COMPRESSED);

  Hll<uint64_t> deserialized_hll(14);
  deserialized_hll.deserialize(byte_array.get(), Format::DENSE);

  EXPECT_EQ(hll.approximateCountDistinct(), deserialized_hll.approximateCountDistinct());
  EXPECT_TRUE( 0 == std::memcmp(hll.getCurrentSynopsis(), deserialized_hll.getCurrentSynopsis(), length));
}

/**
 * This test shares the logic with TestSerializeDeserializeDense.
 * However, it saves the data in a file and then reads it back in. 
 */
TEST_F(HllTest, TestSerializeDeserializeDenseToFile) {
  Hll<uint64_t> hll(14);

  for(uint64_t id; data_file >> id;) {
    hll.add(id);
  }

  uint32_t length = hll.getSynopsisSize(Format::DENSE);

  std::unique_ptr<char[]> byte_array(new char[length]);
  hll.serialize(byte_array.get(), Format::DENSE);
  std::ofstream temp_file_out("tmp", std::ios::binary | std::ios::out);
  temp_file_out.write(byte_array.get(), length);
  temp_file_out.close();

  std::ifstream temp_file_in("tmp", std::ios::binary | std::ios::in);
  std::unique_ptr<char[]> byte_array2(new char[length]);
  temp_file_in.read(byte_array2.get(), length);
  temp_file_in.close();
  unlink("tmp");


  Hll<uint64_t> deserialized_hll(14);
  deserialized_hll.deserialize(byte_array2.get(), Format::DENSE);

  EXPECT_EQ(hll.approximateCountDistinct(), deserialized_hll.approximateCountDistinct());
  EXPECT_TRUE( 0 == std::memcmp(hll.getCurrentSynopsis(), deserialized_hll.getCurrentSynopsis(), length));
}



/**
 * This test uses a non-trivial hash function, but different from murmur.
 * std::hash can't be used here, since in many implementations it's
 * not random or broken.
 */
TEST_F(HllTest, TestKnuthHash) {
  Hll<uint32_t, KnuthHash> hll(4);

  for(uint32_t id; data_file >> id;) {
    hll.add(id);
  }

  const uint32_t realCardinality = 632055;
  EXPECT_LT(hll.approximateCountDistinct(), 1.02*realCardinality);
  EXPECT_GT(hll.approximateCountDistinct(), 0.98*realCardinality);
}


TEST_F(HllTest, TestErrorWithinRangeForDifferentBucketMasks) {
  /**
   * We create a bunch of different HLL synopsis with bucket masks spreading
   * from 8 to 16. We store them together with the precision parameter
   * in order to be able to estimate the relative error later on.
   */
  std::vector<std::pair<uint8_t, Hll<uint64_t>>> hlls = { {6, {6}},
      {8,  {8} },
      {10, {10}},
      {12, {12}},
      {14, {14}},
      {16, {16}}
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
    Hll<uint64_t> hll = bits_hll.second;
    
    int64_t approximated_cardinality = static_cast<uint64_t>(hll.approximateCountDistinct());

    uint32_t m = 2 << basket_bits;
    /**
     * The paper says that the relative error is typically lower than 
     * 1.04/sqrt(m)
     */
    double expected_error = 1.04/std::sqrt(m);
    double real_error = std::abs(real_cardinality-approximated_cardinality)/real_cardinality;
    
    EXPECT_LT(real_error, expected_error*error_tolerance);
  }
}


/**
 * This test uses a non-standard hash function which yields 0x1 for every input
 * value. For such a hash function Hll should always return the same estimate,
 * no matter how many elements will be added.
 */
TEST_F(HllTest, TestDummyHashFunction) {
  Hll<uint64_t, DummyHash> hll(14);
  hll.add(static_cast<uint64_t>(0));
  auto singleElementDummyCount = hll.approximateCountDistinct();
  for(uint32_t id; data_file >> id;) {
    hll.add(id);
  }
  auto millionElementsDummyCount = hll.approximateCountDistinct();
  ASSERT_EQ(singleElementDummyCount, millionElementsDummyCount);
}


/**
 * This test whether the serialized synopses have expected lengths
 * NOTE: Length of a synopsis depends on the used precision and chosen format.
 */
TEST_F(HllTest, TestNonStandardSynopsisSize) {
  for(uint8_t precision=4; precision<=18; ++precision) {
    Hll<uint64_t> hll(precision);
    ASSERT_EQ(hll.getSynopsisSize(Format::SPARSE), 1<<precision);
    // dense format is expected to go down by 3/4
    // since 8 bits become 6
    ASSERT_EQ(hll.getSynopsisSize(Format::DENSE), (1<<precision)*3/4);
  }
}


/**
 * This tests splits the input data set into two halves. The first half is added
 * to one Hll, the second half to another Hll. At the same time all the elements
 * are added to a third Hll. Assumption is that if we add to halfs of the input
 * data, we should get the same estimate as for having all the elements stored in
 * one Hll.
 */
TEST_F(HllTest, TestSynopsisAdditionAssociativity) {
  Hll<uint64_t> hll1(14), hll2(14), hll1plus2(14);
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
  ASSERT_EQ(hll1.approximateCountDistinct(), hll1plus2.approximateCountDistinct());
}


/**
 * This test uses a hash function accepting 32 bit values. The target values are
 * 64 bits long, but this should work as well. 
 */
TEST_F(HllTest, Test32BitHash) {
  Hll<uint32_t> hll(14);
  for(uint32_t id; data_file >> id;) {
    hll.add(id);
  }
  const uint32_t realCardinality = 632055;
  EXPECT_LT(hll.approximateCountDistinct(), 1.01*realCardinality);
  EXPECT_GT(hll.approximateCountDistinct(), 0.99*realCardinality);
}


TEST_F(HllTest, Test32BitIds) {
  Hll<uint32_t> hll(14);
  for(uint32_t id; data_file >> id;) {
    hll.add(id);
  }
  const uint32_t realCardinality = 632055;
  EXPECT_LT(hll.approximateCountDistinct(), 1.01*realCardinality);
  EXPECT_GT(hll.approximateCountDistinct(), 0.99*realCardinality);
}


/**
 * Since the estimate is proportional to a harmonic mean of bucketed values
 * growing monotonically, adding new elements should never make the estimate
 * lower.
 */
TEST_F(HllTest, TestEstimateGrowsMonotonically) {
  Hll<uint64_t> hll(14);

  uint32_t counter = 0;
  uint64_t prevEstimate = 0;
  for(uint32_t id; data_file >> id;) {
    hll.add(id);
    ++counter;
    if(counter % 100000 == 0) { // check how the estimate behaves every 100k elements
      uint64_t curEstimate = hll.approximateCountDistinct();
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
TEST_F(HllTest, TestAlreadySeenItemsDontChangeEstimate) {
  Hll<uint64_t> hll(14);

  for(uint32_t id; data_file >> id;) {
    hll.add(id);
  }

  auto firstEstimate = hll.approximateCountDistinct();

  data_file.seekg(0); // go back to the beginning
  data_file.ignore(256, '\n');

  for(uint32_t id; data_file >> id;) {
    hll.add(id);
  }

  EXPECT_EQ(firstEstimate, hll.approximateCountDistinct());
}


/**
 * Test whether an estimate for 1M numbers with cardinality=630k will fall
 * within 1% from the real value.
 */
TEST_F(HllTest, TestBigInput) {
  Hll<uint64_t> hll(14);

  for(uint32_t id; data_file >> id;) {
    hll.add(id);
  }

  const uint32_t realCardinality = 632055;
  EXPECT_LT(hll.approximateCountDistinct(), 1.01*realCardinality);
  EXPECT_GT(hll.approximateCountDistinct(), 0.99*realCardinality);
}


TEST_F(HllTest, TestSmallInput) {
  Hll<uint64_t> hll(14);

  size_t i=0;
  for(uint32_t id; data_file >> id && i < 100000; ++i) {
    hll.add(id);
  }

  const uint32_t realCardinality = 95245;
  EXPECT_LT(hll.approximateCountDistinct(), 1.01*realCardinality);
  EXPECT_GT(hll.approximateCountDistinct(), 0.99*realCardinality);
}

}  // namespace

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
