#include <cmath>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <stdint.h>

#include "base_test.hpp"
#include "gtest/gtest.h"
#include "hll.hpp"

using namespace std;

namespace {


class HllTest : public HllBaseTest {
 protected:
  std::ifstream data_file;

  HllTest() : HllBaseTest("ids.dat"),
    data_file{getInputPath(), std::ifstream::in} {}

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

/**
 *                **************
 *                *** 6 BITS ***
 *                **************
 *
 * What we want to test is basically that the HLLHdr
 * get serialized as well. If so, the synopsis size will
 * be 8 bytes longer than for HllRaw
 */
TEST_F(HllTest, TestSerializeDeserialize6Bits) {
  std::vector<uint64_t> ids;
  for(uint64_t id; data_file >> id;) {
    ids.push_back(id);
  }

  for(uint8_t prec = 10; prec <= 18; ++prec) {
    Hll<uint64_t> hll(prec);

    for(uint64_t id: ids) {
      hll.add(id);
    }
    uint32_t length = hll.getSynopsisSize(Format::COMPACT_6BITS);

    std::unique_ptr<char[]> byte_array(new char[length]);
    hll.serialize(byte_array.get(), Format::COMPACT_6BITS);
    /**
     * Maybe we modify this as the codebase matures, but for the time being
     * we expect his fixed length

     */
    const uint32_t ARRAY_LENGTH_6BYTES_BUCKETS_COMPRESSED_WITH_HDR = ((1<<prec)*6/8)+8;
    EXPECT_EQ(length, ARRAY_LENGTH_6BYTES_BUCKETS_COMPRESSED_WITH_HDR);

    Hll<uint64_t> deserialized_hll(prec);
    deserialized_hll.deserialize(byte_array.get(), Format::COMPACT_6BITS);
    EXPECT_EQ(hll.approximateCountDistinct(), deserialized_hll.approximateCountDistinct());

    Hll<uint64_t> folded_hll(prec);
    folded_hll.fold(reinterpret_cast<const uint8_t*>(byte_array.get()), length);
    EXPECT_EQ(hll.approximateCountDistinct(), folded_hll.approximateCountDistinct());
  }
}

/**
 *                **************
 *                *** 5 BITS ***
 *                **************
 */
TEST_F(HllTest, TestSerializeDeserialize5Bits) {
  std::vector<uint64_t> ids;
  for(uint64_t id; data_file >> id;) {
    ids.push_back(id);
  }

  for(uint8_t prec = 10; prec <= 18; ++prec) {
    Hll<uint64_t> hll(prec);

    for(uint64_t id: ids) {
      hll.add(id);
    }
    uint32_t length = hll.getSynopsisSize(Format::COMPACT_5BITS);

    std::unique_ptr<char[]> byte_array(new char[length]);
    hll.serialize(byte_array.get(), Format::COMPACT_5BITS);
    /**
     * Maybe we modify this as the codebase matures, but for the time being
     * we expect his fixed length

     */
    const uint32_t ARRAY_LENGTH_5BYTES_BUCKETS_COMPRESSED_WITH_HDR = ((1<<prec)*5/8)+8;
    EXPECT_EQ(length, ARRAY_LENGTH_5BYTES_BUCKETS_COMPRESSED_WITH_HDR);

    Hll<uint64_t> deserialized_hll(prec);
    deserialized_hll.deserialize(byte_array.get(), Format::COMPACT_5BITS);

    EXPECT_EQ(hll.approximateCountDistinct(), deserialized_hll.approximateCountDistinct());

    Hll<uint64_t> folded_hll(prec);
    folded_hll.fold(reinterpret_cast<const uint8_t*>(byte_array.get()), length);
    EXPECT_EQ(hll.approximateCountDistinct(), folded_hll.approximateCountDistinct());
  }
}

/**
 *                **************
 *                *** 5 BITS ***
 *                **************
 *
 * This test shares the logic with the test above.
 * However, it saves the data in a file and then reads it back in.
 */
TEST_F(HllTest, TestSerializeDeserialize5BitsToFile) {
  std::vector<uint64_t> ids;
  for(uint64_t id; data_file >> id;) {
    ids.push_back(id);
  }

  for(uint8_t prec = 10; prec <= 18; ++prec) {
    Hll<uint64_t> hll(prec);

    for(uint64_t id: ids) {
      hll.add(id);
    }

    uint32_t length = hll.getSynopsisSize(Format::COMPACT_5BITS);
    std::unique_ptr<char[]> byte_array(new char[length]);
    hll.serialize(byte_array.get(), Format::COMPACT_5BITS);
    std::ofstream temp_file_out("/tmp/tmp1", std::ios::binary | std::ios::out);
    temp_file_out.write(byte_array.get(), length);
    temp_file_out.close();

    std::ifstream temp_file_in("/tmp/tmp1", std::ios::binary | std::ios::in);
    std::unique_ptr<char[]> byte_array2(new char[length]);
    temp_file_in.read(byte_array2.get(), length);
    temp_file_in.close();
    unlink("tmp");

    Hll<uint64_t> deserialized_hll(prec);
    deserialized_hll.deserialize(byte_array2.get(), Format::COMPACT_5BITS);

    EXPECT_EQ(hll.approximateCountDistinct(), deserialized_hll.approximateCountDistinct());

    Hll<uint64_t> folded_hll(prec);
    folded_hll.fold(reinterpret_cast<const uint8_t*>(byte_array2.get()), length);
    EXPECT_EQ(hll.approximateCountDistinct(), folded_hll.approximateCountDistinct());
  }
}


/**
 *                **************
 *                *** 4 BITS ***
 *                **************
 */
TEST_F(HllTest, TestSerializeDeserialize4BitsToFile) {
  std::vector<uint64_t> ids;
  for(uint64_t id; data_file >> id;) {
    ids.push_back(id);
  }

  for(uint8_t prec = 10; prec <= 18; ++prec) {
    Hll<uint64_t> hll(prec);

    for(uint64_t id: ids) {
      hll.add(id);
    }

    uint32_t length = hll.getSynopsisSize(Format::COMPACT_4BITS);
    std::unique_ptr<char[]> byte_array(new char[length]);
    hll.serialize(byte_array.get(), Format::COMPACT_4BITS);
    std::ofstream temp_file_out("tmp1", std::ios::binary | std::ios::out);
    temp_file_out.write(byte_array.get(), length);
    temp_file_out.close();

    std::ifstream temp_file_in("tmp1", std::ios::binary | std::ios::in);
    std::unique_ptr<char[]> byte_array2(new char[length]);
    temp_file_in.read(byte_array2.get(), length);
    temp_file_in.close();
    unlink("tmp1");

    Hll<uint64_t> deserialized_hll(prec);
    deserialized_hll.deserialize(byte_array2.get(), Format::COMPACT_4BITS);

    /**
     * With 4 bits we really experience bucket clipping.
     * To somehow take this this into account, we expect the ser/deserialized estimate
     * to be off my 10 from the original one.
     */
    EXPECT_LT(hll.approximateCountDistinct(), deserialized_hll.approximateCountDistinct()+10);
    EXPECT_GT(hll.approximateCountDistinct(), deserialized_hll.approximateCountDistinct()-10);

    Hll<uint64_t> folded_hll(prec);
    folded_hll.fold(reinterpret_cast<const uint8_t*>(byte_array2.get()), length);
    EXPECT_LT(hll.approximateCountDistinct(), folded_hll.approximateCountDistinct()+10);
    EXPECT_GT(hll.approximateCountDistinct(), folded_hll.approximateCountDistinct()-10);

  }
}


TEST_F(HllTest, TestWrongParametersSerializationThrowsError) {
  Hll<uint64_t> hll(14);

  for(uint64_t id; data_file >> id;) {
    hll.add(id);
  }

  uint32_t length = hll.getSynopsisSize(Format::COMPACT_6BITS);
  std::unique_ptr<char[]> byte_array(new char[length]);
  hll.serialize(byte_array.get(), Format::COMPACT_6BITS);

  Hll<uint64_t> deserialized_hll(14);
  // NOTE: serialization was done using 6 bits, here we use 5
  // this is an incorrect usage and should throw an erro
  ASSERT_THROW(deserialized_hll.deserialize(byte_array.get(), Format::COMPACT_5BITS), SerializationError);

  // this usage is legit, as the synopsis was serialized using 6 bits per bucket
  ASSERT_NO_THROW(deserialized_hll.deserialize(byte_array.get(), Format::COMPACT_6BITS));
}

} // namespace
