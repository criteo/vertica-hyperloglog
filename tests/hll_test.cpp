#include <cmath>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <stdint.h>


#include "gtest/gtest.h"
#include "hll.hpp"

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

/**
 * What we want to test is basically that the HLLHdr
 * get serialized as well. If so, the synopsis size will
 * be 8 bytes longer than for HllRaw
 */
TEST_F(HllTest, TestSerializeDeserialize6Bits) {
  Hll<uint64_t> hll(14);

  for(uint64_t id; data_file >> id;) {
    hll.add(id);
  }
  uint32_t length = hll.getSynopsisSize(Format::COMPACT);

  std::unique_ptr<char[]> byte_array(new char[length]);
  hll.serialize(byte_array.get(), Format::COMPACT);
  /**
   * Maybe we modify this as the codebase matures, but for the time being
   * we expect his fixed length

   */
  const uint32_t ARRAY_LENGTH_8BYTES_BUCKETS_COMPRESSED_WITH_HDR = 12288U + 8;
  EXPECT_EQ(length, ARRAY_LENGTH_8BYTES_BUCKETS_COMPRESSED_WITH_HDR);

  Hll<uint64_t> deserialized_hll(14);
  deserialized_hll.deserialize(byte_array.get(), Format::COMPACT);

  EXPECT_EQ(hll.approximateCountDistinct(), deserialized_hll.approximateCountDistinct());
}

/**
 * 
 */
TEST_F(HllTest, TestSerializeDeserialize5Bits) {
  Hll<uint64_t> hll(14);

  for(uint64_t id; data_file >> id;) {
    hll.add(id);
  }
  uint32_t length = hll.getSynopsisSize(Format::COMPACT_BASE);

  std::unique_ptr<char[]> byte_array(new char[length]);
  hll.serialize(byte_array.get(), Format::COMPACT_BASE);
  /**
   * Maybe we modify this as the codebase matures, but for the time being
   * we expect his fixed length

   */
  const uint32_t ARRAY_LENGTH_5BYTES_BUCKETS_COMPRESSED_WITH_HDR = ((1<<14)*5/8)+8;
  EXPECT_EQ(length, ARRAY_LENGTH_5BYTES_BUCKETS_COMPRESSED_WITH_HDR);

  Hll<uint64_t> deserialized_hll(14);
  deserialized_hll.deserialize(byte_array.get(), Format::COMPACT_BASE);

  EXPECT_EQ(hll.approximateCountDistinct(), deserialized_hll.approximateCountDistinct());
}

/**
 * This test shares the logic with the test above.
 * However, it saves the data in a file and then reads it back in. 
 */
TEST_F(HllTest, TestSerializeDeserialize5BitsToFile) {
  Hll<uint64_t> hll(13);

  for(uint64_t id; data_file >> id;) {
    hll.add(id);
  }

  uint32_t length = hll.getSynopsisSize(Format::COMPACT_BASE);
  std::unique_ptr<char[]> byte_array(new char[length]);
  hll.serialize(byte_array.get(), Format::COMPACT_BASE);
  std::ofstream temp_file_out("/tmp/tmp1", std::ios::binary | std::ios::out);
  temp_file_out.write(byte_array.get(), length);
  temp_file_out.close();

  std::ifstream temp_file_in("/tmp/tmp1", std::ios::binary | std::ios::in);
  std::unique_ptr<char[]> byte_array2(new char[length]);
  temp_file_in.read(byte_array2.get(), length);
  temp_file_in.close();
  unlink("tmp");


  Hll<uint64_t> deserialized_hll(13);
  deserialized_hll.deserialize(byte_array2.get(), Format::COMPACT_BASE);

  EXPECT_EQ(hll.approximateCountDistinct(), deserialized_hll.approximateCountDistinct());
}


} // namespace