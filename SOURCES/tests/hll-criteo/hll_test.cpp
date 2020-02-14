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
    SizedBuffer buffer = Hll<uint64_t>::makeDeserializedBuffer(prec);
    Hll<uint64_t> hll(prec, buffer.first.get());

    for(uint64_t id: ids) {
      hll.add(id);
    }
    SizedBuffer byte_array = Hll<uint64_t>::makeSerializedBuffer(Format::COMPACT_6BITS, prec);
    hll.serialize(byte_array.first.get(), Format::COMPACT_6BITS);
    /**
     * Maybe we modify this as the codebase matures, but for the time being
     * we expect his fixed length

     */
    const uint32_t ARRAY_LENGTH_6BYTES_BUCKETS_COMPRESSED_WITH_HDR = ((1<<prec)*6/8)+8;
    EXPECT_EQ(byte_array.second, ARRAY_LENGTH_6BYTES_BUCKETS_COMPRESSED_WITH_HDR);

    SizedBuffer bufferFolded = Hll<uint64_t>::makeDeserializedBuffer(prec);
    Hll<uint64_t> folded_hll(prec, bufferFolded.first.get());
    folded_hll.fold(byte_array.first.get(), byte_array.second);
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
    SizedBuffer buffer = Hll<uint64_t>::makeDeserializedBuffer(prec);
    Hll<uint64_t> hll(prec, buffer.first.get());

    for(uint64_t id: ids) {
      hll.add(id);
    }
    SizedBuffer byte_array = Hll<uint64_t>::makeSerializedBuffer(Format::COMPACT_5BITS, prec);
    hll.serialize(byte_array.first.get(), Format::COMPACT_5BITS);
    /**
     * Maybe we modify this as the codebase matures, but for the time being
     * we expect his fixed length

     */
    const uint32_t ARRAY_LENGTH_5BYTES_BUCKETS_COMPRESSED_WITH_HDR = ((1<<prec)*5/8)+8;
    EXPECT_EQ(byte_array.second, ARRAY_LENGTH_5BYTES_BUCKETS_COMPRESSED_WITH_HDR);

    SizedBuffer bufferFolded = Hll<uint64_t>::makeDeserializedBuffer(prec);
    Hll<uint64_t> folded_hll(prec, bufferFolded.first.get());
    folded_hll.fold(byte_array.first.get(), byte_array.second);
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
    SizedBuffer buffer = Hll<uint64_t>::makeDeserializedBuffer(prec);
    Hll<uint64_t> hll(prec, buffer.first.get());

    for(uint64_t id: ids) {
      hll.add(id);
    }

    SizedBuffer byte_array = Hll<uint64_t>::makeSerializedBuffer(Format::COMPACT_5BITS, prec);
    hll.serialize(byte_array.first.get(), Format::COMPACT_5BITS);
    std::ofstream temp_file_out("/tmp/tmp1", std::ios::binary | std::ios::out);
    temp_file_out.write(reinterpret_cast<const char*>(byte_array.first.get()), byte_array.second);
    temp_file_out.close();

    std::ifstream temp_file_in("/tmp/tmp1", std::ios::binary | std::ios::in);
    SizedBuffer byte_array2 = Hll<uint64_t>::makeSerializedBuffer(Format::COMPACT_5BITS, prec);
    temp_file_in.read(reinterpret_cast<char*>(byte_array2.first.get()), byte_array2.second);
    temp_file_in.close();
    unlink("tmp");

    SizedBuffer bufferFolded = Hll<uint64_t>::makeDeserializedBuffer(prec);
    Hll<uint64_t> folded_hll(prec, bufferFolded.first.get());
    folded_hll.fold(byte_array2.first.get(), byte_array2.second);
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
    SizedBuffer buffer = Hll<uint64_t>::makeDeserializedBuffer(prec);
    Hll<uint64_t> hll(prec, buffer.first.get());

    for(uint64_t id: ids) {
      hll.add(id);
    }

    SizedBuffer byte_array = Hll<uint64_t>::makeSerializedBuffer(Format::COMPACT_4BITS, prec);
    hll.serialize(byte_array.first.get(), Format::COMPACT_4BITS);
    std::ofstream temp_file_out("tmp1", std::ios::binary | std::ios::out);
    temp_file_out.write(reinterpret_cast<const char*>(byte_array.first.get()), byte_array.second);
    temp_file_out.close();

    std::ifstream temp_file_in("tmp1", std::ios::binary | std::ios::in);;
    SizedBuffer byte_array2 = Hll<uint64_t>::makeSerializedBuffer(Format::COMPACT_4BITS, prec);
    temp_file_in.read(reinterpret_cast<char*>(byte_array2.first.get()), byte_array2.second);
    temp_file_in.close();
    unlink("tmp1");

    /**
     * With 4 bits we really experience bucket clipping.
     * To somehow take this this into account, we expect the ser/deserialized estimate
     * to be off my 10 from the original one.
     */
    SizedBuffer bufferFolded = Hll<uint64_t>::makeDeserializedBuffer(prec);
    Hll<uint64_t> folded_hll(prec, bufferFolded.first.get());
    folded_hll.fold(byte_array2.first.get(), byte_array2.second);
    EXPECT_LT(hll.approximateCountDistinct(), folded_hll.approximateCountDistinct()+10);
    EXPECT_GT(hll.approximateCountDistinct(), folded_hll.approximateCountDistinct()-10);
  }
}

/**
 * Test approximate count is working properly
 *
 * For supported precisions of 14 and over (currently up to 18)
 * and a set of unique elsments - verify that LogLog-Beta and
 * LogLog++ return estimations within 1% of target
 **/
TEST_F(HllTest, TestApproximateCounts) {
  const std::vector<size_t> testCardinalities = {100, 200, 1000, 100000, 500000};
  const double expectedMaxError = 0.01; // For precisions of 14 or higher we expect error to stay within 1% of real cardinality
  std::set<uint64_t> ids;
  for(uint64_t id; data_file >> id;) {
    ids.insert(id);
  }

  size_t availableIds = ids.size();
  EXPECT_GT(availableIds, 500000); // we have enough unique IDs in the file ()

  for(uint8_t prec = 14; prec <= 18; ++prec) {
    SizedBuffer buffer = Hll<uint64_t>::makeDeserializedBuffer(prec);
    Hll<uint64_t> hll(prec, buffer.first.get());
    for (size_t nCard = 0; nCard < testCardinalities.size(); ++nCard) {
      size_t itemsAdded = 0;
      uint64_t expectedMinimum = static_cast<uint64_t>( (1.0 - expectedMaxError) * testCardinalities[nCard]);
      uint64_t expectedMaximum = static_cast<uint64_t>( (1.0 + expectedMaxError) * testCardinalities[nCard]);
      for(uint64_t id: ids) {
        if (itemsAdded++ < testCardinalities[nCard]) {
          hll.add(id);
        } else {
          break;
        }
      }

      EXPECT_GT(hll.approximateCountDistinct(), expectedMinimum);
      EXPECT_LT(hll.approximateCountDistinct(), expectedMaximum);
      EXPECT_GT(hll.approximateCountDistinct_beta(), expectedMinimum);
      EXPECT_LT(hll.approximateCountDistinct_beta(), expectedMaximum);
    }
  }
}

} // namespace
