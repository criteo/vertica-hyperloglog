#ifndef _BASE_TEST_HPP_
#define _BASE_TEST_HPP_

#include <string>
#include "gtest/gtest.h"
#include <iostream>


/**
 * To be able to compile and run the tests in any location,
 * we make cmake inject macro SOURCE_PATH. This holds location of the
 * CMakeLists.txt file. We use it to navigate to the input files needed
 * by the tests, located in the <root>/data directory.
 */

//#define SOURCE_PATH


class HllBaseTest : public ::testing::Test {
protected:


 std::string ids_filename;

  HllBaseTest(std::string ids_filename) : ids_filename(ids_filename) {}

  std::string getInputFilename() {
    return ids_filename;
  }

    std::string getInputPath() {
        return std::string(SOURCE_PATH) + "/data/" + getInputFilename();
    }

    void generateNumbers(std::set<uint64_t>& s, uint32_t cardinality) {
      std::srand(0);
      while(s.size() < cardinality) {
          uint64_t missing = cardinality - s.size();
          for(uint32_t i=0; i < missing; ++i)
              s.insert(std::rand());
      }
  }
};

#endif
