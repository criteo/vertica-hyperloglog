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

class HllBaseTest : public ::testing::Test
{
protected:
  std::string ids_filename;

  HllBaseTest(std::string ids_filename) : ids_filename(ids_filename) {}

  std::string getInputFilename()
  {
    return ids_filename;
  }

  std::string getInputPath()
  {
    return std::string(SOURCE_PATH) + "/data/" + getInputFilename();
  }

  void generateNumbers(std::set<uint64_t> &s, uint32_t cardinality)
  {
    std::srand(0);
    while (s.size() < cardinality)
    {
      uint64_t missing = cardinality - s.size();
      for (uint32_t i = 0; i < missing; ++i)
        s.insert(std::rand());
    }
  }
};

#endif
