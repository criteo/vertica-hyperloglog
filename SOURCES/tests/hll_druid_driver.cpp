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

#include <iostream>
#include "hll_druid.hpp"


/**
 * A simple driver program to show off how Hll can be used
 */

using namespace druid;

int main(int argc, char** argv) {

  uint8_t bufferDruid[1031] = {0};
  HllDruid hll = HllDruid::wrapRawBuffer(bufferDruid, sizeof(bufferDruid) / sizeof(char));
  hll.reset();

  for(uint32_t i=0; i<100; ++i)
    hll.add<uint64_t>(i); // put every value in HLL

  // The algorithm can yield the estimate at any time
  std::cout << "HLL estimate " << hll.approximateCountDistinct() << std::endl;


  size_t newBufferLength;
  uint8_t newBufferDruid[1031] = {0};
  hll.serialize(newBufferDruid, newBufferLength);

  hll.reset();
  std::cout << "HLL estimate " << hll.approximateCountDistinct() << std::endl;;

  hll.fold(newBufferDruid, newBufferLength);
  std::cout << "HLL estimate " << hll.approximateCountDistinct() << std::endl;;

  for(uint32_t i=0; i<10000; ++i)
    hll.add<uint64_t>(i); // put every value in HLL

  // The algorithm can yield the estimate at any time
  std::cout << "HLL estimate " << hll.approximateCountDistinct() << std::endl;

  hll.serialize(newBufferDruid, newBufferLength);

  hll.reset();
  std::cout << "HLL estimate " << hll.approximateCountDistinct() << std::endl;

  hll.fold(newBufferDruid, newBufferLength);
  std::cout << "HLL estimate " << hll.approximateCountDistinct() << std::endl;


  // // depending on the precision and format chosen, the output vector (aka synopsis)
  // // will have different size
  // auto synopsisSize = hll.getSerializedSynopsisSize(format);
  // char outputArray[synopsisSize];

  // // It's the users responsability to use a
  // // buffer with sufficient space.
  // // the serialization method is guaranteed not to write out of bounds
  // hll.serialize(outputArray, format);

}
