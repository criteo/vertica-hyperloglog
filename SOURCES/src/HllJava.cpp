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

#include "stdlib.h"
#include "hll.hpp"

auto format = Format::COMPACT_6BITS;
uint8_t precision = 12;
auto bufferSize = Hll<uint64_t>::getMaxDeserializedBufferSize(precision);

extern "C" void init(char* arr) {
  uint8_t* synopsis = (uint8_t*)malloc(bufferSize);

  Hll<uint64_t> hll(12, synopsis);
  hll.reset();

  hll.serialize(reinterpret_cast<uint8_t*>(arr), format);

  free(synopsis);
}

extern "C" void add(char* arr, long i) {
  uint8_t* synopsis = (uint8_t*)malloc(bufferSize);

  Hll<uint64_t> hll(12, synopsis);
  hll.reset();

  hll.fold(reinterpret_cast<uint8_t*>(arr), bufferSize);
  hll.add(i);
  hll.serialize(reinterpret_cast<uint8_t*>(arr), format);

  free(synopsis);
}

extern "C" void merge(char* arr, char* arrOther) {
  uint8_t* synopsis = (uint8_t*)malloc(bufferSize);

  Hll<uint64_t> hll(12, synopsis);
  hll.reset();
  hll.fold(reinterpret_cast<uint8_t*>(arr), bufferSize);
  hll.fold(reinterpret_cast<uint8_t*>(arrOther), bufferSize);
  hll.serialize(reinterpret_cast<uint8_t*>(arr), format);

  free(synopsis);
}

extern "C" long count(char* arr) {
  uint8_t* synopsis = (uint8_t*)malloc(bufferSize);

  Hll<uint64_t>hll(12, synopsis);
  hll.reset();
  hll.fold(reinterpret_cast<uint8_t*>(arr), bufferSize);
  return hll.approximateCountDistinct();

  free(synopsis);
}
