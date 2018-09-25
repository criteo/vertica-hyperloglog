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

#ifndef _HLL_VERTICA_HPP_
#define _HLL_VERTICA_HPP_

#include "hll.hpp"
#include "Vertica.h"

#define HLL_ARRAY_SIZE_PARAMETER_NAME "hllLeadingBits"
#define HLL_ARRAY_SIZE_DEFAULT_VALUE 12
#define HLL_ARRAY_SIZE_MIN_VALUE 1
#define HLL_ARRAY_SIZE_MAX_VALUE 16

#define HLL_BITS_PER_BUCKET_PARAMETER_NAME "bitsPerBucket"
#define HLL_BITS_PER_BUCKET_DEFAULT_VALUE 6

using namespace Vertica;
using HLL = Hll<uint64_t>;


Format formatCodeToEnum(uint8_t f);
int readSubStreamBits(ServerInterface &srvInterface);
Format readSerializationFormat(ServerInterface &srvInterface);

#endif