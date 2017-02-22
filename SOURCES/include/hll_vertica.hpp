#ifndef _HLL_VERTICA_HPP_
#define _HLL_VERTICA_HPP_

#include "hll.hpp"
#include "Vertica.h"

#define HLL_ARRAY_SIZE_PARAMETER_NAME "hllLeadingBits"
#define HLL_ARRAY_SIZE_DEFAULT_VALUE 4

#define HLL_BITS_PER_BUCKET_PARAMETER_NAME "bitsPerBucket"
#define HLL_BITS_PER_BUCKET_DEFAULT_VALUE 4

using namespace Vertica;
using HLL = Hll<uint64_t>;


Format formatCodeToEnum(uint8_t f);
int readSubStreamBits(ServerInterface &srvInterface);
Format readSerializationFormat(ServerInterface &srvInterface);

#endif