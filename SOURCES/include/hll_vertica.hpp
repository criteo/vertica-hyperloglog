#ifndef _HLL_VERTICA_HPP_
#define _HLL_VERTICA_HPP_

#include "hll.hpp"
#include "Vertica.h"
#include "BuildInfo.h"

#define HLL_ARRAY_SIZE_PARAMETER_NAME "hllLeadingBits"
#define HLL_ARRAY_SIZE_DEFAULT_VALUE 4

#define HLL_BITS_PER_BUCKET_PARAMETER_NAME "bitsPerBucket"
#define HLL_BITS_PER_BUCKET_DEFAULT_VALUE 4

using namespace Vertica;
using HLL = Hll<uint64_t>;


Format formatCodeToEnum(uint8_t f);
int readSubStreamBits(ServerInterface &srvInterface);
Format readSerializationFormat(ServerInterface &srvInterface);

constexpr int majorSdkVersion(const char *version)
{
	/* support only 0-99 major version*/
	return (version[1] == '.') ? version[0] - '0' : version[0] * 10 + version[1];
}

#endif