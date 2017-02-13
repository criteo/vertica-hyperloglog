#include "Vertica.h"
#include "hll_raw.hpp"
#include "hll_vertica.hpp"

Format formatCodeToEnum(uint8_t f) {
  if(f == 4)
    return Format::COMPACT_4BITS;
  else if(f == 5)
    return Format::COMPACT_5BITS;
  else if(f == 6)
    return Format::COMPACT_6BITS;
  else if(f == 8)
    return Format::NORMAL;
  else
    vt_report_error(0, "Number of bits per bucket is not recognized: %d", f);
}

int readSubStreamBits(ServerInterface &srvInterface) {
    int substreamBits = HLL_ARRAY_SIZE_DEFAULT_VALUE;
    ParamReader paramReader = srvInterface.getParamReader();
    if (paramReader.containsParameter(HLL_ARRAY_SIZE_PARAMETER_NAME))
       substreamBits = paramReader.getIntRef(HLL_ARRAY_SIZE_PARAMETER_NAME);
    return substreamBits;
}

Format readSerializationFormat(ServerInterface &srvInterface) {
    Format serializationFormat = formatCodeToEnum(HLL_BITS_PER_BUCKET_DEFAULT_VALUE);
    ParamReader paramReader = srvInterface.getParamReader();
    if (paramReader.containsParameter(HLL_BITS_PER_BUCKET_PARAMETER_NAME))
      serializationFormat =
        formatCodeToEnum(paramReader.getIntRef(HLL_BITS_PER_BUCKET_PARAMETER_NAME));
    return serializationFormat;
}
