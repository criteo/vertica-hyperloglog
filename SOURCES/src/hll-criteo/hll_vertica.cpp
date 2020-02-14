#include "Vertica.h"
#include "hll-criteo/hll_raw.hpp"
#include "hll-criteo/hll_vertica.hpp"

Format formatCodeToEnum(uint8_t f) {
  Format ret = Format::NORMAL;
  if(f == 4)
    ret = Format::COMPACT_4BITS;
  else if(f == 5)
    ret = Format::COMPACT_5BITS;
  else if(f == 6)
    ret = Format::COMPACT_6BITS;
  else if(f == 8)
    ret = Format::NORMAL;
  else
    vt_report_error(0, "Number of bits per bucket is not recognized: %d", f);
  return ret;
}

int readSubStreamBits(ServerInterface &srvInterface) {
  vint hllLeadingBits;
  ParamReader paramReader = srvInterface.getParamReader();

  if (paramReader.containsParameter(HLL_ARRAY_SIZE_PARAMETER_NAME) ) {
    hllLeadingBits = paramReader.getIntRef(HLL_ARRAY_SIZE_PARAMETER_NAME);
    if(hllLeadingBits < HLL_ARRAY_SIZE_MIN_VALUE || hllLeadingBits > HLL_ARRAY_SIZE_MAX_VALUE) {
      vt_report_error(2, "Provided value of the %s parameter is not supported. The value should be between %d and %d, inclusive",
        HLL_ARRAY_SIZE_PARAMETER_NAME, HLL_ARRAY_SIZE_MIN_VALUE, HLL_ARRAY_SIZE_MAX_VALUE);
    }
  } else {
    LogDebugUDxWarn(srvInterface, "Parameter %s was not provided. Defaulting to %d",
      HLL_ARRAY_SIZE_PARAMETER_NAME, HLL_ARRAY_SIZE_DEFAULT_VALUE);
    hllLeadingBits = HLL_ARRAY_SIZE_DEFAULT_VALUE;
  }
  return hllLeadingBits;
}

Format readSerializationFormat(ServerInterface &srvInterface) {
  Format format;
  ParamReader paramReader = srvInterface.getParamReader();

  if(paramReader.containsParameter(HLL_BITS_PER_BUCKET_PARAMETER_NAME)) {
    int formatInt = paramReader.getIntRef(HLL_BITS_PER_BUCKET_PARAMETER_NAME);
    if(formatInt != 4 && formatInt != 5 && formatInt != 6 && formatInt != 8) {
      vt_report_error(2, "Provided value of the %s parameter is not supported. The value should be equal to 4,5,6 or 8",
        HLL_BITS_PER_BUCKET_PARAMETER_NAME);
    }
    format = formatCodeToEnum(formatInt);
  } else {
    LogDebugUDxWarn(srvInterface, "Parameter %s was not provided. Defaulting to %d",
      HLL_BITS_PER_BUCKET_PARAMETER_NAME, HLL_BITS_PER_BUCKET_DEFAULT_VALUE);
    format = formatCodeToEnum(HLL_BITS_PER_BUCKET_DEFAULT_VALUE);
  }
  return format;
}
