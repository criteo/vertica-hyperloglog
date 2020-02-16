#include "stdlib.h"
#include "hll-criteo/hll.hpp"

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

extern "C" int compact(char* arr, char* output) {
  uint8_t* synopsis = (uint8_t*)malloc(bufferSize);

  Hll<uint64_t> hll(12, synopsis);
  hll.reset();
  hll.fold(reinterpret_cast<uint8_t*>(arr), bufferSize);

  if (hll.isBetterSerializedSparse()) {
    hll.serialize(
      reinterpret_cast<uint8_t *>(output),
      Format::SPARSE
    );
    return hll.getSerializedBufferSize(Format::SPARSE);
  } else {
    hll.serialize(
      reinterpret_cast<uint8_t*>(output),
      format
    );
    return hll.getMaxSerializedBufferSize(format, 12);
  }
}

extern "C" long count(char* arr) {
  uint8_t* synopsis = (uint8_t*)malloc(bufferSize);

  Hll<uint64_t>hll(12, synopsis);
  hll.reset();
  hll.fold(reinterpret_cast<uint8_t*>(arr), bufferSize);
  return hll.approximateCountDistinct();

  free(synopsis);
}
