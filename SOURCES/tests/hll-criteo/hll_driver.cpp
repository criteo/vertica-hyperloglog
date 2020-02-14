#include <iostream>
#include "hll-criteo/hll.hpp"


/**
 * A simple driver program to show how Hll can be used
 */

int main(int argc, char** argv) {
  const uint8_t precision = 12;
  Format format = Format::COMPACT_4BITS;

  // template argument tells which type will be distinct-counted
  // precision (constructor's argument) is a parameter changing the output
  // vector format and avarage estimation error
  SizedBuffer buffer = Hll<uint64_t>::makeDeserializedBuffer(precision);
  Hll<uint64_t> hll(precision, buffer.first.get());

  for(uint32_t i=0; i<1000000; ++i)
    hll.add(i); // put every value in HLL

  // The algorithm can yield the estimate at any time
  std::cout << "HLL estimate " << hll.approximateCountDistinct() << std::endl;

  // depending on the precision and format chosen, the output vector (aka synopsis)
  // will have different size
  SizedBuffer bufferDeser = Hll<uint64_t>::makeDeserializedBuffer(precision);

  // It's the users responsability to use a
  // buffer with sufficient space.
  // the serialization method is guaranteed not to write out of bounds
  hll.serialize(bufferDeser.first.get(), format);

}
