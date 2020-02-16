#ifndef _HLL_UTILS_H_
#define _HLL_UTILS_H_

#include <stdint.h>

#define __packed__ __attribute__((packed))
#define __const_fun__ __attribute__((const))


// c.f. https://en.wikipedia.org/wiki/Double-precision_floating-point_format
#define FLOAT64_FRACTION_BITS_COUNT 52

/*
 * Computes 1 / 2 ^ pow by directly decrementing the exponent bits in the number's
 * binary representation.
 */
static inline __const_fun__ double fast_inv_pow2(const uint8_t pow) {
  union { double d; uint64_t u; } res = { .d = 1 };
  res.u -= ((uint64_t)pow) << FLOAT64_FRACTION_BITS_COUNT; // evil floating point bit level hacking
  return res.d;
}


#endif
