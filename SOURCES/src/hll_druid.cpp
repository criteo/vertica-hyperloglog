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

#include "hll_druid.hpp"

using namespace druid;

inline static uint16_t merge(uint8_t *payload, size_t position, uint8_t byteToAdd, uint8_t offsetDiff)
{
  const int32_t upperNibble = *(payload + position) & 0xf0;
  const int32_t lowerNibble = *(payload + position) & 0x0f;

  const int32_t otherUpper = byteToAdd > 0 ? ((byteToAdd & 0xf0) - (offsetDiff << BITS_PER_BUCKET)) : 0;
  const int32_t otherLower = byteToAdd > 0 ? ((byteToAdd & 0x0f) - offsetDiff) : 0;

  const int32_t newUpper = upperNibble > otherUpper ? upperNibble : otherUpper;
  const int32_t newLower = lowerNibble > otherLower ? lowerNibble : otherLower;

  *(payload + position) = (int8_t)((newUpper | newLower) & 0xff);

  return (upperNibble == 0 && newUpper > 0) + (lowerNibble == 0 && newLower > 0);
}

/*
* merge an other dense hll into self
*/
static uint16_t mergeDense(uint8_t *payload, const uint8_t *otherPayload, size_t otherLength, uint8_t offsetDiff)
{
  uint16_t numNonZero = 0;
  for (size_t position = NUM_HEADER_BYTES; position < NUM_BYTES_FOR_BUCKETS + NUM_HEADER_BYTES; position++)
  {
    uint8_t byteToAdd = *(otherPayload + position);
    numNonZero += merge(payload, position, byteToAdd, offsetDiff);
  }

  return numNonZero;
}

/*
* merge an other sparse hll into self
*/
static uint16_t mergeSparse(uint8_t *payload, const uint8_t *otherPayload, size_t otherLength, uint8_t offsetDiff)
{
  uint16_t numNonZero = 0;
  for (size_t position = NUM_HEADER_BYTES; position < otherLength; position += 3)
  {
    uint16_t registerPosition = bswap_16(*reinterpret_cast<const uint16_t *>(otherPayload + position)) - NUM_HEADER_BYTES;
    uint8_t byteToAdd = *(otherPayload + position + sizeof(registerPosition));

    if (byteToAdd != 0)
    {
      if (registerPosition >= NUM_BYTES_FOR_BUCKETS + NUM_HEADER_BYTES - 1)
      {
        throw std::runtime_error("invalid payloadstart position:" + std::to_string(registerPosition));
      }
      numNonZero += merge(payload, registerPosition + NUM_HEADER_BYTES, byteToAdd, offsetDiff);
    }
  }

  return numNonZero;
}

/**
 * some other smart maths
 */
static double applyCorrection(double e, uint16_t zeroCount)
{
  if (e == 0)
  {
    throw std::runtime_error("estimation cannot be zero");
  }

  e = CORRECTION_PARAMETER / e;

  if (e <= LOW_CORRECTION_THRESHOLD)
  {
    // this is linear counting http://www.moderndescartes.com/essays/hyperloglog/
    return zeroCount == 0 ? e : NUM_BUCKETS * log(NUM_BUCKETS / (double)zeroCount);
  }

  if (e > HIGH_CORRECTION_THRESHOLD)
  {
    double ratio = e / TWO_TO_THE_SIXTY_FOUR;
    if (ratio >= 1)
    {
      // handle very unlikely case that value is > 2^64
      return -1;
    }
    else
    {
      return -TWO_TO_THE_SIXTY_FOUR * log(1 - ratio);
    }
  }

  return e;
}

void HllDruid::add(const uint8_t *hashedValue, size_t length)
{
  uint16_t bucket = bswap_16(*reinterpret_cast<const uint16_t *>(hashedValue + 14)) & BUCKET_MASK;

  uint8_t positionOf1 = 0;

  for (uint8_t i = 0; i < 8; ++i)
  {
    uint8_t lookupVal = positionOf1Lookup[*(hashedValue + i)];
    switch (lookupVal)
    {
    case 0:
      positionOf1 += 8;
      continue;
    default:
      positionOf1 += lookupVal;
      i = 8;
      break;
    }
  }

  addRegister(bucket, positionOf1);
}

/**
 * Merge an other HLL (sparse or dense) into self
 * */
void HllDruid::fold(const HllDruid &other)
{
  // increase this bucket offset up to other offset
  while (this->getRegisterOffset() < other.getRegisterOffset())
  {
    this->setRegisterOffset(this->getRegisterOffset() + 1);
    this->setNumNonZeroRegisters(decrementBuckets());
  }

  uint16_t numNonZero = this->getNumNonZeroRegisters();

  if (other.isSparse())
  { // sparse
    numNonZero += mergeSparse(this->mutablePayload(),
                              other.payload,
                              other.length,
                              this->getRegisterOffset() - other.getRegisterOffset());
  }
  else
  { // dense
    numNonZero += mergeDense(this->mutablePayload(),
                             other.payload,
                             other.length,
                             this->getRegisterOffset() - other.getRegisterOffset());
  }

  if (numNonZero == NUM_BUCKETS)
  {
    numNonZero = decrementBuckets();
    setRegisterOffset(getRegisterOffset() + 1);
  }

  setNumNonZeroRegisters(numNonZero);

  // update overflow and decrement bucket if needed;
  addRegister(other.getMaxOverFlowRegister(), other.getMaxOverFlowValue());
}
size_t HllDruid::getSerializedBufferSize()
{
  if (getNumNonZeroRegisters() < DENSE_THRESHOLD)
  {
    size_t length = NUM_HEADER_BYTES;
    for (int i = 0; i < NUM_BYTES_FOR_BUCKETS; ++i)
    {
      if (payload[i + NUM_HEADER_BYTES] != 0)
      {
        length += 3;
      }
    }
    return length;
  }
  else
  {
    return NUM_HEADER_BYTES + NUM_BYTES_FOR_BUCKETS;
  }
}

void HllDruid::serialize(uint8_t *outBuffer, size_t &outLength)
{
  if (getNumNonZeroRegisters() < DENSE_THRESHOLD)
  { // store sparsely;
    memcpy(outBuffer, payload, sizeof(Header));
    outLength = NUM_HEADER_BYTES;
    for (uint16_t i = 0; i < NUM_BYTES_FOR_BUCKETS; ++i)
    {
      if (payload[i + NUM_HEADER_BYTES] != 0)
      {
        *(outBuffer + outLength + 0) = ((i + NUM_HEADER_BYTES) >> 8);
        *(outBuffer + outLength + 1) = ((i + NUM_HEADER_BYTES) & 0x00ff);
        *(outBuffer + outLength + 2) = payload[i + NUM_HEADER_BYTES];
        outLength += 3;
      }
    }
  }
  else
  { // store densely
    outLength = NUM_HEADER_BYTES + NUM_BYTES_FOR_BUCKETS;
    memcpy(outBuffer, payload, outLength);
  }
}

/**
 * Add nibble in register
 * */
uint16_t HllDruid::addNibbleRegister(uint16_t bucket, uint8_t positionOf1)
{
  uint16_t numNonZeroRegs = getNumNonZeroRegisters();

  size_t position = getPayloadBytePosition() + (uint16_t)(bucket >> 1);
  bool isUpperNibble = ((bucket & 0x1) == 0);

  uint8_t shiftedPositionOf1 = (isUpperNibble) ? (uint8_t)(positionOf1 << BITS_PER_BUCKET) : positionOf1;

  uint8_t origVal = *(payload + position);
  uint8_t newValueMask = (isUpperNibble) ? (uint8_t)0xf0 : (uint8_t)0x0f;
  uint8_t originalValueMask = (uint8_t)(newValueMask ^ 0xff);

  // if something was at zero, we have to increase the numNonZeroRegisters
  if ((origVal & newValueMask) == 0 && shiftedPositionOf1 != 0)
  {
    numNonZeroRegs++;
  }

  const uint8_t left = (origVal & newValueMask);
  const uint8_t right = shiftedPositionOf1;

  *(this->mutablePayload() + position) = (left > right ? left : right) | (origVal & originalValueMask);

  return numNonZeroRegs;
}

/**
 * add a position of 1 into a register (or overflow)
 * */
void HllDruid::addRegister(uint16_t bucket, uint8_t positionOf1)
{

  uint8_t registerOffset = getRegisterOffset();

  if (positionOf1 <= registerOffset)
  {
    return;
  }
  else if (positionOf1 > (registerOffset + RANGE))
  {
    uint8_t currMax = getMaxOverFlowValue();
    if (positionOf1 > currMax)
    {
      if (currMax <= (registerOffset + RANGE))
      {
        addRegister(getMaxOverFlowRegister(), currMax);
      }
      setMaxOverflowValue(positionOf1);
      setMaxOverflowRegister(bucket);
    }
    return;
  }

  uint16_t numNonZeroRegisters = addNibbleRegister(bucket, (uint8_t)((0xff & positionOf1) - registerOffset));
  setNumNonZeroRegisters(numNonZeroRegisters);
  if (numNonZeroRegisters == NUM_BUCKETS)
  {
    setRegisterOffset(++registerOffset);
    uint16_t newNumZero = decrementBuckets();
    setNumNonZeroRegisters(newNumZero);
  }
}

/*
* decrement each nibble in all registers by 1
*/
uint16_t HllDruid::decrementBuckets()
{
  uint8_t *mutPayload = this->mutablePayload();
  uint16_t count = 0;
  for (size_t i = NUM_HEADER_BYTES; i < NUM_HEADER_BYTES + NUM_BYTES_FOR_BUCKETS; i++)
  {
    uint8_t val = *(mutPayload + i);
    if ((val & 0xf0) != 0)
    {
      val -= 0x10;
    }
    if ((val & 0x0f) != 0)
    {
      val -= 0x01;
    }

    if ((val & 0xf0) != 0)
    {
      ++count;
    }
    if ((val & 0x0f) != 0)
    {
      ++count;
    }
    *(mutPayload + i) = val;
  }
  return count;
}

/**
 * smart maths
 */
uint64_t HllDruid::estimateCardinality() const
{
  const uint8_t registerOffset = getRegisterOffset();
  const uint8_t overflowValue = getMaxOverFlowValue();
  const uint16_t overflowRegister = getMaxOverFlowRegister();
  const uint16_t overflowPosition = overflowRegister / 2;
  const bool isUpperNibble = ((overflowRegister & 0x1) == 0);
  double e = 0.0;
  uint16_t zeroCount = 0;
  for (size_t position = 0; position < NUM_BYTES_FOR_BUCKETS; position++)
  {
    uint8_t registerValue = *(payload + NUM_HEADER_BYTES + position);
    if (overflowValue != 0 && position == overflowPosition)
    {
      uint8_t upperNibble = ((registerValue & 0xf0) >> BITS_PER_BUCKET) + registerOffset;
      uint8_t lowerNibble = (registerValue & 0x0f) + registerOffset;
      if (isUpperNibble)
      {
        upperNibble = upperNibble > overflowValue ? upperNibble : overflowValue;
      }
      else
      {
        lowerNibble = lowerNibble > overflowValue ? lowerNibble : overflowValue;
      }
      e += 1.0 / pow(2, upperNibble) + 1.0 / pow(2, lowerNibble);
      zeroCount += (((upperNibble & 0xf0) == 0) ? 1 : 0) + (((lowerNibble & 0x0f) == 0) ? 1 : 0);
    }
    else
    {
      e += minNumRegisterLookup[registerOffset][registerValue];
      zeroCount += numZeroLookup[registerValue];
    }
  }
  return applyCorrection(e, zeroCount);
}
