#include <bitset>
#include "Vertica.h"
#include <time.h>
#include <sstream>
#include <iostream>


#define HLL_ARRAY_SIZE_PARAMETER_NAME "hllLeadingBits"
#define HLL_ARRAY_SIZE_DEFAULT_VALUE 4
#define HASH_SEED 27072015

using namespace Vertica;

class HllCreateSynopsis : public AggregateFunction
{

  static const uint8_t zeroValue = 48;
  vint hllLeadingBits;
  uint64_t bucketMask;
  uint64_t valueMask;
  uint64_t synopsisSize;
  vint shift;

  uint8_t bucket(uint64_t hash) {
      // Get the most significant bits and shift that
      return (hash & bucketMask) >> shift;
  }

  uint8_t leftMostSetBit(uint64_t hash) {
      // We set the bucket bits to 0
      if (hash == 0)
        return 0;
      else
        return zeroValue + __builtin_clzll(hash & valueMask) + 1 - hllLeadingBits;
  }

  void printSynopsis(ServerInterface &srvInterface, uint8_t synopsis[]) {
        std::string value = "";
        for (uint64_t i = 0; i < synopsisSize; i++)
        {
          value += std::to_string(synopsis[i]) + ",";
        }
        srvInterface.log(value.c_str());
  }


  void printBinaryStr(ServerInterface &srvInterface, uint64_t value)
  {
    srvInterface.log("%u => %s", value, std::bitset<64>(value).to_string().c_str());
  }

  uint64_t murmurHash( const void * key, int len, unsigned int seed )
  {
    const uint64_t m = 0xc6a4a7935bd1e995;
    const int r = 47;
    uint64_t h = seed ^ (len * m);
    const uint64_t * data = (const uint64_t *)key;
    const uint64_t * end = data + (len/8);
    while(data != end)
    {
      uint64_t k = *data++;
      k *= m;
      k ^= k >> r;
      k *= m;
      h ^= k;
      h *= m;
    }
    const unsigned char * data2 = (const unsigned char*)data;
    switch(len & 7)
    {
      case 7: h ^= uint64_t(data2[6]) << 48;
      case 6: h ^= uint64_t(data2[5]) << 40;
      case 5: h ^= uint64_t(data2[4]) << 32;
      case 4: h ^= uint64_t(data2[3]) << 24;
      case 3: h ^= uint64_t(data2[2]) << 16;
      case 2: h ^= uint64_t(data2[1]) << 8;
      case 1: h ^= uint64_t(data2[0]);
      h *= m;
    };
    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h;
  }

  public:

    virtual void initAggregate(ServerInterface &srvInterface, IntermediateAggs &aggs)
    {
        ParamReader paramReader= srvInterface.getParamReader();
        if (! paramReader.containsParameter(HLL_ARRAY_SIZE_PARAMETER_NAME) )
            vt_report_error(0, "Parameter %s is mandatory!", HLL_ARRAY_SIZE_PARAMETER_NAME);
        hllLeadingBits = paramReader.getIntRef(HLL_ARRAY_SIZE_PARAMETER_NAME);
        synopsisSize = 1UL << hllLeadingBits;
        shift = sizeof(vint) * 8 - hllLeadingBits;

        // Ex: with a 4 bits bucket on a 2 bytes size_t we want 1111 0000 0000 0000
        // So that's 10000 minus 1 shifted with 12 zeroes 1111 0000 0000 0000
        bucketMask = (( 1UL << hllLeadingBits ) - 1UL) << shift;
        // Ex: with a 4 bits bucket on a 16 bit size_t  we want 0000 1111 1111 1111
        // So that's 1 with 12 ( 16 - 4 ) zeroes 0001 0000 0000 0000, minus one = 0000 1111 1111 1111
        valueMask = (1UL << shift ) - 1UL;

        VString serializedSynopsis = aggs.getStringRef(0);
        uint8_t synopsis[synopsisSize];
        memset( synopsis, zeroValue, sizeof(synopsis) );
        serializedSynopsis.copy(reinterpret_cast<char*>(synopsis));
    }

    void aggregate(ServerInterface &srvInterface,
                   BlockReader &argReader,
                   IntermediateAggs &aggs)
    {
      uint8_t (&currentSynopsis)[synopsisSize] = *reinterpret_cast<uint8_t(*)[synopsisSize]>(aggs.getStringRef(0).data());
      do {
        uint64_t hash = murmurHash(&argReader.getIntRef(0), 8, HASH_SEED);
        // We store in the synopsis for the the biggest leftmost one
        currentSynopsis[bucket(hash)] = max(currentSynopsis[bucket(hash)], leftMostSetBit(hash));
      } while (argReader.next());
    }

    virtual void combine(ServerInterface &srvInterface,
                         IntermediateAggs &aggs,
                         MultipleIntermediateAggs &aggsOther)
    {
      uint8_t (&currentSynopsis)[synopsisSize] = *reinterpret_cast<uint8_t(*)[synopsisSize]>(aggs.getStringRef(0).data());
      do {
        const uint8_t (&otherSynopsis)[synopsisSize] = *reinterpret_cast<const uint8_t(*)[synopsisSize]>(aggsOther.getStringRef(0).data());
        // We store in the synopsis for the the biggest leftmost one
        for (uint64_t i = 0; i < synopsisSize; i++)
        {
          currentSynopsis[i] = std::max(currentSynopsis[i], otherSynopsis[i]);
        }
      } while (aggsOther.next());
    }

    virtual void terminate(ServerInterface &srvInterface,
                           BlockWriter &resWriter,
                           IntermediateAggs &aggs)
    {
      resWriter.getStringRef().copy(aggs.getStringRef(0).data());
      resWriter.next();
    }

    InlineAggregate()
};


class HllCreateSynopsisFactory : public AggregateFunctionFactory
{

    int readSubStreamBits(ServerInterface &srvInterface) {
        int substreamBits = HLL_ARRAY_SIZE_DEFAULT_VALUE;
        ParamReader paramReader = srvInterface.getParamReader();
        if (paramReader.containsParameter(HLL_ARRAY_SIZE_PARAMETER_NAME))
           substreamBits = paramReader.getIntRef(HLL_ARRAY_SIZE_PARAMETER_NAME);
        return substreamBits;
    }

    virtual void getIntermediateTypes(ServerInterface &srvInterface,
                                      const SizedColumnTypes &inputTypes,
                                      SizedColumnTypes &intermediateTypeMetaData)
    {
        intermediateTypeMetaData.addVarbinary(8 * (1UL << readSubStreamBits(srvInterface)));
    }


    virtual void getPrototype(ServerInterface &srvInterface,
                              ColumnTypes &argTypes,
                              ColumnTypes &returnType)
    {
        argTypes.addInt();
        returnType.addVarbinary();
    }

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &inputTypes,
                               SizedColumnTypes &outputTypes)
    {
       outputTypes.addVarbinary(8 * (1UL << readSubStreamBits(srvInterface)));
    }

    virtual AggregateFunction *createAggregateFunction(ServerInterface &srvInterface)
    {
        return vt_createFuncObject<HllCreateSynopsis>(srvInterface.allocator);
    }

    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes)
    {
         parameterTypes.addInt("_minimizeCallCount");
         parameterTypes.addInt(HLL_ARRAY_SIZE_PARAMETER_NAME);
    }

};

RegisterFactory(HllCreateSynopsisFactory);
