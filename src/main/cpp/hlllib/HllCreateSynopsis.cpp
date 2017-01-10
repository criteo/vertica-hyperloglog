#include "Vertica.h"
#include <time.h> 
#include <sstream>
#include <iostream>
#include <functional>

#define HLL_ARRAY_SIZE_PARAMETER_NAME "hllLeadingBits"
#define HLL_ARRAY_SIZE_DEFAULT_VALUE 4
#define NO_ONE_IN_BINARY 65

using namespace Vertica;

class HllCreateSynopsis : public AggregateFunction
{

  vint hllLeadingBits;
  size_t bucketMask;
  size_t valueMask;
  size_t shift;
  std::hash<size_t> hashFunction;

  size_t bucket(size_t hash) {
      // Get the most significant bits and shift that
      return (hash & bucketMask) >> shift;
  }

  int8_t leftMostSetBit(size_t hash) {
      // We set the bucket bits to 0
      if (hash == 0)
        return 0;
      else
        return __builtin_clz(hash & valueMask) + 1 - hllLeadingBits;
  }

  public:
    virtual void setup(ServerInterface &srvInterface, 
                       const SizedColumnTypes &argTypes) {

        ParamReader paramReader= srvInterface.getParamReader();
        if (! paramReader.containsParameter(HLL_ARRAY_SIZE_PARAMETER_NAME) );
            vt_report_error(0, "Parameter HLL_ARRAY_SIZE_PARAMETER_NAME is mandatory");
        hllLeadingBits = paramReader.getIntRef(HLL_ARRAY_SIZE_PARAMETER_NAME);
        shift = sizeof(size_t) * 8 - hllLeadingBits; 

        // Ex: with a 4 bits bucket on a 2 bytes size_t we want 1111 0000 0000 0000
        // So that's 10000 minus 1 shifted with 12 zeroes 1111 0000 0000 0000
        bucketMask = (( 1 << hllLeadingBits ) - 1) >> shift;
        // Ex: with a 4 bits bucket on a 16 bit size_t  we want 0000 1111 1111 1111
        // So that's 1 with 12 ( 16 - 4 ) zeroes 0001 0000 0000 0000, minus one = 0000 1111 1111 1111
        valueMask = (1 << shift ) - 1;
    }

    virtual void initAggregate(ServerInterface &srvInterface, IntermediateAggs &aggs)
    {
        VString serializedSynopsis = aggs.getStringRef(0);
        int8_t synopsis[2 ^ hllLeadingBits ] = {};
        serializedSynopsis.copy(reinterpret_cast<char*>(synopsis));
    }

    void aggregate(ServerInterface &srvInterface,
                   BlockReader &argReader,
                   IntermediateAggs &aggs)
    {
      int8_t (&currentSynopsis)[2 ^ hllLeadingBits] = *reinterpret_cast<int8_t(*)[2 ^ hllLeadingBits]>(aggs.getStringRef(0).data());
      do {
        size_t hash = hashFunction(argReader.getIntRef(0));
        // We store in the synopsis for the the biggest leftmost one
        currentSynopsis[bucket(hash)] = max(currentSynopsis[bucket(hash)], leftMostSetBit(hash)); 
      } while (argReader.next());
    }

    virtual void combine(ServerInterface &srvInterface, 
                         IntermediateAggs &aggs, 
                         MultipleIntermediateAggs &aggsOther)
    {
      int8_t (&currentSynopsis)[2 ^ hllLeadingBits] = *reinterpret_cast<int8_t(*)[2 ^ hllLeadingBits]>(aggs.getStringRef(0).data());
      do {
        size_t hash = hashFunction(aggsOther.getIntRef(0));
        // We store in the synopsis for the the biggest leftmost one
        currentSynopsis[bucket(hash)] = max(currentSynopsis[bucket(hash)], leftMostSetBit(hash));
      } while (aggsOther.next());
    }

    virtual void terminate(ServerInterface &srvInterface, 
                           BlockWriter &resWriter, 
                           IntermediateAggs &aggs)
    {
      const VString &synopsis = aggs.getStringRef(0);
      VString &result = resWriter.getStringRef();
      result.copy(&synopsis);
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
       intermediateTypeMetaData.addVarbinary(64 * 2 ^ readSubStreamBits(srvInterface));
    }
   

    virtual void getPrototype(ServerInterface &srvInterface,
                              ColumnTypes &argTypes, 
                              ColumnTypes &returnType)
    {
        argTypes.addInt();
        returnType.addInt();
    }

    virtual void getReturnType(ServerInterface &srvInterface, 
                               const SizedColumnTypes &inputTypes, 
                               SizedColumnTypes &outputTypes)
    {
        outputTypes.addVarbinary(64 * 2 ^ readSubStreamBits(srvInterface));
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
