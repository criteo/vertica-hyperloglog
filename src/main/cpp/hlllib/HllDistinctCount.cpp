#include <bitset>
#include "Vertica.h"
#include <time.h>
#include <sstream>
#include <iostream>

#define HLL_ARRAY_SIZE_PARAMETER_NAME "hllLeadingBits"
#define HLL_ARRAY_SIZE_DEFAULT_VALUE 4
#define HASH_SEED 27072015

using namespace Vertica;

class HllDistinctCount : public AggregateFunction
{

  static const uint8_t zeroValue = 48;
  vint hllLeadingBits;
  uint64_t bucketMask;
  uint64_t valueMask;
  uint64_t synopsisSize;
  vint shift;

  double alpha;

  public:

    virtual void initAggregate(ServerInterface &srvInterface, IntermediateAggs &aggs)
    {
        ParamReader paramReader= srvInterface.getParamReader();
        if (! paramReader.containsParameter(HLL_ARRAY_SIZE_PARAMETER_NAME) )
            vt_report_error(0, "Parameter %s is mandatory!", HLL_ARRAY_SIZE_PARAMETER_NAME);
        hllLeadingBits = paramReader.getIntRef(HLL_ARRAY_SIZE_PARAMETER_NAME);
        synopsisSize = 1UL << hllLeadingBits;

        // Alpha computation (see paper)
        switch (hllLeadingBits) {
          case 4:
            alpha = 0.673;
            break;
          case 5:
            alpha = 0.697;
            break;
          case 6:
            alpha = 0.709;
          default:
            alpha = (0.7213 / (1.0 + (1.079 / static_cast<double>(1 << hllLeadingBits))));
        }

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
        const uint8_t (&otherSynopsis)[synopsisSize] = *reinterpret_cast<const uint8_t(*)[synopsisSize]>(argReader.getStringRef(0).data());
        // We store in the synopsis for the the biggest leftmost one
        for (uint64_t i = 0; i < synopsisSize; i++)
        {
          currentSynopsis[i] = std::max(currentSynopsis[i], otherSynopsis[i]);
        }
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
      uint8_t (&finalSynopsis)[synopsisSize] = *reinterpret_cast<uint8_t(*)[synopsisSize]>(aggs.getStringRef(0).data());
      double harmonicMean = 0.0;
      for (uint64_t i = 0; i < synopsisSize; i++)
      {
          harmonicMean += 1.0 / (1 << (finalSynopsis[i] - zeroValue));
      }
      harmonicMean = synopsisSize / harmonicMean;
      srvInterface.log("harmonic mean: %f", harmonicMean);
      resWriter.setInt(round(0.5 + alpha * harmonicMean * synopsisSize));
    }

    InlineAggregate()
};


class HllDistinctCountFactory : public AggregateFunctionFactory
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
        argTypes.addVarbinary();
        returnType.addInt();
    }

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &inputTypes,
                               SizedColumnTypes &outputTypes)
    {
       outputTypes.addInt();
    }

    virtual AggregateFunction *createAggregateFunction(ServerInterface &srvInterface)
    {
        return vt_createFuncObject<HllDistinctCount>(srvInterface.allocator);
    }

    virtual void getParameterType(ServerInterface &srvInterface,
                                  SizedColumnTypes &parameterTypes)
    {
         parameterTypes.addInt("_minimizeCallCount");
         parameterTypes.addInt(HLL_ARRAY_SIZE_PARAMETER_NAME);
    }

};

RegisterFactory(HllDistinctCountFactory);
