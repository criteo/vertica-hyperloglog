#include <bitset>
#include "Vertica.h"
#include "Hll.cpp"
#include <time.h>
#include <sstream>
#include <iostream>

#define HLL_ARRAY_SIZE_PARAMETER_NAME "hllLeadingBits"
#define HLL_ARRAY_SIZE_DEFAULT_VALUE 4

using namespace Vertica;

class HllDistinctCount : public AggregateFunction
{

  vint hllLeadingBits;
  uint64_t synopsisSize;

  public:

    virtual void initAggregate(ServerInterface &srvInterface, IntermediateAggs &aggs)
    {
        ParamReader paramReader= srvInterface.getParamReader();
        if (! paramReader.containsParameter(HLL_ARRAY_SIZE_PARAMETER_NAME) )
            vt_report_error(0, "Parameter %s is mandatory!", HLL_ARRAY_SIZE_PARAMETER_NAME);
        hllLeadingBits = paramReader.getIntRef(HLL_ARRAY_SIZE_PARAMETER_NAME);
        Hll* initialHll = new Hll(hllLeadingBits);
        this -> synopsisSize = initialHll -> getSynopsisSize();
        aggs.getStringRef(0).copy(reinterpret_cast<char*>(initialHll->getCurrentSynopsis()), synopsisSize);
        delete initialHll;
    }

    void aggregate(ServerInterface &srvInterface,
                   BlockReader &argReader,
                   IntermediateAggs &aggs)
    {
      Hll* outputHll = new Hll(*reinterpret_cast<uint8_t(*)[synopsisSize]>(aggs.getStringRef(0).data()), hllLeadingBits);
      do {
        const uint8_t (&currentSynopsis)[synopsisSize] = *reinterpret_cast<const uint8_t(*)[synopsisSize]>(argReader.getStringRef(0).data());
        outputHll->add(currentSynopsis);
      } while (argReader.next());
      aggs.getStringRef(0).copy(reinterpret_cast<char*>(outputHll->getCurrentSynopsis()), synopsisSize);
      delete outputHll;

    }

    virtual void combine(ServerInterface &srvInterface,
                         IntermediateAggs &aggs,
                         MultipleIntermediateAggs &aggsOther)
    {
      Hll* outputHll = new Hll(*reinterpret_cast<uint8_t(*)[synopsisSize]>(aggs.getStringRef(0).data()), hllLeadingBits);
      do {
        const uint8_t (&currentSynopsis)[synopsisSize] = *reinterpret_cast<const uint8_t(*)[synopsisSize]>(aggsOther.getStringRef(0).data());
        outputHll->add(currentSynopsis);
      } while (aggsOther.next());
      aggs.getStringRef(0).copy(reinterpret_cast<char*>(outputHll->getCurrentSynopsis()), synopsisSize);
      delete outputHll;
    }

    virtual void terminate(ServerInterface &srvInterface,
                           BlockWriter &resWriter,
                           IntermediateAggs &aggs)
    {
      Hll* finalHll = new Hll(*reinterpret_cast<uint8_t(*)[synopsisSize]>(aggs.getStringRef(0).data()), hllLeadingBits);
      resWriter.setInt(finalHll -> Hll::approximateCountDistinct());
      delete finalHll;
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
        intermediateTypeMetaData.addVarbinary((1UL << readSubStreamBits(srvInterface)) + 1);
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
