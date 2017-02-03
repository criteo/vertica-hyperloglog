#include <bitset>
#include <time.h>
#include <sstream>
#include <iostream>

#include "hll.hpp"
#include "Vertica.h"

#define HLL_ARRAY_SIZE_PARAMETER_NAME "hllLeadingBits"
#define HLL_ARRAY_SIZE_DEFAULT_VALUE 4

using namespace Vertica;
using HLL = Hll<uint64_t>;

const Format FORMAT = Format::SPARSE;

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
        HLL initialHll(hllLeadingBits);
        this -> synopsisSize = initialHll.getSynopsisSize(FORMAT);
        initialHll.serialize(aggs.getStringRef(0).data(), FORMAT);
    }

    void aggregate(ServerInterface &srvInterface,
                   BlockReader &argReader,
                   IntermediateAggs &aggs)
    {
      HLL outputHll(hllLeadingBits);
      outputHll.deserialize(aggs.getStringRef(0).data(), FORMAT);
      do {
        HLL currentSynopsis(hllLeadingBits);
        currentSynopsis.deserialize(argReader.getStringRef(0).data(), FORMAT);
        outputHll.add(currentSynopsis);
      } while (argReader.next());
      outputHll.serialize(aggs.getStringRef(0).data(), FORMAT);

    }

    virtual void combine(ServerInterface &srvInterface,
                         IntermediateAggs &aggs,
                         MultipleIntermediateAggs &aggsOther)
    {
      HLL outputHll(hllLeadingBits);
      outputHll.deserialize(aggs.getStringRef(0).data(), FORMAT);
      do {
        HLL currentSynopsis(hllLeadingBits);
        currentSynopsis.deserialize(aggsOther.getStringRef(0).data(), FORMAT);
        outputHll.add(currentSynopsis);
      } while (aggsOther.next());
      outputHll.serialize(aggs.getStringRef(0).data(), FORMAT);
    }

    virtual void terminate(ServerInterface &srvInterface,
                           BlockWriter &resWriter,
                           IntermediateAggs &aggs)
    {
      HLL finalHll(hllLeadingBits);
      finalHll.deserialize(aggs.getStringRef(0).data(), FORMAT);
      resWriter.setInt(finalHll.approximateCountDistinct());
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
      HLL dummy(readSubStreamBits(srvInterface));
      intermediateTypeMetaData.addVarbinary(dummy.getSynopsisSize(FORMAT));
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
