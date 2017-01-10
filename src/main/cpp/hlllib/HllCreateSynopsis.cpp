#include "Vertica.h"
#include <time.h> 
#include <sstream>
#include <iostream>

#define HLL_ARRAY_SIZE_PARAMETER_NAME "hllLeadingBits"
#define HLL_ARRAY_SIZE_DEFAULT_VALUE 4


using namespace Vertica;

class HllCreateSynopsis : public AggregateFunction
{
    virtual void initAggregate(ServerInterface &srvInterface, IntermediateAggs &aggs)
    {
        vint &biggestIntSoFar = aggs.getIntRef(0);
        biggestIntSoFar = 0;
    }

    void aggregate(ServerInterface &srvInterface,
                   BlockReader &argReader,
                   IntermediateAggs &aggs)
    {
      vint &biggestIntSoFar = aggs.getIntRef(0);
      do {
        const vint &current = argReader.getIntRef(0);
        biggestIntSoFar = ( current > biggestIntSoFar ) ? current : biggestIntSoFar;
      } while (argReader.next());
    }

    virtual void combine(ServerInterface &srvInterface, 
                         IntermediateAggs &aggs, 
                         MultipleIntermediateAggs &aggsOther)
    {
      vint &biggestIntSoFar = aggs.getIntRef(0);
      do {
        const vint &current = aggsOther.getIntRef(0);
        biggestIntSoFar = ( current > biggestIntSoFar ) ? current : biggestIntSoFar;
      } while (aggsOther.next());
    }

    virtual void terminate(ServerInterface &srvInterface, 
                           BlockWriter &resWriter, 
                           IntermediateAggs &aggs)
    {
      resWriter.setInt(aggs.getIntRef(0));
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
