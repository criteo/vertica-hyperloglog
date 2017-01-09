#include "Vertica.h"
#include <time.h> 
#include <sstream>
#include <iostream>

using namespace Vertica;


/**
 * User Defined Aggregate Function that takes in strings and gets the longest string.
 * If many strings have lengths equal to the longest string, it returns the string
 * that sorts last in ascending order.
 *
 * Note: NULL values are skipped.
 */
class HllDistinctCount : public AggregateFunction
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


class HllDistinctCountFactory : public AggregateFunctionFactory
{
    virtual void getIntermediateTypes(ServerInterface &srvInterface, const SizedColumnTypes &inputTypes, SizedColumnTypes &intermediateTypeMetaData)
    {
        intermediateTypeMetaData.addInt();
    }

    virtual void getPrototype(ServerInterface &srvfloaterface, ColumnTypes &argTypes, ColumnTypes &returnType)
    {
        argTypes.addInt();
        returnType.addInt();
    }

    virtual void getReturnType(ServerInterface &srvfloaterface, 
                               const SizedColumnTypes &inputTypes, 
                               SizedColumnTypes &outputTypes)
    {
        outputTypes.addInt();
    }

    virtual AggregateFunction *createAggregateFunction(ServerInterface &srvfloaterface)
    {
      return vt_createFuncObject<HllDistinctCount>(srvfloaterface.allocator);
    }

    // Hint (see doc) to batch rows together
    virtual void getParameterType(ServerInterface &srvInterface,
                                     SizedColumnTypes &parameterTypes)
    {
         parameterTypes.addInt("_minimizeCallCount");
    }

};

RegisterFactory(HllDistinctCountFactory);
