#include <bitset>
#include <time.h>
#include <sstream>
#include <iostream>

#include "hll.hpp"
#include "hll_vertica.hpp"

class HllDistinctCount : public AggregateFunction
{

  vint hllLeadingBits;
  uint64_t synopsisSize;
  Format format;

  public:

    virtual void initAggregate(ServerInterface &srvInterface, IntermediateAggs &aggs)
    {
        ParamReader paramReader= srvInterface.getParamReader();
        if (! paramReader.containsParameter(HLL_ARRAY_SIZE_PARAMETER_NAME) )
            vt_report_error(0, "Parameter %s is mandatory!", HLL_ARRAY_SIZE_PARAMETER_NAME);
        hllLeadingBits = paramReader.getIntRef(HLL_ARRAY_SIZE_PARAMETER_NAME);

        if(paramReader.containsParameter(HLL_BITS_PER_BUCKET_PARAMETER_NAME))
          this->format = formatCodeToEnum(paramReader.getIntRef(HLL_BITS_PER_BUCKET_PARAMETER_NAME));
        else
          this->format = formatCodeToEnum(HLL_BITS_PER_BUCKET_DEFAULT_VALUE);

        HLL initialHll(hllLeadingBits);
        this -> synopsisSize = initialHll.getSynopsisSize(format);
        initialHll.serialize(aggs.getStringRef(0).data(), format);
    }

    void aggregate(ServerInterface &srvInterface,
                   BlockReader &argReader,
                   IntermediateAggs &aggs)
    {
      HLL outputHll(hllLeadingBits);
      outputHll.deserialize(aggs.getStringRef(0).data(), format);
      do {
        HLL currentSynopsis(hllLeadingBits);
        currentSynopsis.deserialize(argReader.getStringRef(0).data(), format);
        outputHll.add(currentSynopsis);
      } while (argReader.next());
      outputHll.serialize(aggs.getStringRef(0).data(), format);

    }

    virtual void combine(ServerInterface &srvInterface,
                         IntermediateAggs &aggs,
                         MultipleIntermediateAggs &aggsOther)
    {
      HLL outputHll(hllLeadingBits);
      outputHll.deserialize(aggs.getStringRef(0).data(), format);
      do {
        HLL currentSynopsis(hllLeadingBits);
        currentSynopsis.deserialize(aggsOther.getStringRef(0).data(), format);
        outputHll.add(currentSynopsis);
      } while (aggsOther.next());
      outputHll.serialize(aggs.getStringRef(0).data(), format);
    }

    virtual void terminate(ServerInterface &srvInterface,
                           BlockWriter &resWriter,
                           IntermediateAggs &aggs)
    {
      HLL finalHll(hllLeadingBits);
      finalHll.deserialize(aggs.getStringRef(0).data(), format);
      resWriter.setInt(finalHll.approximateCountDistinct());
    }

    InlineAggregate()
};


class HllDistinctCountFactory : public AggregateFunctionFactory
{
    virtual void getIntermediateTypes(ServerInterface &srvInterface,
                                      const SizedColumnTypes &inputTypes,
                                      SizedColumnTypes &intermediateTypeMetaData)
    {
      HLL dummy(readSubStreamBits(srvInterface));
      Format format = readSerializationFormat(srvInterface);
      intermediateTypeMetaData.addVarbinary(dummy.getSynopsisSize(format));
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
