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
        if (paramReader.containsParameter(HLL_ARRAY_SIZE_PARAMETER_NAME) )
          hllLeadingBits = paramReader.getIntRef(HLL_ARRAY_SIZE_PARAMETER_NAME);
        else
          hllLeadingBits = HLL_ARRAY_SIZE_DEFAULT_VALUE;

        if(paramReader.containsParameter(HLL_BITS_PER_BUCKET_PARAMETER_NAME))
          this->format = formatCodeToEnum(paramReader.getIntRef(HLL_BITS_PER_BUCKET_PARAMETER_NAME));
        else
          this->format = formatCodeToEnum(HLL_BITS_PER_BUCKET_DEFAULT_VALUE);

        HLL initialHll(hllLeadingBits);
        this -> synopsisSize = initialHll.getSynopsisSize(format);
        try {
          VString &vs = aggs.getStringRef(0);
          if (majorSdkVersion(VERTICA_BUILD_ID_SDK_Version) >= 8)
            vs.alloc(this -> synopsisSize);
          initialHll.serialize(vs.data(), format);
        } catch(SerializationError& e) {
          vt_report_error(0, e.what());
        }
    }

    void aggregate(ServerInterface &srvInterface,
                   BlockReader &argReader,
                   IntermediateAggs &aggs)
    {
      HLL outputHll(hllLeadingBits);
      try {
        outputHll.deserialize(aggs.getStringRef(0).data(), format);
        do {
          HLL currentSynopsis(hllLeadingBits);
          currentSynopsis.deserialize(argReader.getStringRef(0).data(), format);
          outputHll.add(currentSynopsis);
        } while (argReader.next());
        outputHll.serialize(aggs.getStringRef(0).data(), format);
      } catch(SerializationError& e) {
        vt_report_error(0, e.what());
      }

    }

    virtual void combine(ServerInterface &srvInterface,
                         IntermediateAggs &aggs,
                         MultipleIntermediateAggs &aggsOther)
    {
      HLL outputHll(hllLeadingBits);
      try {
        outputHll.deserialize(aggs.getStringRef(0).data(), format);
        do {
          HLL currentSynopsis(hllLeadingBits);
          currentSynopsis.deserialize(aggsOther.getStringRef(0).data(), format);
          outputHll.add(currentSynopsis);
        } while (aggsOther.next());
        outputHll.serialize(aggs.getStringRef(0).data(), format);
      } catch(SerializationError& e) {
        vt_report_error(0, e.what());
      }
    }

    virtual void terminate(ServerInterface &srvInterface,
                           BlockWriter &resWriter,
                           IntermediateAggs &aggs)
    {
      HLL finalHll(hllLeadingBits);
      try {
      finalHll.deserialize(aggs.getStringRef(0).data(), format);
      } catch(SerializationError& e) {
        vt_report_error(0, e.what());
      }
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

      SizedColumnTypes::Properties props;
      props.required = false;
      props.canBeNull = false;
      props.comment = "Precision bits";
      parameterTypes.addInt(HLL_ARRAY_SIZE_PARAMETER_NAME, props);

      props.comment = "Serialization/deserialization bits per bucket";
      parameterTypes.addInt(HLL_BITS_PER_BUCKET_PARAMETER_NAME, props);
    }

};

RegisterFactory(HllDistinctCountFactory);
