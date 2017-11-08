#include <bitset>
#include <time.h>
#include <sstream>
#include <iostream>

#include "Vertica.h"
#include "hll.hpp"
#include "hll_vertica.hpp"
#include "stack_trace.hpp"

class HllCreateSynopsis : public AggregateFunction
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
        try {
        srvInterface.log("====initAggregate: aggs.getStringRef(0).data(): %s\n",aggs.getStringRef(0).str().c_str());
          initialHll.serialize(aggs.getStringRef(0).data(), format);
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
          const vint &currentValue = argReader.getIntRef(0);
          outputHll.add(currentValue);
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
      //print_stacktrace();
      srvInterface.log("====combine: got hllLeadingBits: %d, format: %d\n", (int) hllLeadingBits, (int)format);
      srvInterface.log("====combine: aggs.getStringRef(0).data(): %s\n",aggs.getStringRef(0).str().c_str());
      try {
        outputHll.deserialize(aggs.getStringRef(0).data(), format );
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
      resWriter.getStringRef().copy(aggs.getStringRef(0).data(), synopsisSize);
      resWriter.next();
    }

    InlineAggregate()
};


class HllCreateSynopsisFactory : public AggregateFunctionFactory
{

    virtual void getIntermediateTypes(ServerInterface &srvInterface,
                                      const SizedColumnTypes &inputTypes,
                                      SizedColumnTypes &intermediateTypeMetaData)
    {
      HLL dummy(readSubStreamBits(srvInterface));
      Format format = readSerializationFormat(srvInterface);
      intermediateTypeMetaData.addVarbinary(dummy.getSynopsisSize(format));
      srvInterface.log("=====getIntermediateTypes======\n");
      srvInterface.log("=====readSubStreamBits: %d == format: %d, size: %d\n",readSubStreamBits(srvInterface), format, dummy.getSynopsisSize(format));
    }


    virtual void getPrototype(ServerInterface &srvInterface,
                              ColumnTypes &argTypes,
                              ColumnTypes &returnType)
    {
        argTypes.addInt();
        returnType.addVarbinary();
        srvInterface.log("=====getPrototype======\n");
    }

    virtual void getReturnType(ServerInterface &srvInterface,
                               const SizedColumnTypes &inputTypes,
                               SizedColumnTypes &outputTypes)
    {
      HLL dummy(readSubStreamBits(srvInterface));
      Format format = readSerializationFormat(srvInterface);
      outputTypes.addVarbinary(dummy.getSynopsisSize(format));
      srvInterface.log("=====getReturnType======\n");
      srvInterface.log("=====readSubStreamBits: %d == format: %d, size: %d\n",readSubStreamBits(srvInterface), format, dummy.getSynopsisSize(format));
    }

    virtual AggregateFunction *createAggregateFunction(ServerInterface &srvInterface)
    {
        return vt_createFuncObject<HllCreateSynopsis>(srvInterface.allocator);
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

RegisterFactory(HllCreateSynopsisFactory);
