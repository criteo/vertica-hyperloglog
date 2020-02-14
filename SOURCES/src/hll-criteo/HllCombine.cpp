#include <bitset>
#include <time.h>
#include <sstream>
#include <iostream>

#include "Vertica.h"
#include "hll-criteo/hll.hpp"
//#include "hll_aggregate_function.hpp"
#include "hll-criteo/hll_vertica.hpp"


class HllCombine : public AggregateFunction
{

  vint hllLeadingBits;
  Format format;

public:

  virtual void setup(ServerInterface& srvInterface, const SizedColumnTypes& argTypes) {
    this -> hllLeadingBits = readSubStreamBits(srvInterface);
    this -> format = readSerializationFormat(srvInterface);
  }

  virtual void initAggregate(ServerInterface &srvInterface, IntermediateAggs &aggs)
   {
    try
    {
      size_t maxSize = Hll<uint64_t>::getMaxDeserializedBufferSize(hllLeadingBits);
      aggs.getStringRef(0).alloc(maxSize);
      Hll<uint64_t> hll = Hll<uint64_t>::wrapRawBuffer(
        hllLeadingBits,
        reinterpret_cast<uint8_t *>(aggs.getStringRef(0).data()),
        aggs.getTypeMetaData().getColumnType(0).getStringLength()
      );
      hll.reset();
    } catch (std::exception &e)
    {
      vt_report_error(0, "Exception while initializing intermediate aggregates: [%s] [%d]", e.what(), hllLeadingBits);
    }

  }

  void aggregate(ServerInterface &srvInterface,
                 BlockReader &argReader,
                 IntermediateAggs &aggs)
  {
    try {
      Hll<uint64_t> hll = Hll<uint64_t>::wrapRawBuffer(
        hllLeadingBits,
        reinterpret_cast<uint8_t *>(aggs.getStringRef(0).data()),
        aggs.getTypeMetaData().getColumnType(0).getStringLength()
      );
      do {
        hll.fold(
          reinterpret_cast<const uint8_t *>(argReader.getStringRef(0).data()),
          argReader.getStringRef(0).length()
        );
      } while (argReader.next());
    } catch(SerializationError& e) {
      vt_report_error(0, e.what());
    }
  }

  virtual void combine(ServerInterface &srvInterface,
                       IntermediateAggs &aggs,
                       MultipleIntermediateAggs &aggsOther)
  {
    try {
      Hll<uint64_t> hll = Hll<uint64_t>::wrapRawBuffer(
        hllLeadingBits,
        reinterpret_cast<uint8_t *>(aggs.getStringRef(0).data()),
        aggs.getTypeMetaData().getColumnType(0).getStringLength()
      );
      do {
        hll.fold(
          reinterpret_cast<const uint8_t *>(aggsOther.getStringRef(0).data()),
          aggsOther.getStringRef(0).length()
        );
      } while (aggsOther.next());
    } catch(SerializationError& e) {
      vt_report_error(0, e.what());
    }
  }

  virtual void terminate(ServerInterface &srvInterface,
                         BlockWriter &resWriter,
                         IntermediateAggs &aggs)
  {
    try {
      Hll<uint64_t> hll = Hll<uint64_t>::wrapRawBuffer(
        hllLeadingBits,
        reinterpret_cast<uint8_t *>(aggs.getStringRef(0).data()),
        aggs.getTypeMetaData().getColumnType(0).getStringLength()
      );
      if (hll.isBetterSerializedSparse()) {
        resWriter.getStringRef().alloc(hll.getSerializedBufferSize(Format::SPARSE));
        hll.serialize(
          reinterpret_cast<uint8_t *>(resWriter.getStringRef().data()),
          Format::SPARSE
        );
      } else {
        resWriter.getStringRef().alloc(hll.getSerializedBufferSize(format));
        hll.serialize(
          reinterpret_cast<uint8_t *>(resWriter.getStringRef().data()),
          format
        );
      }

      resWriter.next();
    } catch(SerializationError& e) {
      vt_report_error(0, e.what());
    }
  }

  InlineAggregate()
};


class HllCombineFactory : public AggregateFunctionFactory
{

  virtual void getIntermediateTypes(ServerInterface &srvInterface,
                                    const SizedColumnTypes &inputTypes,
                                    SizedColumnTypes &intermediateTypeMetaData)
  {
    uint8_t precision = readSubStreamBits(srvInterface);
    intermediateTypeMetaData.addVarbinary(Hll<uint64_t>::getMaxDeserializedBufferSize(precision) + sizeof(HLLHdr));
  }


  virtual void getPrototype(ServerInterface &srvInterface,
                            ColumnTypes &argTypes,
                            ColumnTypes &returnType)
  {
    argTypes.addVarbinary();
    returnType.addVarbinary();
  }

  virtual void getReturnType(ServerInterface &srvInterface,
                             const SizedColumnTypes &inputTypes,
                             SizedColumnTypes &outputTypes)
  {
    Format format = readSerializationFormat(srvInterface);
    uint8_t precision = readSubStreamBits(srvInterface);
    outputTypes.addVarbinary(Hll<uint64_t>::getMaxSerializedBufferSize(format, precision));
  }

  virtual AggregateFunction *createAggregateFunction(ServerInterface &srvInterface)
  {
    return vt_createFuncObject<HllCombine>(srvInterface.allocator);
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

RegisterFactory(HllCombineFactory);
