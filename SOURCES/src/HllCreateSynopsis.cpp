/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

#include <bitset>
#include <time.h>
#include <sstream>
#include <iostream>

#include "Vertica.h"
#include "hll.hpp"
#include "hll_vertica.hpp"


class HllCreateSynopsis : public AggregateFunction
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
    try {
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
        const vint &currentValue = argReader.getIntRef(0);
        hll.add(currentValue);
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


class HllCreateSynopsisFactory : public AggregateFunctionFactory
{

  virtual void getIntermediateTypes(ServerInterface &srvInterface,
                                    const SizedColumnTypes &inputTypes,
                                    SizedColumnTypes &intermediateTypeMetaData)
  {
    uint8_t precision = readSubStreamBits(srvInterface);
    intermediateTypeMetaData.addVarbinary(Hll<uint64_t>::getMaxDeserializedBufferSize(precision));
  }


  virtual void getPrototype(ServerInterface &srvInterface,
                            ColumnTypes &argTypes,
                            ColumnTypes &returnType)
  {
    argTypes.addInt();
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
