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

#include "hll_druid.hpp"
#include "Vertica.h"

using namespace druid;
using namespace Vertica;

static const std::string BUFFER = "BUFFER";

class HllDruidDistinctCount : public AggregateFunction
{
public:
  virtual void initAggregate(ServerInterface &srvInterface, IntermediateAggs &aggs)
  {
    try
    {
      aggs.getStringRef(0).alloc(NUM_BYTES_FOR_BUCKETS + NUM_HEADER_BYTES);
      HllDruid hll = HllDruid::wrapRawBuffer(
          reinterpret_cast<std::uint8_t *>(aggs.getStringRef(0).data()),
          aggs.getTypeMetaData().getColumnType(0).getStringLength());
      hll.reset();
    }
    catch (std::exception &e)
    {
      vt_report_error(0, "Exception while initializing intermediate aggregates: [%s]", e.what());
    }
  }

  void aggregate(ServerInterface &srvInterface,
                 BlockReader &argReader,
                 IntermediateAggs &aggs)
  {
    try
    {
      HllDruid hll = HllDruid::wrapRawBuffer(
          reinterpret_cast<std::uint8_t *>(aggs.getStringRef(0).data()),
          aggs.getTypeMetaData().getColumnType(0).getStringLength());
      do
      {

        hll.fold(
            reinterpret_cast<const uint8_t *>(argReader.getStringRef(0).data()),
            argReader.getStringRef(0).length());
      } while (argReader.next());
    }
    catch (std::exception &e)
    {
      vt_report_error(0, "Exception while aggregating intermediates: [%s]", e.what());
    }
  }

  virtual void combine(ServerInterface &srvInterface,
                       IntermediateAggs &aggs,
                       MultipleIntermediateAggs &aggsOther)
  {
    try
    {
      HllDruid hll = HllDruid::wrapRawBuffer(
          reinterpret_cast<std::uint8_t *>(aggs.getStringRef(0).data()),
          aggs.getTypeMetaData().getColumnType(0).getStringLength());
      do
      {
        hll.fold(
            reinterpret_cast<const uint8_t *>(aggsOther.getStringRef(0).data()),
            aggsOther.getStringRef(0).length());
      } while (aggsOther.next());
    }
    catch (std::exception &e)
    {
      vt_report_error(0, "Exception while combining intermediates: [%s]", e.what());
    }
  }
  virtual void terminate(ServerInterface &srvInterface,
                         BlockWriter &resWriter,
                         IntermediateAggs &aggs)
  {
    try
    {
      HllDruid hll = HllDruid::wrapRawBuffer(
          reinterpret_cast<std::uint8_t *>(aggs.getStringRef(0).data()),
          aggs.getTypeMetaData().getColumnType(0).getStringLength());

      resWriter.setInt(hll.approximateCountDistinct());
    }
    catch (std::exception &e)
    {
      vt_report_error(0, "Exception while terminating intermediate aggregates: [%s]", e.what());
    }
  }

  InlineAggregate()
};

class HllDruidDistinctCountFactory : public AggregateFunctionFactory
{
  virtual void getIntermediateTypes(ServerInterface &srvInterface,
                                    const SizedColumnTypes &inputTypes,
                                    SizedColumnTypes &intermediateTypeMetaData)
  {
    intermediateTypeMetaData.addVarbinary(NUM_BYTES_FOR_BUCKETS + NUM_HEADER_BYTES, BUFFER);
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
    return vt_createFuncObject<HllDruidDistinctCount>(srvInterface.allocator);
  }

  virtual void getParameterType(ServerInterface &srvInterface,
                                SizedColumnTypes &parameterTypes)
  {
  }
};

RegisterFactory(HllDruidDistinctCountFactory);
RegisterLibrary("Criteo",                                                               // author
                "",                                                                     // lib_build_tag
                "0.1",                                                                  // lib_version
                "7.2.1",                                                                // lib_sdk_version
                "https://github.com/criteo/vertica-hyperloglog",                        // URL
                "Druid HyperLogLog implementation as User Defined Aggregate Functions", // description
                "",                                                                     // licenses required
                ""                                                                      // signature
);