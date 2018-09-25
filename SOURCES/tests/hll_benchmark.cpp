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

#include <iostream>
#include <fstream>
#include "hll.hpp"
#include "optionparser.h"

using namespace option;
using namespace std;

/**
 * Code to run benchmarks on Hll implementations
 */


enum  optionIndex {
  UNKNOWN, HELP, MAX_CARDINALITY, MIN_CARDINALITY, REPEAT_COUNT, OUT_FILE
};
const option::Descriptor usage[] =
{
  { UNKNOWN, 0, "", "", option::Arg::None, "USAGE: hll_benchmark [options]\n\n"
    "Options:" },
  { HELP,    0, "", "help", option::Arg::None, "  --help  \tPrint usage and exit." },
  { MIN_CARDINALITY, 0, "-l", "min_cardinality", Arg::Optional, "  -l[<arg>], \t--min_cardinality[=<arg>]"
    "  \tSet max target cardinality to run benchmark, default is 1000." },
  { MAX_CARDINALITY, 0, "-h", "max_cardinality", Arg::Optional, "  -h[<arg>], \t--max_cardinality[=<arg>]"
    "  \tSet min target cardinality to run benchmark, default is 1." },
  { OUT_FILE, 0, "-o", "output_file", Arg::Optional, "  -o[<arg>], \t--output_file[=<arg>]"
    "  \tOutput file to save results" },
  { REPEAT_COUNT, 0, "-r", "repeat", Arg::Optional, "  -r[<arg>], \t--repeat[=<arg>]"
    "  \tRepeat test N times, changing hash distribution each time. Default is 10" },
  { 0, 0, 0, 0, 0, 0 }
};

int64_t getOutputStep(int64_t realCardinality) {
  // for numbers up to 999 - return 1, then increase step 10x for each extra digit in the number
  // (e.g. step 10 for 1000->9999, step 100 for 10000->99999 etc)
  if (realCardinality < 1000) {
    return 1;
  }
  int nDigitsMinusOne = (int)log10((double)realCardinality);

  return std::pow(10, nDigitsMinusOne - 2);
}

int main(int argc, char **argv) {

  uint64_t minCardinality = 1;
  uint64_t maxCardinality = 1000;
  int repeatCount = 10;
  string outputFile = "./hll_benchmark_result.csv";

  // Command line parsing code
  argc -= (argc > 0); argv += (argc > 0); // skip program name argv[0] if present
  option::Stats  stats(usage, argc, argv);
  option::Option options[stats.options_max], buffer[stats.buffer_max];
  option::Parser parse(usage, argc, argv, options, buffer);
  if (parse.error()) return 1;
  if (options[HELP]) {
    option::printUsage(cout, usage);
    return 0;
  }

  if (options[MAX_CARDINALITY].arg) {
    maxCardinality = stoull(options[MAX_CARDINALITY].arg);
    cout << "MAX_CARDINALITY <<" << maxCardinality << endl;
  }

  if (options[MAX_CARDINALITY].arg) {
    minCardinality = stoull(options[MIN_CARDINALITY].arg);
    cout << "MIN_CARDINALITY <<" << minCardinality << endl;
  }

  if (options[REPEAT_COUNT].arg) {
    cout << "REPEAT_COUNT <<" << options[REPEAT_COUNT].arg << endl;
    repeatCount = stoull(options[REPEAT_COUNT].arg);
  }

  if (options[OUT_FILE].arg) {
    outputFile = options[OUT_FILE].arg;
    cout << "OUT_FILE <<" << outputFile << endl;
  }


  for (option::Option* opt = options[UNKNOWN]; opt; opt = opt->next())
  cout << "Unknown option: " << opt->name << "\n";
  for (int i = 0; i < parse.nonOptionsCount(); ++i) cout << "Non-option #" << i << ": " << parse.nonOption(i) << "\n";

  if (maxCardinality < minCardinality) {
    cerr << "Max cardinality has to be bigger than min cardinality" << endl;
  }

  std::ofstream output(outputFile);
  uint32_t input_value = 111;
  for (uint8_t precision = 14; precision <= 14; ++precision) {
    cout << "Running for precision " << (int)precision << endl;
    for (size_t iteration = 0; iteration < repeatCount; ++iteration) {
      uint32_t hashSeed = MURMURHASH_DEFAULT_SEED;
      for (int64_t real_cardinality = 1; real_cardinality <= maxCardinality; ++real_cardinality) {
        if (iteration > 0) {
          hashSeed = (uint32_t)time(nullptr) + iteration*real_cardinality; // epoch seconds, shifted by iteration number
        }
        SizedBuffer buffer = Hll<uint64_t>::makeDeserializedBuffer(precision); // sizeof(HLLHdr) + synopsis
        Hll<uint64_t> *hll = new Hll<uint64_t>(precision, buffer.first.get(), hashSeed);
        for (size_t i=0; i<real_cardinality; ++i) {
          hll->add(input_value++); // put every value in HLL
        }
        if (real_cardinality >= minCardinality && real_cardinality % getOutputStep(real_cardinality) == 0) {

          output <<
            (int)precision   << "," <<
            real_cardinality << "," <<
            iteration        << "," <<
            static_cast<int64_t>(hll->approximateCountDistinct()) - real_cardinality << "," <<
            static_cast<int64_t>(hll->approximateCountDistinct_beta()) - real_cardinality << "\n";
        }
      }
    }
  }

}
