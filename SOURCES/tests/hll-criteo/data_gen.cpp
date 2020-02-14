#include <cstdlib>
#include <fstream>
#include <iostream>
#include <cstdint>
#include <algorithm>


const uint8_t MAX_CARDINALITY_LOG = 25;
const uint32_t MAX_CARDINALITY = (1 << MAX_CARDINALITY_LOG);

// uint32_t lrand() {
//   return (static_cast<uint32_t>(rand()) << (sizeof(int) * 8)) | rand();
// }

void shuffle(uint32_t array[], uint32_t from, uint32_t to, uint32_t count) {

  for(uint32_t idx=0; from < to, idx < count; ++from, ++idx) {
    uint32_t rand = std::rand();
    std::swap(array[from], array[to-rand%(to-from+1)]);
  }
}

int main(int argc, char** argv) {
  if(argc != 2) {
    std::cerr << "ERROR: Output file name is not given." << std::endl;
    return 1;
  }

  std::ofstream outputFile(argv[1]);
  if(!outputFile) {
    std::cerr << "ERROR: Output file exists or permissions are incorrect: " << argv[1] << std::endl;
    return 1;
  }

  srand(0);
  uint32_t* numbers = new uint32_t[MAX_CARDINALITY];

  // fill the array with consecutive numbers 0..MAX_CARDINALITY-1
  for(uint32_t a=0; a<MAX_CARDINALITY; ++a) {
    numbers[a] = a;
  }

  for(uint32_t cardLog=2; cardLog < MAX_CARDINALITY_LOG; cardLog++) {
    uint32_t thisIterCard = 1<<cardLog;
    shuffle(numbers, 0, MAX_CARDINALITY-1, thisIterCard);
    for(uint32_t idx=0; idx<thisIterCard; ++idx) {
      outputFile << cardLog << "|" << numbers[idx] << std::endl;
    }
  }
  delete[] numbers;
  outputFile.close();
}
