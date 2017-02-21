#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
ORANGE='\033[0;33m'
NC='\033[0m'

ITEST_DIR=/home/dbadmin/src/tests/integration

echoerr() { echo "$@" 1>&2; }
execute() {
  filename=$1

  echoerr -n "Running $(basename ${test%.*})..."
  if [[ $filename =~ \.sql$ ]]; then
    output=$(/opt/vertica/bin/vsql -U dbadmin -f "$filename")
  elif [[ $filename =~ \.sh$ ]]; then
    output=$(/bin/bash "$filename")
  fi

  if [ $? -eq 0 ]
  then
    echoerr -e "${GREEN}[SUCCESS]${NC}"
  else
    echoerr -e "${RED}[FAILED]${NC}"
    echoerr -e "${RED}$output${NC}"
    exit 1
  fi
}

# Send message to supervisor event group: we're ready
echo "READY"

# Waiting for the message
read -r line

# Message received, run the tests
rm -rf /home/dbadmin/build
rm -rf /home/dbadmin/src/CMakeCache.txt
mkdir /home/dbadmin/build && cd $_
cmake /home/dbadmin/src -DBUILD_DATA_GEN=ON -DSDK_HOME=/opt/vertica/sdk >/dev/null
make > /dev/null

echoerr -e "${ORANGE}Running test suite...${NC}"
for test in $(ls $ITEST_DIR | grep -e '[0-1][0-9]_.*\.\(sql\|sh\)' | sort)
do
  execute $ITEST_DIR/$test
done
echoerr -e "${ORANGE}End of test suite.${NC}"

echo -e "RESULT 2\nOK"
echoerr "Sending INT signal to PID 1"
kill -s SIGINT 1
