#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
ORANGE='\033[0;33m'
NC='\033[0m'

echoerr() { echo "$@" 1>&2; }

# Send message to supervisor event group: we're ready
echo "READY"

# Waiting for the message
read -r line

# Message received, run the tests
rm -rf /home/dbadmin/build
mkdir /home/dbadmin/build && cd $_
cmake /home/dbadmin/src >/dev/null 2>&1
make > /dev/null 2>&1

echoerr -e "${ORANGE}Running test suite...${NC}"
for test in /home/dbadmin/src/tests/integration/*.sql
do
  echoerr -n "Running $(basename ${test%.*})..."
  output=$(/opt/vertica/bin/vsql -U dbadmin -f "$test")
  if [ $? -eq 0 ]
  then
    echoerr -e "${GREEN}[SUCCESS]${NC}"
  else
    echoerr -e "${RED}[FAILED]${NC}"
    echoerr "${RED}$output${NC}"
    exit 1
  fi
done
echoerr -e "${ORANGE}End of test suite.${NC}"

echo -e "RESULT 2\nOK"
echoerr "Sending INT signal to PID 1"
kill -s SIGINT 1
