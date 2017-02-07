#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
ORANGE='\033[0;33m'
NC='\033[0m'

until /opt/vertica/bin/vsql -U dbadmin -c "SELECT 'OK';" > /dev/null 2>&1
do
  sleep 10
done

echo "Compiling libraries"

rm -rf /home/dbadmin/build
mkdir /home/dbadmin/build && cd $_
cmake /home/dbadmin/src >/dev/null 2>&1
make > /dev/null 2>&1

read -r -d '' install <<- EOF
  CREATE OR REPLACE LIBRARY HllLib AS '/home/dbadmin/build/libhll.so';

  CREATE OR REPLACE AGGREGATE FUNCTION HllCreateSynopsis
  AS LANGUAGE 'C++'
  NAME 'HllCreateSynopsisFactory'
  LIBRARY HllLib;

  GRANT EXECUTE ON AGGREGATE FUNCTION HllCreateSynopsis(BIGINT) TO PUBLIC;

  CREATE OR REPLACE AGGREGATE FUNCTION HllDistinctCount
  AS LANGUAGE 'C++'
  NAME 'HllDistinctCountFactory'
  LIBRARY HllLib;

  GRANT EXECUTE ON AGGREGATE FUNCTION HllDistinctCount(VARBINARY) TO PUBLIC;
EOF

/opt/vertica/bin/vsql -U dbadmin -c "$install" -q


echo -e "${ORANGE}Running test suite...${NC}"
for test in /home/dbadmin/src/tests/integration/*.sql
do
  echo -n "Running $(basename ${test%.*})..."
  output=$(/opt/vertica/bin/vsql -U dbadmin -f "$test")
  if [ $? -eq 0 ]
  then
    echo -e "${GREEN}[SUCCESS]${NC}"
  else
    echo -e "${RED}[FAILED]${NC}"
    echo "${RED}$output${NC}"
  fi
done
echo -e "${ORANGE}End of test suite.${NC}"
