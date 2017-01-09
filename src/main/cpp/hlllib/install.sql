select version();

\set u_libfile '\'/tmp/build/HllLib.so\'';
\echo Loading the Approximate library: :u_libfile
CREATE OR REPLACE LIBRARY HllLib AS :u_libfile;

CREATE OR REPLACE AGGREGATE FUNCTION HllDistinctCount
AS LANGUAGE 'C++' 
NAME 'HllDistinctCountFactory' 
LIBRARY HllLib;

GRANT EXECUTE
ON AGGREGATE FUNCTION HllDistinctCount(BIGINT)
TO PUBLIC;

