select version();

\set u_libfile '\'/tmp/build/HllLib.so\'';
\echo Loading the HLL library: :u_libfile
CREATE OR REPLACE LIBRARY HllLib AS :u_libfile;

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


