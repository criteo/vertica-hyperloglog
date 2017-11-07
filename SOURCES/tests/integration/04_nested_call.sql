-- Example table used by Average and Longest string aggregate UD functions.
DROP IF EXISTS T;
CREATE TABLE T (x INTEGER, y NUMERIC(5,2), z VARCHAR(10));
COPY T FROM STDIN DELIMITER ',';
1,1.5,'A'
1,3.5,'A'
2,2.0,'B'
2,3.0,'A'
2,2.6,'B'
2,1.4,'A'
3,0.5,'C'
3,3.5,'C'
3,1.5,'B'
3,7.5,'B'
4,5.5,'BC'
4,7.5,'AB'
\.

\set ON_ERROR_STOP on
select HllCreateSynopsis(x USING PARAMETERS hllLeadingBits=11) as synopsis
from T
group by z  ;
