select HllDistinctCount(synopsis USING PARAMETERS hllLeadingBits=11) as cnt
from
(
  select HllCreateSynopsis(value USING PARAMETERS hlLLeadingBits=11) as synopsis
  from
  (
    select 1234567890 as value
    union
    select 2234567890
    union
    select 67890
    union
    select 12345
  ) as t
) as u;
