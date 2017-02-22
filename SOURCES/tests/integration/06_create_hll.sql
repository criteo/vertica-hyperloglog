CREATE TABLE
  test.pos_transaction_number_hll
AS SELECT
  customer_key, HllCreateSynopsis(pos_transaction_number USING PARAMETERS hllLeadingBits=12, bitsPerBucket=4) as hll
FROM
  online_sales.online_sales_fact
GROUP BY
  customer_key; /* 100 */

CREATE TABLE
  test.pos_transaction_number_dc
AS SELECT
  customer_key, count (distinct pos_transaction_number) as dc
FROM
  online_sales.online_sales_fact
GROUP BY
  customer_key; /* 100 */

CREATE TABLE
  test.pos_transaction_number_comp
AS SELECT
  a.customer_key, HllDistinctCount(a.hll USING PARAMETERS hllLeadingBits=12, bitsPerBucket=4), b.dc
FROM
  test.pos_transaction_number_hll AS a
JOIN
  test.pos_transaction_number_dc AS b
ON
  a.customer_key = b.customer_key
GROUP BY
  a.customer_key, b.dc;


/* **************************** */

CREATE TABLE
  test.pos_transaction_number_date_hll
AS SELECT
  date_key, HllCreateSynopsis(pos_transaction_number USING PARAMETERS hllLeadingBits=12, bitsPerBucket=4) as hll
FROM
  store.store_sales_fact
GROUP BY
  date_key;

CREATE TABLE
  test.pos_transaction_number_date_dc
AS SELECT
  date_key, count(distinct pos_transaction_number) as dc
FROM
  store.store_sales_fact
GROUP BY
  date_key;

CREATE TABLE
  test.pos_transaction_number_date_comp
AS SELECT
  a.date_key, HllDistinctCount(a.hll USING PARAMETERS hllLeadingBits=12, bitsPerBucket=4), b.dc
FROM
  test.pos_transaction_number_date_hll AS a
JOIN
  test.pos_transaction_number_date_dc AS b
ON
  a.date_key = b.date_key
GROUP BY
  a.date_key, b.dc;

/* **************************** */

CREATE TABLE
  test.ids_hll
AS SELECT
  cardinality_log, HllCreateSynopsis(ids USING PARAMETERS hllLeadingBits=12, bitsPerBucket=4) as hll
FROM
  test.artificial_data
GROUP BY
  cardinality_log;

CREATE TABLE
  test.ids_dc
AS SELECT
  cardinality_log, count(distinct ids) as dc
FROM
  test.artificial_data
GROUP BY
  cardinality_log;

CREATE TABLE
  test.ids_comp
AS SELECT
  a.cardinality_log, HllDistinctCount(a.hll USING PARAMETERS hllLeadingBits=12, bitsPerBucket=4), b.dc
FROM
  test.ids_hll AS a
JOIN
  test.ids_dc AS b
ON
  a.cardinality_log = b.cardinality_log
GROUP BY
  a.cardinality_log, b.dc;
/* *************************** */

CREATE TABLE
  test.transaction_timeselect_hll
AS SELECT
  date_key, HllCreateSynopsis(EXTRACT(EPOCH FROM transaction_time)::int USING PARAMETERS hllLeadingBits=12, bitsPerBucket=4) as hll
FROM
  store.store_sales_fact
GROUP BY
  date_key;

CREATE TABLE
  test.transaction_timeselect_dc
AS SELECT
  date_key, count(distinct EXTRACT(EPOCH FROM transaction_time)::int) as dc
FROM
  store.store_sales_fact
GROUP BY
  date_key;

CREATE TABLE
  test.transaction_timeselect_comp
AS SELECT
  a.date_key, HllDistinctCount(a.hll USING PARAMETERS hllLeadingBits=12, bitsPerBucket=4), b.dc
FROM
  test.transaction_timeselect_hll AS a
JOIN
  test.transaction_timeselect_dc AS b
ON
  a.date_key = b.date_key
GROUP BY
  a.date_key, b.dc;
