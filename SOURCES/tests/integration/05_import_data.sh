set -e

cd /opt/vertica/examples/VMart_Schema
/opt/vertica/examples/VMart_Schema/vmart_gen # --store_sales_fact=10000000
/opt/vertica/bin/vsql -U dbadmin -f vmart_schema_drop.sql
/opt/vertica/bin/vsql -U dbadmin -f vmart_define_schema.sql
/opt/vertica/bin/vsql -U dbadmin -f vmart_load_data.sql

/home/dbadmin/build/data_gen /home/dbadmin/build/hll.tbl
/opt/vertica/bin/vsql -U dbadmin -c 'DROP SCHEMA test CASCADE' | true
/opt/vertica/bin/vsql -U dbadmin -c 'CREATE SCHEMA test'
/opt/vertica/bin/vsql -U dbadmin -c 'CREATE TABLE test.artificial_data
(
  cardinality_log integer,
  ids integer

);'

/opt/vertica/bin/vsql -U dbadmin -c "COPY test.artificial_data FROM '/home/dbadmin/build/hll.tbl' DELIMITER '|' NULL '' DIRECT;"
