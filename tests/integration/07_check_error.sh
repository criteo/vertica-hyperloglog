rm -f ids_comp.csv timeselect_comp.csv date_comp.csv number_comp.csv

/opt/vertica/bin/vsql -U dbadmin -F ',' -A -o ids_comp.csv -c 'select * from test.ids_comp;'
/opt/vertica/bin/vsql -U dbadmin -F ',' -A -o timeselect_comp.csv -c 'select * from test.transaction_timeselect_comp;'
/opt/vertica/bin/vsql -U dbadmin -F ',' -A -o date_comp.csv -c 'select * from test.pos_transaction_number_date_comp;'
/opt/vertica/bin/vsql -U dbadmin -F ',' -A -o number_comp.csv -c 'select * from test.pos_transaction_number_comp;'

python /home/dbadmin/src/tests/integration/check_error.py ids_comp.csv timeselect_comp.csv date_comp.csv number_comp.csv
exit $?
