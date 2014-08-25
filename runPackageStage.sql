show user;
select * from global_name;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
set timing on;
set serveroutput on;
set linesize 160
exec dbms_output.enable(1000000);
select 'start time: ' || systimestamp from dual;

begin
   create_storet_objects.main;
end;
/

select 'end time: ' || systimestamp from dual;
