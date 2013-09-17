show user;
select * from global_name;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
set timing on;
set serveroutput on;
set linesize 160
exec dbms_output.enable(100000);
select 'start time: ' || systimestamp from dual;

declare
   mesg varchar2(4000);
begin
   mesg := null;
   create_storet_objects.main(mesg, '&1', '&2');
   dbms_output.put_line(mesg);
   if mesg like 'FAIL%' then
      raise_application_error(-20000, mesg);
   end if;
end;
/

select 'end time: ' || systimestamp from dual;
