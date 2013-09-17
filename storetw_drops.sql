show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'start time: ' || systimestamp from dual;

drop table fa_regular_result;
drop table fa_station;
drop table di_activity_matrix;
drop table di_activity_medium;
drop table di_characteristic;
drop table di_geo_county;
drop table di_geo_state;
drop table di_org;
drop table di_statn_types;
drop table lu_mad_hmethod;
drop table lu_mad_hdatum;
drop table lu_mad_vmethod;
drop table lu_mad_vdatum;
drop table mt_wh_config;

select 'end time: ' || systimestamp from dual;
