show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'start time: ' || systimestamp from dual;

grant select on fa_regular_result to storetmodern;
grant select on fa_station to storetmodern;
grant select on di_activity_matrix to storetmodern;
grant select on di_activity_medium to storetmodern;
grant select on di_characteristic to storetmodern;
grant select on di_geo_county to storetmodern;
grant select on di_geo_state to storetmodern;
grant select on di_org to storetmodern;
grant select on di_statn_types to storetmodern;
grant select on lu_mad_hmethod to storetmodern;
grant select on lu_mad_hdatum to storetmodern;
grant select on lu_mad_vmethod to storetmodern;
grant select on lu_mad_vdatum to storetmodern;
grant select on mt_wh_config to storetmodern;

select 'end time: ' || systimestamp from dual;
