show user;
select * from global_name;
set timing on;
set serveroutput on;
whenever sqlerror exit failure rollback;
whenever oserror exit failure rollback;
select 'start time: ' || systimestamp from dual;

grant select on fa_regular_result to wqp_core;
grant select on fa_station to wqp_core;
grant select on di_activity_matrix to wqp_core;
grant select on di_activity_medium to wqp_core;
grant select on di_characteristic to wqp_core;
grant select on di_geo_county to wqp_core;
grant select on di_geo_state to wqp_core;
grant select on di_org to wqp_core;
grant select on lu_mad_hmethod to wqp_core;
grant select on lu_mad_hdatum to wqp_core;
grant select on lu_mad_vmethod to wqp_core;
grant select on lu_mad_vdatum to wqp_core;

grant select on regular_result_project to wqp_core;
grant select on biological_result_project to wqp_core;
grant select on habitat_result_project to wqp_core;
grant select on metric_result_project to wqp_core;
grant select on di_project to wqp_core;
grant select on di_activity_intent to wqp_core;
grant select on di_community_sampled to wqp_core;
grant select on fa_blob to wqp_core;

select 'end time: ' || systimestamp from dual;
