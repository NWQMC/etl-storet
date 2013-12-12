create or replace package create_storet_objects
   authid definer
   as
   procedure main();
end create_storet_objects;
/

create or replace package body create_storet_objects
      /*-----------------------------------------------------------------------------------
        package create_storet_objects                                created by barry, 11/2011

        A lot of this package was borrowed from create_nad_objects()

        This package is run after 14 staging tables are loaded into a staging database
        ('storetw' on __stage) from the expdp files downloaded from EPA.  The big table,
        FA_REGULAR_RESULT, is an export of "changeable" rows (SOURCE_SYSTEM is not null)
        and those rows are combined with rows not subject to change.

        -----------------------------------------------------------------------------------*/
   as
   lf constant varchar(1) := chr(10);

   type cursor_type is ref cursor;

   procedure create_regular_result
   is
   begin

      dbms_output.put_line(systimestamp || ' creating regular_result...');

      execute immediate 'truncate table fa_regular_result'; 
      execute immediate q'!insert /*+ append nologging */ into fa_regular_result
      select
         fa_regular_result_no_source.pk_isn,
         fa_regular_result_no_source.organization_id,
         fa_regular_result_no_source.station_id,
         fa_regular_result_no_source.activity_start_date_time,
         fa_regular_result_no_source.act_start_time_zone,
         fa_regular_result_no_source.trip_id,
         di_characteristic.characteristic_group_type,
         di_characteristic.display_name characteristic_name,
         fa_regular_result_no_source.result_value, 
         nvl2(fa_regular_result_no_source.result_value, fa_regular_result_no_source.result_unit, null) result_unit,
         nvl2(fa_regular_result_no_source.result_value, null, fa_regular_result_no_source.result_value_text) result_value_text,
         fa_regular_result_no_source.sample_fraction_type,
         fa_regular_result_no_source.result_value_type,
         fa_regular_result_no_source.statistic_type,
         fa_regular_result_no_source.result_value_status,
         fa_regular_result_no_source.weight_basis_type,
         fa_regular_result_no_source.temperature_basis_level,
         fa_regular_result_no_source.duration_basis,
         trim(fa_regular_result_no_source.analytical_procedure_source) analytical_procedure_source,
         fa_regular_result_no_source.analytical_procedure_id,
         fa_regular_result_no_source.lab_name,
         fa_regular_result_no_source.analysis_date_time,
         fa_regular_result_no_source.detection_limit,
         fa_regular_result_no_source.detection_limit_unit,
         fa_regular_result_no_source.detection_limit_description,
         fa_regular_result_no_source.lab_remark,
         fa_regular_result_no_source.particle_size,
         fa_regular_result_no_source.precision,
         di_activity_medium.activity_medium,
         fa_regular_result_no_source.fk_station,
         fa_regular_result_no_source.fk_org,
         fa_regular_result_no_source.fk_geo_county,
         fa_regular_result_no_source.fk_geo_state,
         fa_regular_result_no_source.fk_act_medium,
         fa_regular_result_no_source.fk_act_matrix,
         fa_regular_result_no_source.fk_char,
         fa_regular_result_no_source.fk_unit_conversion,
         fa_regular_result_no_source.activity_id,
         fa_regular_result_no_source.replicate_number,
         fa_regular_result_no_source.activity_type,
         fa_regular_result_no_source.activity_stop_date_time,
         fa_regular_result_no_source.act_stop_time_zone,
         fa_regular_result_no_source.activity_depth,
         fa_regular_result_no_source.activity_depth_unit,
         fa_regular_result_no_source.activity_upper_depth,
         fa_regular_result_no_source.activity_lower_depth,
         fa_regular_result_no_source.upr_lwr_depth_unit,
         nvl(fa_regular_result_no_source.field_procedure_id, 'USEPA') field_procedure_id,
         nvl(fa_regular_result_no_source.field_gear_id, 'Unknown') field_gear_id,
         fa_regular_result_no_source.result_comment,
         fa_regular_result_no_source.itis_number,
         fa_regular_result_no_source.activity_comment,
         fa_regular_result_no_source.activity_depth_ref_point,
         nvl(fa_regular_result_no_source.project_id, 'EPA') project_id,
         fa_regular_result_no_source.result_meas_qual_code,
         fa_regular_result_no_source.activity_cond_org_text,
         fa_regular_result_no_source.result_depth_meas_value,
         nvl2(fa_regular_result_no_source.result_depth_meas_value, fa_regular_result_no_source.result_depth_meas_unit_code, null) result_depth_meas_unit_code,
         nvl2(fa_regular_result_no_source.result_depth_meas_value, fa_regular_result_no_source.result_depth_alt_ref_pt_txt, null) result_depth_alt_ref_pt_txt,
         fa_regular_result_no_source.source_system,
         fa_regular_result_no_source.lab_samp_prp_method_id,
         fa_regular_result_no_source.lab_samp_prp_start_date_time,
         di_activity_matrix.matrix_name,
         nvl2(fa_regular_result_no_source.activity_upper_depth, fa_regular_result_no_source.upr_lwr_depth_unit, null) activity_upper_depth_unit,
         nvl2(fa_regular_result_no_source.activity_lower_depth, fa_regular_result_no_source.upr_lwr_depth_unit, null) activity_lower_depth_unit,
         nvl2(coalesce(fa_regular_result_no_source.activity_upper_depth, fa_regular_result_no_source.activity_lower_depth), fa_regular_result_no_source.activity_depth_ref_point, null) activity_uprlwr_depth_ref_pt,
         regexp_replace(fa_regular_result_no_source.detection_limit, '[[:space:]].*') as myql,
         nvl2(fa_regular_result_no_source.detection_limit, fa_regular_result_no_source.detection_limit_unit, null) as myqlunits,
         nvl2(fa_regular_result_no_source.detection_limit, fa_regular_result_no_source.detection_limit_description, null) as myqldesc
      from fa_regular_result_no_source
           left join di_activity_matrix
             on fk_act_matrix = di_activity_matrix.pk_isn
           left join di_characteristic
             on fk_char = di_characteristic.pk_isn
           left join di_activity_medium
             on fk_act_medium = di_activity_medium.pk_isn!';

     commit;

     execute immediate q'!insert /*+ append nologging */ into fa_regular_result
       select
         fa_regular_result.pk_isn,
         fa_regular_result.organization_id,
         fa_regular_result.station_id,
         fa_regular_result.activity_start_date_time,
         fa_regular_result.act_start_time_zone,
         fa_regular_result.trip_id,
         di_characteristic.characteristic_group_type,
         di_characteristic.display_name characteristic_name,
         fa_regular_result.result_value,
         nvl2(fa_regular_result.result_value, fa_regular_result.result_unit, null) result_unit,
         nvl2(fa_regular_result.result_value, null, fa_regular_result.result_value_text) result_value_text,
         fa_regular_result.sample_fraction_type,
         fa_regular_result.result_value_type,
         fa_regular_result.statistic_type,
         fa_regular_result.result_value_status,
         fa_regular_result.weight_basis_type,
         fa_regular_result.temperature_basis_level,
         fa_regular_result.duration_basis,
         trim(fa_regular_result.analytical_procedure_source) analytical_procedure_source,
         fa_regular_result.analytical_procedure_id,
         fa_regular_result.lab_name,
         fa_regular_result.analysis_date_time,
         fa_regular_result.detection_limit,
         fa_regular_result.detection_limit_unit,
         fa_regular_result.detection_limit_description,
         fa_regular_result.lab_remark,
         fa_regular_result.particle_size,
         fa_regular_result.precision,
         di_activity_medium.activity_medium,
         fa_regular_result.fk_station,
         fa_regular_result.fk_org,
         fa_regular_result.fk_geo_county,
         fa_regular_result.fk_geo_state,
         fa_regular_result.fk_act_medium,
         fa_regular_result.fk_act_matrix,
         fa_regular_result.fk_char,
         fa_regular_result.fk_unit_conversion,
         fa_regular_result.activity_id,
         fa_regular_result.replicate_number,
         fa_regular_result.activity_type,
         fa_regular_result.activity_stop_date_time,
         fa_regular_result.act_stop_time_zone,
         fa_regular_result.activity_depth,
         fa_regular_result.activity_depth_unit,
         fa_regular_result.activity_upper_depth,
         fa_regular_result.activity_lower_depth,
         fa_regular_result.upr_lwr_depth_unit,
         nvl(fa_regular_result.field_procedure_id, 'USEPA') field_procedure_id,
         nvl(fa_regular_result.field_gear_id, 'Unknown') field_gear_id,
         fa_regular_result.result_comment,
         fa_regular_result.itis_number,
         fa_regular_result.activity_comment,
         fa_regular_result.activity_depth_ref_point,
         nvl(fa_regular_result.project_id, 'EPA') project_id,
         fa_regular_result.result_meas_qual_code,
         fa_regular_result.activity_cond_org_text,
         fa_regular_result.result_depth_meas_value,
         nvl2(fa_regular_result.result_depth_meas_value, fa_regular_result.result_depth_meas_unit_code, null) result_depth_meas_unit_code,
         nvl2(fa_regular_result.result_depth_meas_value, fa_regular_result.result_depth_alt_ref_pt_txt, null) result_depth_alt_ref_pt_txt,
         fa_regular_result.source_system,
         fa_regular_result.lab_samp_prp_method_id,
         fa_regular_result.lab_samp_prp_start_date_time,
         di_activity_matrix.matrix_name,
         nvl2(fa_regular_result.activity_upper_depth, fa_regular_result.upr_lwr_depth_unit, null) activity_upper_depth_unit,
         nvl2(fa_regular_result.activity_lower_depth, fa_regular_result.upr_lwr_depth_unit, null) activity_lower_depth_unit,
         nvl2(coalesce(fa_regular_result.activity_upper_depth, fa_regular_result.activity_lower_depth), fa_regular_result.activity_depth_ref_point, null) activity_uprlwr_depth_ref_pt,
         regexp_replace(fa_regular_result.detection_limit, '[[:space:]].*') myql,
         nvl2(fa_regular_result.detection_limit, fa_regular_result.detection_limit_unit, null) myqlunits,
         nvl2(fa_regular_result.detection_limit, fa_regular_result.detection_limit_description, null) myqldesc
      from storetw_fa_regular_result
           left join di_activity_matrix
             on fk_act_matrix = di_activity_matrix.pk_isn
           left join di_characteristic
             on fk_char = di_characteristic.pk_isn
           left join di_activity_medium
             on fk_act_medium = di_activity_medium.pk_isn!';
         
      commit;
   end create_regular_result;

   procedure create_station
   is
   begin

      dbms_output.put_line(systimestamp || ' creating station...');

      execute immediate 'truncate table fa_station';
      execute immediate q'!insert /*+ append nologging */ into fa_station
      select
         storetw_fa_station.organization_id || '-' || storetw_fa_station.station_id station_id,
         trim(storetw_fa_station.station_name) station_name,
         storetw_fa_station.organization_id,
         storetw_fa_station.latitude,
         storetw_fa_station.longitude,
         regexp_substr(storetw_fa_station.map_scale, '[[:digit:]]+$') map_scale,
         storetw_fa_station.elevation,
         storetw_fa_station.generated_huc,
         storetw_fa_station.station_group_type,
         trim(storetw_fa_station.description_text) description_text,
         storetw_fa_station.project_id,
         storetw_fa_station.fk_geo_state,
         storetw_fa_station.fk_geo_county,
         storetw_fa_station.fk_mad_hmethod,
         storetw_fa_station.fk_mad_hdatum,
         storetw_fa_station.fk_mad_vmethod,
         storetw_fa_station.fk_mad_vdatum,
         storetw_fa_station.fk_org,
         storetw_fa_station.fk_primary_type,
         storetw_fa_station.source_system,
         storetw_fa_station.geom,
         storetw_fa_station.pk_isn,
         nvl2(storetw_fa_station.elevation, nvl(storetw_fa_station.elevation_unit, 'ft'), null) elevation_unit,
         storetw_fa_station.huctwelvedigitcode,
         storetw_fa_station.wgs84_latitude,
         storetw_fa_station.wgs84_longitude,
         di_org.organization_name,
         di_geo_state.country_code country_cd,
         di_geo_state.country_name country_name,
         rtrim(di_geo_state.fips_state_code) state_cd,
         di_geo_state.state_name,
         di_geo_county.fips_county_code county_cd,
         di_geo_county.county_name,
         nvl(lu_mad_hmethod.geopositioning_method, 'Unknown') geopositioning_method,
         nvl(rtrim(lu_mad_hdatum.id_code), 'Unknown') hdatum_id_code,
         regexp_substr(storetw_fa_station.elevation, '^[[:digit:]]+') elevation_value,
         nvl2(storetw_fa_station.elevation, lu_mad_vmethod.elevation_method, null) elevation_method,
         nvl2(storetw_fa_station.elevation, nvl(lu_mad_vdatum.id_code, 'Unknown'), null) vdatum_id_code
      from storetw_fa_station
           left join di_org
             on fk_org = di_org.pk_isn
           left join di_geo_state
             on fk_geo_state = di_geo_state.pk_isn
           left join di_geo_county
             on fk_geo_county = di_geo_county.pk_isn
           left join lu_mad_hmethod
             on fk_mad_hmethod = lu_mad_hmethod.pk_isn
           left join lu_mad_hdatum
             on fk_mad_hdatum = lu_mad_hdatum.pk_isn
           left join lu_mad_vmethod
             on fk_mad_vmethod = lu_mad_vmethod.pk_isn
           left join lu_mad_vdatum
             on fk_mad_vdatum = lu_mad_vdatum.pk_isn';

       commit;

   end create_station;


  procedure create_summaries
   is
   begin

      dbms_output.put_line(systimestamp || ' creating storet_station_sum...');

      execute immediate 'truncate table storet_station_sum';
      execute immediate 'insert /*+ append nologging */ into storet_station_sum
         select
            a.pk_isn,
            station_id,
            geom,
            country_cd,
            state_cd,
            county_cd,
            station_group_type,
            a.organization_id,
            organization_name,
            generated_huc,
            station_name,
            fk_primary_type,
            description_text,
            nvl(result_count, 0) result_count
         from
            fa_station a
            left join (select fk_station, count(*) result_count from fa_regular_result group by fk_station) e
              on a.pk_isn = e.fk_station';
      commit;

      dbms_output.put_line(systimestamp || ' creating storet_result_sum...');

      execute immediate 'truncate table storet_result_sum';
      execute immediate 'insert /*+ append nologging */ into storet_result_sum
         select /*+ full(a) parallel(a, 4) full(b) parallel(b, 4) use_hash(a) use_hash(b) */
            b.fk_station,
            a.station_id,
            a.country_cd,
            a.state_cd,
            a.county_cd,
            a.station_group_type,
            a.organization_id,
            a.generated_huc,
            b.activity_medium,
            b.characteristic_group_type,
            b.characteristic_name,
            b.activity_start_date_time,
            b.result_count
         from
            storet_station_sum a,
            (select /*+ parallel(4) */
                fk_station,
                activity_medium,
                characteristic_group_type,
                characteristic_name,
                trunc(activity_start_date_time) activity_start_date_time,
                count(*) result_count
             from
                fa_regular_result
             group by
                fk_station,
                activity_medium,
                characteristic_group_type,
                characteristic_name,
                trunc(activity_start_date_time)) b
         where
             b.fk_station = a.pk_isn(+)';
      commit;

      dbms_output.put_line(systimestamp || ' creating storet_result_nr_sum...');

      execute immediate 'truncate table storet_result_nr_sum';
      execute immediate 'insert /*+ append nologging */ into storet_result_nr_sum
      select /*+ full(a) parallel(a, 4) */
            fk_station,
            activity_medium,
            characteristic_group_type,
            characteristic_name,
            activity_start_date_time,
            sum(result_count) result_count
         from
            storet_result_sum a
         group by
            fk_station,
            activity_medium,
            characteristic_group_type,
            characteristic_name,
            activity_start_date_time
         order by
            fk_station,
            activity_medium,
            characteristic_group_type,
            characteristic_name';
      commit;

      dbms_output.put_line(systimestamp || ' creating storet_lctn_loc...');

	  execute immediate 'truncate table storet_lctn_loc';
      execute immediate 'insert /*+ append nologging */ into storet_lctn_loc
      select /*+ parallel(4) */ distinct
             country_cd,
             state_cd state_fips,
             organization_id,
             organization_name
       from fa_station';
      commit;

   end create_summaries;

   procedure create_lookups
   is
   begin

      dbms_output.put_line(systimestamp || ' creating storet_sum');
      execute immediate 'truncate table storet_sum';
      execute immediate q'!insert /*+ append nologging */ into storet_sum
      select
         cast(trim(station.state_cd) as varchar2(2)) fips_state_code,
         cast(trim(station.county_cd) as varchar2(3)) fips_county_code,
         cast(trim(station.station_group_type) as varchar2(30)) site_type,
         cast(trim(station.generated_huc) as varchar2(8)) huc8,
         cast(trim(station.huctwelvedigitcode) as varchar2(12)) huc12,
         min(case when activity_start_date_time between to_date('01-JAN-1875', 'DD-MON-YYYY') and sysdate then activity_start_date_time else null end) min_date,
         max(case when activity_start_date_time between to_date('01-JAN-1875', 'DD-MON-YYYY') and sysdate then activity_start_date_time else null end) max_date,
         cast(count(distinct case when months_between(sysdate, activity_start_date_time) between 0 and 12 then activity_id || to_char(fk_station) || to_char(trunc(activity_start_date_time)) else null end) as number(7)) samples_past_12_months,
         cast(count(distinct case when months_between(sysdate, activity_start_date_time) between 0 and 60 then activity_id || to_char(fk_station) || to_char(trunc(activity_start_date_time)) else null end) as number(7)) samples_past_60_months,
         cast(count(distinct result.activity_id || to_char(fk_station) || to_char(trunc(activity_start_date_time))) as number(7)) samples_all_time,
         cast(sum(case when months_between(sysdate, activity_start_date_time) between 0 and 12 then 1 else 0 end) as number(7)) results_past_12_months,
         cast(sum(case when months_between(sysdate, activity_start_date_time) between 0 and 60 then 1 else 0 end) as number(7)) results_past_60_months,
         cast(count(*) as number(7)) results_all_time
      from /* TODO remove join*/
         fa_regular_result result,
         fa_station station
      where
         length(trim(station.state_cd  )) = 2  and
         length(trim(station.county_cd)) = 3  and
         trim(station.state_cd) between '01' and '56' and
         result.fk_station     = station.pk_isn
      group by
         cast(trim(station.state_cd) as varchar2(2)) ,
         cast(trim(station.county_cd) as varchar2(3)) ,
         cast(trim(station.station_group_type) as varchar2(30)) ,
         cast(trim(station.generated_huc) as varchar2(8)) ,
         cast(trim(station.huctwelvedigitcode) as varchar2(12))!';
      commit;

      dbms_output.put_line(systimestamp || ' creating characteristicname');
      execute immediate 'truncate table characteristicname';
      execute immediate 'insert /*+ append nologging */ into characteristicname
      select code_value,
             null description,
             rownum sort_order
        from (select distinct characteristic_name code_value
                from fa_regular_result
               where characteristic_name is not null
                  order by 1)';
      commit;
 
      dbms_output.put_line(systimestamp || ' creating characteristictype');
      execute immediate 'truncate table characteristictype';
      execute immediate 'insert /*+ append nologging */ into characteristictype
      select code_value,
             null description,
             rownum sort_order
        from (select distinct characteristic_group_type code_value
                from fa_regular_result
               where characteristic_group_type is not null
                  order by 1)';

      dbms_output.put_line(systimestamp || ' creating country');
      execute immediate 'truncate table country';
      execute immediate 'insert /*+ append nologging */ into country
      select code_value,
             description,
             rownum sort_order
        from (select distinct country_cd code_value,
                              country_name description
                from fa_station
                  order by country_name desc)';

      dbms_output.put_line(systimestamp || ' creating county');
      execute immediate 'truncate table county';
      execute immediate q'!insert /*+ append nologging */ into county
      select code_value,
             description,
             country_cd,
             state_cd,
             rownum sort_order
        from (select di_geo_state.country_code || ':' || rtrim(di_geo_state.fips_state_code) || ':' || di_geo_county.fips_county_code code_value,
                     di_geo_state.country_code || ', ' || di_geo_state.state_name || ', ' || di_geo_county.county_name description,
                     di_geo_state.country_code country_cd,
                     rtrim(di_geo_state.fips_state_code) state_cd
                from di_geo_state
                     join di_geo_county
                       on di_geo_state.pk_isn = di_geo_county.fk_geo_state
               where exists (select null
                               from fa_station
                              where di_geo_county.pk_isn = fk_geo_county)
                  order by country_code desc,
                           fips_state_code,
                           fips_county_code)';

      dbms_output.put_line(systimestamp || ' creating organization');
      execute immediate 'truncate table organization';
      execute immediate 'insert /*+ append nologging */ into organization
       select code_value,
              description,
              rownum sort_order
         from (select distinct organization_id code_value,
                               organization_name description
                 from fa_station
                    order by 1)';

      dbms_output.put_line(systimestamp || ' creating samplemedia');
      execute immediate 'truncate table samplemedia';
      execute immediate 'insert /*+ append nologging */ into samplemedia
       select code_value,
              null description,
              rownum sort_order
         from (select distinct activity_medium as code_value
                 from fa_regular_result
                where fk_act_medium is not null
                   order by activity_medium)';
            
      dbms_output.put_line(systimestamp || ' creating sitetype');
      execute immediate 'truncate table sitetype';
      execute immediate 'insert /*+ append nologging */ into sitetype
       select code_value,
              null description,
              rownum sort_order
         from (select distinct station_group_type code_value
                 from fa_station
                   order by 1)';

      dbms_output.put_line(systimestamp || ' creating state');
      execute immediate 'truncate table state';
      execute immediate q'!insert /*+ append nologging */ into state
       select code_value,
              description_with_country,
              description_with_out_country,
              country_cd,
              rownum sort_order
         from (select distinct country_cd || ':' || state_cd code_value,
                               country_cd || ', ' || state_name description_with_country,
                               state_name description_with_out_country,
                               country_cd,
                               state_cd
                 from fa_station
                   order by country_cd desc,
                            state_cd)!';
      commit;

   end create_lookups;

  procedure main() is
   begin
      dbms_output.enable(100000);

      dbms_output.put_line(systimestamp || ' started storet table transformation.');
      create_regular_result;
      create_station;
      create_lookups;
      create_summaries;

      dbms_output.put_line(systimestamp || ' completed. (success)');

   end main;
end create_storet_objects;
/
