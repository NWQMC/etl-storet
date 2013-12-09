create or replace package create_storet_objects
   authid definer
   as
   procedure main(mesg in out varchar2, success_notify in varchar2, failure_notify in varchar2);
end create_storet_objects;
/

create or replace package body create_storet_objects
      /*-----------------------------------------------------------------------------------
        package create_storet_objects                                created by barry, 11/2011

        A lot of this package was borrowed from create_nad_objects()

        This package is run after 14 staging tables are loaded into a staging database
        ('barry' on pubsdb) from the expdp files downloaded from EPA.  Most of the tables
        are copied "as is" into WIDW.  The big table, FA_REGULAR_RESULT, is an export of
        "changeable" rows (SOURCE_SYSTEM is not null) and those rows are combined with
        rows not subject to change.

        FA_REGULAR_RESULT
        FA_STATION
        DI_ACTIVITY_MATRIX
        DI_ACTIVITY_MEDIUM
        DI_CHARACTERISTIC
        DI_GEO_COUNTY
        DI_GEO_STATE
        DI_ORG
        DI_STATN_TYPES
        LU_MAD_HMETHOD
        LU_MAD_HDATUM
        LU_MAD_VMETHOD
        LU_MAD_VDATUM
        MT_WH_CONFIG

        -----------------------------------------------------------------------------------*/
   as
   lf constant varchar(1) := chr(10);

   message varchar2(4000);
   email_text varchar2(32000);

   
   type cursor_type is ref cursor;

   procedure append_email_text(addition in varchar2)
   is
      addition_with_time varchar2(4000);
   begin

      addition_with_time := to_char(sysdate, 'YYYY.MM.DD HH24:MI:SS ') || addition;
      dbms_output.put_line(addition_with_time);
      if nvl(length(email_text), 0) + nvl(length(addition_with_time), 0) + nvl(length(lf), 0) < 32000 then
         email_text := email_text || addition_with_time || lf;
      end if;

   exception
      when others then
         if message is null then
            message := 'failed to append to email message';
         end if;
   end append_email_text;

   procedure create_regular_result
   is
   begin

      append_email_text('creating regular_result...');

      execute immediate 'truncate table fa_regular_result'; 
      execute immediate 'insert /*+ append nologging */ into fa_regular_result
        (
         PK_ISN,
         ORGANIZATION_ID,
         STATION_ID,
         ACTIVITY_START_DATE_TIME,
         ACT_START_TIME_ZONE,
         TRIP_ID,
         CHARACTERISTIC_GROUP_TYPE,
         CHARACTERISTIC_NAME,
         RESULT_VALUE,
         RESULT_UNIT,
         RESULT_VALUE_TEXT,
         SAMPLE_FRACTION_TYPE,
         RESULT_VALUE_TYPE,
         STATISTIC_TYPE,
         RESULT_VALUE_STATUS,
         WEIGHT_BASIS_TYPE,
         TEMPERATURE_BASIS_LEVEL,
         DURATION_BASIS,
         ANALYTICAL_PROCEDURE_SOURCE,
         ANALYTICAL_PROCEDURE_ID,
         LAB_NAME,
         ANALYSIS_DATE_TIME,
         DETECTION_LIMIT,
         DETECTION_LIMIT_UNIT,
         DETECTION_LIMIT_DESCRIPTION,
         LAB_REMARK,
         PARTICLE_SIZE,
         PRECISION,
         ACTIVITY_MEDIUM,
         FK_STATION,
         FK_ORG,
         FK_GEO_COUNTY,
         FK_GEO_STATE,
         FK_ACT_MEDIUM,
         FK_ACT_MATRIX,
         FK_CHAR,
         FK_UNIT_CONVERSION,
         ACTIVITY_ID,
         REPLICATE_NUMBER,
         ACTIVITY_TYPE,
         ACTIVITY_STOP_DATE_TIME,
         ACT_STOP_TIME_ZONE,
         ACTIVITY_DEPTH,
         ACTIVITY_DEPTH_UNIT,
         ACTIVITY_UPPER_DEPTH,
         ACTIVITY_LOWER_DEPTH,
         UPR_LWR_DEPTH_UNIT,
         FIELD_PROCEDURE_ID,
         FIELD_GEAR_ID,
         RESULT_COMMENT,
         ITIS_NUMBER,
         ACTIVITY_COMMENT,
         ACTIVITY_DEPTH_REF_POINT,
         PROJECT_ID,
         RESULT_MEAS_QUAL_CODE,
         ACTIVITY_COND_ORG_TEXT,
         RESULT_DEPTH_MEAS_VALUE,
         RESULT_DEPTH_MEAS_UNIT_CODE,
         RESULT_DEPTH_ALT_REF_PT_TXT,
         SOURCE_SYSTEM,
         LAB_SAMP_PRP_METHOD_ID,
         LAB_SAMP_PRP_START_DATE_TIME
       )
       SELECT
         PK_ISN,
         ORGANIZATION_ID,
         STATION_ID,
         ACTIVITY_START_DATE_TIME,
         ACT_START_TIME_ZONE,
         TRIP_ID,
         CHARACTERISTIC_GROUP_TYPE,
         CHARACTERISTIC_NAME,
         RESULT_VALUE,
         RESULT_UNIT,
         RESULT_VALUE_TEXT,
         SAMPLE_FRACTION_TYPE,
         RESULT_VALUE_TYPE,
         STATISTIC_TYPE,
         RESULT_VALUE_STATUS,
         WEIGHT_BASIS_TYPE,
         TEMPERATURE_BASIS_LEVEL,
         DURATION_BASIS,
         ANALYTICAL_PROCEDURE_SOURCE,
         ANALYTICAL_PROCEDURE_ID,
         LAB_NAME,
         ANALYSIS_DATE_TIME,
         DETECTION_LIMIT,
         DETECTION_LIMIT_UNIT,
         DETECTION_LIMIT_DESCRIPTION,
         LAB_REMARK,
         PARTICLE_SIZE,
         PRECISION,
         ACTIVITY_MEDIUM,
         FK_STATION,
         FK_ORG,
         FK_GEO_COUNTY,
         FK_GEO_STATE,
         FK_ACT_MEDIUM,
         FK_ACT_MATRIX,
         FK_CHAR,
         FK_UNIT_CONVERSION,
         ACTIVITY_ID,
         REPLICATE_NUMBER,
         ACTIVITY_TYPE,
         ACTIVITY_STOP_DATE_TIME,
         ACT_STOP_TIME_ZONE,
         ACTIVITY_DEPTH,
         ACTIVITY_DEPTH_UNIT,
         ACTIVITY_UPPER_DEPTH,
         ACTIVITY_LOWER_DEPTH,
         UPR_LWR_DEPTH_UNIT,
         FIELD_PROCEDURE_ID,
         FIELD_GEAR_ID,
         RESULT_COMMENT,
         ITIS_NUMBER,
         ACTIVITY_COMMENT,
         ACTIVITY_DEPTH_REF_POINT,
         PROJECT_ID,
         RESULT_MEAS_QUAL_CODE,
         ACTIVITY_COND_ORG_TEXT,
         RESULT_DEPTH_MEAS_VALUE,
         RESULT_DEPTH_MEAS_UNIT_CODE,
         RESULT_DEPTH_ALT_REF_PT_TXT,
         SOURCE_SYSTEM,
         LAB_SAMP_PRP_METHOD_ID,
         LAB_SAMP_PRP_START_DATE_TIME
      FROM
         storetw.FA_REGULAR_RESULT';

     commit;

     execute immediate 'insert /*+ append nologging */ into fa_regular_result' || suffix || '
      select /*+  full(FA_REGULAR_RESULT_NO_SOURCE) parallel(FA_REGULAR_RESULT_NO_SOURCE, 4)*/
         PK_ISN,
         ORGANIZATION_ID,
         STATION_ID,
         ACTIVITY_START_DATE_TIME,
         ACT_START_TIME_ZONE,
         TRIP_ID,
         CHARACTERISTIC_GROUP_TYPE,
         CHARACTERISTIC_NAME,
         RESULT_VALUE,
         RESULT_UNIT,
         RESULT_VALUE_TEXT,
         SAMPLE_FRACTION_TYPE,
         RESULT_VALUE_TYPE,
         STATISTIC_TYPE,
         RESULT_VALUE_STATUS,
         WEIGHT_BASIS_TYPE,
         TEMPERATURE_BASIS_LEVEL,
         DURATION_BASIS,
         ANALYTICAL_PROCEDURE_SOURCE,
         ANALYTICAL_PROCEDURE_ID,
         LAB_NAME,
         ANALYSIS_DATE_TIME,
         DETECTION_LIMIT,
         DETECTION_LIMIT_UNIT,
         DETECTION_LIMIT_DESCRIPTION,
         LAB_REMARK,
         PARTICLE_SIZE,
         PRECISION,
         ACTIVITY_MEDIUM,
         FK_STATION,
         FK_ORG,
         FK_GEO_COUNTY,
         FK_GEO_STATE,
         FK_ACT_MEDIUM,
         FK_ACT_MATRIX,
         FK_CHAR,
         FK_UNIT_CONVERSION,
         ACTIVITY_ID,
         REPLICATE_NUMBER,
         ACTIVITY_TYPE,
         ACTIVITY_STOP_DATE_TIME,
         ACT_STOP_TIME_ZONE,
         ACTIVITY_DEPTH,
         ACTIVITY_DEPTH_UNIT,
         ACTIVITY_UPPER_DEPTH,
         ACTIVITY_LOWER_DEPTH,
         UPR_LWR_DEPTH_UNIT,
         FIELD_PROCEDURE_ID,
         FIELD_GEAR_ID,
         RESULT_COMMENT,
         ITIS_NUMBER,
         ACTIVITY_COMMENT,
         ACTIVITY_DEPTH_REF_POINT,
         PROJECT_ID,
         RESULT_MEAS_QUAL_CODE,
         ACTIVITY_COND_ORG_TEXT,
         RESULT_DEPTH_MEAS_VALUE,
         RESULT_DEPTH_MEAS_UNIT_CODE,
         RESULT_DEPTH_ALT_REF_PT_TXT,
         SOURCE_SYSTEM,
         LAB_SAMP_PRP_METHOD_ID,
         LAB_SAMP_PRP_START_DATE_TIME
      FROM
         FA_REGULAR_RESULT_NO_SOURCE';
         
      commit;
     exception
      when others then
         message := 'FAIL to create FA_REGULAR_RESULT: ' || SQLERRM;
         append_email_text(message);
   end create_regular_result;

   procedure create_station
   is
   begin

      append_email_text('creating station...');

      execute immediate 'truncate table fa_station';
      execute immediate 'insert /*+ append nologging */ into fa_station
      select
         STATION_ID,
         STATION_NAME,
         ORGANIZATION_ID,
         LATITUDE,
         LONGITUDE,
         MAP_SCALE,
         ELEVATION,
         GENERATED_HUC,
         STATION_GROUP_TYPE,
         DESCRIPTION_TEXT,
         PROJECT_ID,
         FK_GEO_STATE,
         FK_GEO_COUNTY,
         FK_MAD_HMETHOD,
         FK_MAD_HDATUM,
         FK_MAD_VMETHOD,
         FK_MAD_VDATUM,
         FK_ORG,
         FK_PRIMARY_TYPE,
         SOURCE_SYSTEM,
         GEOM,
         PK_ISN,
         ELEVATION_UNIT,
         HUCTWELVEDIGITCODE,
         WGS84_LATITUDE,
         WGS84_LONGITUDE
      FROM
         storetw.FA_STATION';

      commit;

   exception
      when others then
         message := 'FAIL to create FA_STATION: ' || SQLERRM;
         append_email_text(message);
   end create_station;


  procedure create_summaries
   is
   begin

      append_email_text('creating storet_station_sum...');

      execute immediate 'truncate table storet_station_sum';
      execute immediate 'insert /*+ append nologging */ into storet_station_sum
         select /*+ full(a) parallel(a, 4) full(b) parallel(b, 4) full(c) parallel(c, 4) full(d) parallel(d, 4) use_hash(a) use_hash(b) use_hash(c) use_hash(d) */
            a.pk_isn,
            a.organization_id || ''-'' || station_id station_id,
            geom,
            country_code,
            fips_state_code,
            fips_county_code,
            station_group_type,
            a.organization_id,
            organization_name,
            generated_huc,
            station_name,
            fk_primary_type,
            description_text,
            cast(nvl(result_count, 0) as number(9)) result_count
         from
            fa_station a
            left join di_geo_state b
              on a.fk_geo_state  = b.pk_isn
            left join di_geo_county c
              on a.fk_geo_county = c.pk_isn
            left join di_org d
              on a.fk_org = d.pk_isn
            left join (select fk_station, count(*) result_count from fa_regular_result group by fk_station) e
              on a.pk_isn = e.fk_station
         order by
            country_code,
            fips_state_code,
            fips_county_code,
            station_group_type';
      commit;

      append_email_text('creating storet_result_sum...');

      execute immediate 'truncate table storet_result_sum';
      execute immediate 'insert /*+ append nologging */ into storet_result_sum
         select /*+ full(a) parallel(a, 4) full(b) parallel(b, 4) use_hash(a) use_hash(b) */
            b.fk_station,
            a.station_id,
            a.country_code,
            a.fips_state_code,
            a.fips_county_code,
            a.station_group_type,
            a.organization_id,
            a.generated_huc,
            b.activity_medium,
            b.characteristic_group_type,
            b.display_name,
            b.activity_start_date_time,
            b.result_count
         from
            storet_station_sum a,
            (select /*+ full(r) parallel(r, 4) full(m) parallel(m, 4) full(z) parallel(z, 4) use_hash(r) use_hash(m) use_hash(z) */
                r.fk_station,
                m.activity_medium,
                z.characteristic_group_type,
                z.display_name,
                trunc(r.activity_start_date_time) activity_start_date_time,
                cast(count(*) as number(9)) result_count
             from
                fa_regular_result r,
                di_activity_medium m,
                di_characteristic z
             where
                r.fk_act_medium = m.pk_isn(+) and
                r.fk_char       = z.pk_isn(+)
             group by
                r.fk_station,
                m.activity_medium,
                z.characteristic_group_type,
                z.display_name,
                trunc(r.activity_start_date_time)) b
         where
             b.fk_station = a.pk_isn(+)
         order by
             fk_station,
             station_id,
             activity_medium,
             characteristic_group_type,
             display_name';
      commit;

      append_email_text('creating storet_result_ct_sum...');

      execute immediate 'truncate table storet_result_ct_sum';
      execute immediate 'insert /*+ append nologging */ into storet_result_ct_sum
        select /*+ full(a) parallel(a, 4) */
            fk_station,
            station_id,
            country_code,
            fips_state_code,
            fips_county_code,
            station_group_type,
            organization_id,
            generated_huc,
            activity_medium,
            characteristic_group_type,
            display_name,
            cast(sum(result_count) as number(9)) result_count
         from
            storet_result_sum a
         group by
            fk_station,
            station_id,
            country_code,
            fips_state_code,
            fips_county_code,
            station_group_type,
            organization_id,
            generated_huc,
            activity_medium,
            characteristic_group_type,
            display_name
         order by
            fk_station,
            station_id,
            activity_medium,
            characteristic_group_type,
            display_name';
      commit;

      execute immediate 'truncate table storet_result_nr_sum';
      execute immediate 'insert /*+ append nologging */ into storet_result_nr_sum
      select /*+ full(a) parallel(a, 4) */
            fk_station,
            activity_medium,
            characteristic_group_type,
            display_name,
            activity_start_date_time,
            cast(sum(result_count) as number(9)) result_count
         from
            storet_result_sum a
         group by
            fk_station,
            activity_medium,
            characteristic_group_type,
            display_name,
            activity_start_date_time
         order by
            fk_station,
            activity_medium,
            characteristic_group_type,
            display_name';
      commit;

      append_email_text('creating storet_lctn_loc...');

	  execute immediate 'truncate table storet_lctn_loc';
      execute immediate 'insert /*+ append nologging */ into storet_lctn_loc
      select /*+ parallel(4) */ distinct
             b.country_code country_cd,
             b.fips_state_code state_fips,
             c.organization_id,
             c.organization_name
       from fa_station a
            left join di_geo_state b
              on a.fk_geo_state  = b.pk_isn
            left join di_org c
              on a.organization_id = c.organization_id';
      commit;

   exception
      when others then
         message := 'FAIL to create summaries: ' || SQLERRM;
         append_email_text(message);
   end create_summaries;

   procedure create_lookups
   is
   begin

      append_email_text('creating lookups...');

     execute immediate 'truncate table di_activity_matrix';
     execute immediate 'insert /*+ append nologging */ into DI_ACTIVITY_MATRIX
      select
         PK_ISN,
         MATRIX_CODE,
         MATRIX_NAME,
         MATRIX_DESCRIPTION
      FROM
         storetw.DI_ACTIVITY_MATRIX';
      commit;

      execute immediate 'truncate table di_activity_medium';
      execute immediate 'insert /*+ append nologging */ into DI_ACTIVITY_MEDIUM
      select
         PK_ISN,
         ACTIVITY_MEDIUM
      FROM
         storetw.DI_ACTIVITY_MEDIUM';
      commit;

      execute immediate 'truncate table di_characteristic';
      execute immediate 'insert /*+ append nologging */ into DI_CHARACTERISTIC
      select
         PK_ISN,
         SEARCH_NAME,
         DISPLAY_NAME,
         CHARACTERISTIC_GROUP_TYPE,
         TYPE_CODE,
         REGISTRY_NAME,
         SRS_ID,
         CAS_NUMBER,
         ITIS_NUMBER,
         CHARTYPE,
         LAST_CHANGE_DATE,
         DESCRIPTION
      FROM
         storetw.DI_CHARACTERISTIC';
      commit;

     execute immediate 'truncate table di_geo_county';
      execute immediate 'insert /*+ append nologging */ into DI_GEO_COUNTY
      select
          PK_ISN,
          FK_GEO_STATE,
          FIPS_COUNTY_CODE,
          COUNTY_NAME
      FROM
         storetw.DI_GEO_COUNTY';
      commit;

     execute immediate 'truncate table di_geo_state';
      execute immediate 'insert /*+ append nologging */ into DI_GEO_STATE
      select
         PK_ISN,
         FIPS_STATE_CODE,
         STATE_POSTAL_CODE,
         STATE_NAME,
         COUNTRY_CODE,
         COUNTRY_NAME
      FROM
         storetw.DI_GEO_STATE';
      commit;

      execute immediate 'truncate table di_org';
      execute immediate 'insert /*+ append nologging */ into DI_ORG
      select
         PK_ISN,
         ORGANIZATION_ID,
         ORGANIZATION_NAME,
         ORGANIZATION_IS_NUMBER,
         ORGANIZATION_TYPE,
         ORGANIZATION_DESCRIPTION,
         PARENT_ORG,
         LAST_CHANGE_DATE
      FROM
         storetw.DI_ORG';
      commit;

     execute immediate 'truncate table di_statn_types';
      execute immediate 'insert /*+ append nologging */ into DI_STATN_TYPES
      select
         PK_ISN,
         PRIMARY_TYPE,
         SECONDARY_TYPE,
         SORT_ORDER,
         SGO_INDICATOR,
         STATION_GROUP_TYPE
      FROM
         storetw.DI_STATN_TYPES';
      commit;

     execute immediate 'truncate table lu_mad_hmethod';
      execute immediate 'insert /*+ append nologging */ into LU_MAD_HMETHOD
      select
         PK_ISN,
         ID_CODE,
         GEOPOSITIONING_METHOD
      FROM
         storetw.LU_MAD_HMETHOD';
      commit;

     execute immediate 'truncate table lu_mad_hdatum';
      execute immediate 'insert /*+ append nologging */ into LU_MAD_HDATUM
      select
         PK_ISN,
         ID_CODE,
         HORIZONTAL_DATUM
      FROM
         storetw.LU_MAD_HDATUM';
      commit;

     execute immediate 'truncate table lu_mad_vmethod';
      execute immediate 'insert /*+ append nologging */ into LU_MAD_VMETHOD
      select
         PK_ISN,
         ID_CODE,
         ELEVATION_METHOD
      FROM
         storetw.LU_MAD_VMETHOD';
      commit;

     execute immediate 'truncate table lu_mad_vdatum';
      execute immediate 'insert /*+ append nologging */ into LU_MAD_VDATUM
      select
         PK_ISN,
         ID_CODE,
         ELEVATION_DATUM
      FROM
         storetw.LU_MAD_VDATUM';
      commit;

      execute immediate 'truncate table mt_wh_config';
      execute immediate 'insert /*+ append nologging */ into MT_WH_CONFIG
      select
         PARAMETER_NAME,
         PARAMETER_VALUE,
         APPLICATION,
         DESCRIPTION
      FROM
         storetw.MT_WH_CONFIG';
      commit;

     execute immediate 'truncate table storet_sum';
      execute immediate 'insert /*+ append nologging */ into storet_sum
      select
         cast(trim(state.fips_state_code) as varchar2(2)) fips_state_code,
         cast(trim(county.fips_county_code) as varchar2(3)) fips_county_code,
         cast(trim(station.station_group_type) as varchar2(30)) site_type,
         /*   took out because multiple per station: cast(trim(result.characteristic_group_type) as varchar2(80)) characteristic_group,  */
         cast(trim(station.generated_huc) as varchar2(8)) as huc8,
         cast(trim(station.huctwelvedigitcode) as varchar2(12)) as huc12,
         min(case when activity_start_date_time between to_date(''01-JAN-1875'', ''DD-MON-YYYY'') and sysdate then activity_start_date_time else null end) min_date,
         max(case when activity_start_date_time between to_date(''01-JAN-1875'', ''DD-MON-YYYY'') and sysdate then activity_start_date_time else null end) max_date,
         /* instead of using "activity_id" for a sample id, we need to add stuff to make sure it only applies to one station on one day */
         cast(count(distinct case when months_between(sysdate, activity_start_date_time) between 0 and 12 then activity_id || to_char(fk_station) || to_char(trunc(activity_start_date_time)) else null end) as number(7)) samples_past_12_months,
         cast(count(distinct case when months_between(sysdate, activity_start_date_time) between 0 and 60 then activity_id || to_char(fk_station) || to_char(trunc(activity_start_date_time)) else null end) as number(7)) samples_past_60_months,
         cast(count(distinct result.activity_id || to_char(fk_station) || to_char(trunc(activity_start_date_time))) as number(7)) samples_all_time,
         cast(sum(case when months_between(sysdate, activity_start_date_time) between 0 and 12 then 1 else 0 end) as number(7)) as results_past_12_months,
         cast(sum(case when months_between(sysdate, activity_start_date_time) between 0 and 60 then 1 else 0 end) as number(7)) as results_past_60_months,
         cast(count(*) as number(7)) as results_all_time
      from
         fa_regular_result result,
         fa_station station,
         di_geo_state state,
         di_geo_county county
      where
         length(trim(state.fips_state_code  )) = 2  and
         length(trim(county.fips_county_code)) = 3  and
         trim(state.fips_state_code) between ''01'' and ''56'' and
         result.fk_station     = station.pk_isn     and
         station.fk_geo_state  = state.pk_isn  (+)  and
         station.fk_geo_county = county.pk_isn (+)
      group by
         cast(trim(state.fips_state_code) as varchar2(2)) ,
         cast(trim(county.fips_county_code) as varchar2(3)) ,
         cast(trim(station.station_group_type) as varchar2(30)) ,
         cast(trim(station.generated_huc) as varchar2(8)) ,
         cast(trim(station.huctwelvedigitcode) as varchar2(12)) ';
      commit;

      execute immediate 'truncate table characteristicname';
      execute immediate 'insert /*+ append nologging */ into characteristicname
       select code_value,
              cast(null as varchar2(4000 char)) description,
              rownum sort_order
         from (select distinct characteristic_name code_value
                 from fa_regular_result
                where characteristic_name is not null
                   order by 1)';
      commit;
 
      execute immediate 'truncate table organization';
      execute immediate 'insert /*+ append nologging */ into organization
       select code_value,
              cast(null as varchar2(4000 char)) description,
              rownum sort_order
         from (select distinct organization_id code_value
                 from fa_station
                    order by 1)';
       commit;
           
      execute immediate 'truncate table samplemedia';
      execute immediate 'insert /*+ append nologging */ into samplemedia
       select code_value,
              cast(null as varchar2(4000 char)) description,
              rownum sort_order
         from (select distinct activity_medium as code_value
                 from di_activity_medium
                where activity_medium is not null and
                      pk_isn in (select fk_act_medium
                                   from fa_regular_result
                                  where fk_act_medium is not null)
                   order by activity_medium)';
      commit;

            
      execute immediate 'truncate table sitetype';
      execute immediate 'insert /*+ append nologging */ into sitetype
       select code_value,
              cast(null as varchar2(4000 char)) description,
              rownum sort_order
         from (select distinct station_group_type code_value
                 from fa_station
                   order by 1)';
      commit;

   exception
      when others then
         message := 'FAIL to create a lookup: ' || SQLERRM;
         append_email_text(message);
   end create_lookups;

  procedure main(mesg in out varchar2, success_notify in varchar2, failure_notify in varchar2) is
      email_subject varchar2(  80);
      email_notify  varchar2( 400);
      k int;
   begin
      message := null;
      dbms_output.enable(100000);

      append_email_text('started storet table transformation.');
      if message is null then create_regular_result; end if;
      if message is null then create_station;        end if;
      if message is null then create_lookups;        end if;
      if message is null then create_summaries;      end if;

      if message is null then
         append_email_text('completed. (success)');
         message := 'OK';
         email_subject := 'storet successful';
         email_text := email_subject || lf || lf || email_text || lf || 'have a nice day!' || lf || '-barry''s program';
         email_notify := success_notify;
      else
         append_email_text('completed. (failed)');
         dbms_output.put_line('errors occurred.');
         email_subject := 'storet FAILED in drop_old_stuff';
         email_text := email_subject || lf || lf || email_text;
         email_notify := failure_notify;
      end if;
      
--      $IF $$ci_db $THEN
--         dbms_output.put_line('Not emailing from ci database.');
         dbms_output.put_line(email_text);
--	  $ELSE
--         utl_mail.send@dbtrans(sender => 'bheck@usgs.gov', recipients => email_notify, subject => email_subject, message => email_text);
--      $END
      mesg := message;

   end main;
end create_storet_objects;
/
