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
   suffix varchar2(10);

   type cleanuptable is table of varchar2(80) index by binary_integer;
   cleanup cleanuptable;
   email_text varchar2(32000);

   
   table_list varchar2(4000 char) := 'translate(table_name, ''0123456789'', ''0000000000'') in ' ||
                                     '(''FA_REGULAR_RESULT_00000'',''FA_STATION_00000'',''DI_ACTIVITY_MATRIX_00000'',''DI_ACTIVITY_MEDIUM_00000'',' ||
                                      '''DI_CHARACTERISTIC_00000'',''DI_GEO_COUNTY_00000'',''DI_GEO_STATE_00000'',''DI_ORG_00000'',' ||
                                      '''DI_STATN_TYPES_00000'',''LU_MAD_HMETHOD_00000'',''LU_MAD_HDATUM_00000'',''LU_MAD_VMETHOD_00000'',' ||
                                      '''LU_MAD_VDATUM_00000'',''MT_WH_CONFIG_00000'',''STORET_SUM_00000'',''STORET_STATION_SUM_00000'',''STORET_RESULT_SUMT_00000'',' ||
                                      '''STORET_RESULT_SUM_00000'',''STORET_RESULT_CT_SUM_00000'',''STORET_RESULT_NR_SUM_00000'',''STORET_LCTN_LOC_00000'',' ||
                                      '''CHARACTERISTICNAME_00000'',''CHARACTERISTICTYPE_00000'',''COUNTRY_00000'',''COUNTY_00000'',''ORGANIZATION_00000'',' ||
                                      '''SAMPLEMEDIA_00000'',''SITETYPE_00000'',''STATE_00000'')';
                                      
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

   procedure determine_suffix
   is
      drop_remnants cursor_type;
      query         varchar2(4000) := 'select table_name from user_tables where ' || table_list ||
                                         ' and substr(table_name, -5) = substr(:current_suffix, 2) order by table_name';

      drop_name varchar2(30);
      stmt      varchar2(80);

   begin

      select '_' || to_char(nvl(max(to_number(substr(table_name, length('FA_REGULAR_RESULT_') + 1)) + 1), 1), 'fm00000')
        into suffix from user_tables
        where translate(table_name, '0123456789', '0000000000') = 'FA_REGULAR_RESULT_00000';


      append_email_text('using ''' || suffix || ''' for suffix.');

      open drop_remnants for query using suffix;
      loop
         fetch drop_remnants into drop_name;
         exit when drop_remnants%NOTFOUND;
         stmt := 'drop table ' || drop_name || ' cascade constraints purge';
         append_email_text('CLEANUP remnants: ' || stmt);
         execute immediate stmt;
      end loop;

   exception
      when others then
         message := 'FAIL to determine suffix: ' || SQLERRM;
         append_email_text(message);
   end determine_suffix;

   procedure create_regular_result
   is
   begin

      append_email_text('creating regular_result...');

      execute immediate '
      create table fa_regular_result' || suffix || ' parallel 4 compress pctfree 0 nologging cache
      partition by range(activity_start_date_time)
      (
         partition fa_regular_result_pre_1990 values less than (to_date(''01-JAN-1990'', ''DD-MON-YYYY'')),
         partition fa_regular_result_1990     values less than (to_date(''01-JAN-1991'', ''DD-MON-YYYY'')),
         partition fa_regular_result_1991     values less than (to_date(''01-JAN-1992'', ''DD-MON-YYYY'')),
         partition fa_regular_result_1992     values less than (to_date(''01-JAN-1993'', ''DD-MON-YYYY'')),
         partition fa_regular_result_1993     values less than (to_date(''01-JAN-1994'', ''DD-MON-YYYY'')),
         partition fa_regular_result_1994     values less than (to_date(''01-JAN-1995'', ''DD-MON-YYYY'')),
         partition fa_regular_result_1995     values less than (to_date(''01-JAN-1996'', ''DD-MON-YYYY'')),
         partition fa_regular_result_1996     values less than (to_date(''01-JAN-1997'', ''DD-MON-YYYY'')),
         partition fa_regular_result_1997     values less than (to_date(''01-JAN-1998'', ''DD-MON-YYYY'')),
         partition fa_regular_result_1998     values less than (to_date(''01-JAN-1999'', ''DD-MON-YYYY'')),
         partition fa_regular_result_1999     values less than (to_date(''01-JAN-2000'', ''DD-MON-YYYY'')),
         partition fa_regular_result_2000     values less than (to_date(''01-JAN-2001'', ''DD-MON-YYYY'')),
         partition fa_regular_result_2001     values less than (to_date(''01-JAN-2002'', ''DD-MON-YYYY'')),
         partition fa_regular_result_2002     values less than (to_date(''01-JAN-2003'', ''DD-MON-YYYY'')),
         partition fa_regular_result_2003     values less than (to_date(''01-JAN-2004'', ''DD-MON-YYYY'')),
         partition fa_regular_result_2004     values less than (to_date(''01-JAN-2005'', ''DD-MON-YYYY'')),
         partition fa_regular_result_2005     values less than (to_date(''01-JAN-2006'', ''DD-MON-YYYY'')),
         partition fa_regular_result_2006     values less than (to_date(''01-JAN-2007'', ''DD-MON-YYYY'')),
         partition fa_regular_result_2007     values less than (to_date(''01-JAN-2008'', ''DD-MON-YYYY'')),
         partition fa_regular_result_2008     values less than (to_date(''01-JAN-2009'', ''DD-MON-YYYY'')),
         partition fa_regular_result_2009     values less than (to_date(''01-JAN-2010'', ''DD-MON-YYYY'')),
         partition fa_regular_result_2010     values less than (to_date(''01-JAN-2011'', ''DD-MON-YYYY'')),
         partition fa_regular_result_2011     values less than (to_date(''01-JAN-2012'', ''DD-MON-YYYY'')),
         partition fa_regular_result_last     values less than (maxvalue)
      )
      as
      select /*+  full(FA_REGULAR_RESULT_NO_SOURCE) parallel(FA_REGULAR_RESULT_NO_SOURCE, 4)*/
         fa_regular_result_no_source.PK_ISN,                      /* might be handy*/
         fa_regular_result_no_source.ORGANIZATION_ID,             /* OK */
      /* ORGANIZATION_IS_NUMBER,   */
         fa_regular_result_no_source.organization_id || ''-'' || fa_regular_result_no_source.station_id STATION_ID,                        /* OK */
      /* STATION_NAME,                      */
         fa_regular_result_no_source.ACTIVITY_START_DATE_TIME,          /* OK */
         fa_regular_result_no_source.ACT_START_TIME_ZONE,          /* OK */
         fa_regular_result_no_source.TRIP_ID,          /* OK */
      /* TRIP_NAME,                         */
      /* STATION_VISIT_ID,  */
         fa_regular_result_no_source.CHARACTERISTIC_GROUP_TYPE,    /* OK */
         fa_regular_result_no_source.CHARACTERISTIC_NAME,          /* leave it just in case */
         fa_regular_result_no_source.RESULT_VALUE,                 /* OK */
         fa_regular_result_no_source.RESULT_UNIT,                  /* OK */
         fa_regular_result_no_source.RESULT_VALUE_TEXT,            /* OK */
         fa_regular_result_no_source.SAMPLE_FRACTION_TYPE,         /* OK */
         fa_regular_result_no_source.RESULT_VALUE_TYPE,            /* OK */
         fa_regular_result_no_source.STATISTIC_TYPE,               /* OK */
         fa_regular_result_no_source.RESULT_VALUE_STATUS,          /* OK */
         fa_regular_result_no_source.WEIGHT_BASIS_TYPE,          /* OK */
         fa_regular_result_no_source.TEMPERATURE_BASIS_LEVEL,          /* OK */
         fa_regular_result_no_source.DURATION_BASIS,          /* OK */
         fa_regular_result_no_source.ANALYTICAL_PROCEDURE_SOURCE,      /* OK */
         fa_regular_result_no_source.ANALYTICAL_PROCEDURE_ID,         /* OK */
      /* LAB_ID,                       */
         fa_regular_result_no_source.LAB_NAME,                        /* OK */
      /* LAB_CERTIFIED, */
      /* LAB_BATCH_ID, */
         fa_regular_result_no_source.ANALYSIS_DATE_TIME,
      /* ANALYSIS_TIME_ZONE, */
      /* LOWER_QUANTITATION_LIMIT, */
      /* UPPER_QUANTITATION_LIMIT, */
         fa_regular_result_no_source.DETECTION_LIMIT,                 /* OK */
         fa_regular_result_no_source.DETECTION_LIMIT_UNIT,                 /* OK */
         fa_regular_result_no_source.DETECTION_LIMIT_DESCRIPTION,                 /* OK */
         fa_regular_result_no_source.LAB_REMARK,                     /* OK */
      /* DISTANCE_MEASURE_FROM,              */
      /* DISTANCE_MEASURE_TO,                */
         fa_regular_result_no_source.PARTICLE_SIZE,                  /* OK */
      /* REPLICATE_ANALYSIS_COUNT,     */
         fa_regular_result_no_source.PRECISION,                      /* OK */
      /* CONFIDENCE_LEVEL,             */
      /* DILUTION_INDICATOR,           */
      /* RECOVERY_INDICATOR,           */
      /* CORRECTION_INDICATOR,           */
      /* STN_LATITUDE,  */
      /* STN_LONGITUDE,  */
      /* STN_HDATUM,  */
      /* STN_STD_LATITUDE,  */
      /* STN_STD_LONGITUDE,  */
      /* STN_STD_HDATUM,  */
      /* HYDROLOGIC_UNIT_CODE, */
      /* GENERATED_HUC, */
      /* RESULT_IS_NUMBER, */
         fa_regular_result_no_source.ACTIVITY_MEDIUM,     /* Ok */
         fa_regular_result_no_source.FK_STATION,          /* OK */
         fa_regular_result_no_source.FK_ORG,
      /* FK_DB_CAT, */
      /* FK_GEN_DB_CAT, */
         fa_regular_result_no_source.FK_GEO_COUNTY,      /* OK */
         fa_regular_result_no_source.FK_GEO_STATE,       /* OK */
      /* FK_DATE_ACT_START, */
         fa_regular_result_no_source.FK_ACT_MEDIUM,      /* OK */
         fa_regular_result_no_source.FK_ACT_MATRIX,      /* OK */
      /* ACTIVITY_IS_NUMBER, */
         fa_regular_result_no_source.FK_CHAR,
         fa_regular_result_no_source.FK_UNIT_CONVERSION,
         fa_regular_result_no_source.ACTIVITY_ID,          /* OK */
         fa_regular_result_no_source.REPLICATE_NUMBER,          /* OK */
         fa_regular_result_no_source.ACTIVITY_TYPE,          /* OK */
      /* ACTIVITY_CATEGORY, */
      /* ACTIVITY_INTENT,   */
      /* LOCATION_POINT_TYPE, */
      /* POINT_SEQUENCE_NUMBER, */
      /* WELL_NUMBER,  */
      /* PIPE_NUMBER,  */
         fa_regular_result_no_source.ACTIVITY_STOP_DATE_TIME,          /* OK */
         fa_regular_result_no_source.ACT_STOP_TIME_ZONE,          /* OK */
      /* ACTIVITY_REL_DEPTH,   */
         fa_regular_result_no_source.ACTIVITY_DEPTH,          /* OK */
         fa_regular_result_no_source.ACTIVITY_DEPTH_UNIT,          /* OK */
         fa_regular_result_no_source.ACTIVITY_UPPER_DEPTH,          /* OK */
         fa_regular_result_no_source.ACTIVITY_LOWER_DEPTH,          /* OK */
         fa_regular_result_no_source.UPR_LWR_DEPTH_UNIT,            /* OK */
         fa_regular_result_no_source.FIELD_PROCEDURE_ID,            /* OK */
      /* GEAR_CONFIG_ID, */
      /* ACTIVITY_LATITUDE, */
      /* ACTIVITY_LONGITUDE, */
      /* ACT_STD_LATITUDE,  */
      /* ACT_STD_LONGITUDE,  */
      /* ACT_STD_HDATUM,  */
      /* STD_VALUE,  */
      /* STD_UNIT,  */
      /* FK_ACT_MAD_HDATUM,  */
      /* FK_ACT_MAD_HMETHOD,  */
      /* ACTIVITY_ISN, */
      /* VISIT_START_DATE_TIME, */
      /* VISIT_START_TIME_ZONE, */
      /* VISIT_STOP_DATE_TIME, */
      /* VISIT_STOP_TIME_ZONE, */
      /* ACTIVITY_MATRIX, */
      /* FIELD_SET, */
      /* POINT_NAME, */
      /* SGO_INDICATOR, */
      /* MAP_SCALE, */
         fa_regular_result_no_source.FIELD_GEAR_ID,       /* OK */
      /* BIAS, */
      /* CONF_LVL_CORR_BIAS, */
         fa_regular_result_no_source.RESULT_COMMENT,      /* OK */
      /* TEXT_RESULT,  */
      /* CAS_NUMBER, */
      /* EPA_REG_NUMBER, */
         fa_regular_result_no_source.ITIS_NUMBER,   /* OK */
      /* CONTAINER_DESC, */
      /* TEMP_PRESERVN_TYPE, */
      /* PRESRV_STRGE_PRCDR, */
      /* PORTABLE_DATA_LOGGER, */
      /* FK_STN_ACT_PT,  */
      /* FK_STATN_TYPES, */
      /* LAST_USERID,  */
      /* LAST_CHANGE_DATE,  */
      /* BLOB_ID,    */
      /* BLOB_TITLE,    */
      /* ACT_BLOB_ID,    */
      /* ACT_BLOB_TITLE,    */
         fa_regular_result_no_source.ACTIVITY_COMMENT,    /* OK */
         fa_regular_result_no_source.ACTIVITY_DEPTH_REF_POINT,   /* OK */
         fa_regular_result_no_source.PROJECT_ID,   /* OK */
      /* TRIBAL_WATER_QUALITY_MEASURE, */
         fa_regular_result_no_source.RESULT_MEAS_QUAL_CODE,  /* OK */
         fa_regular_result_no_source.ACTIVITY_COND_ORG_TEXT,    /* OK */
         fa_regular_result_no_source.RESULT_DEPTH_MEAS_VALUE,     /* OK */
         fa_regular_result_no_source.RESULT_DEPTH_MEAS_UNIT_CODE,     /* OK */
         fa_regular_result_no_source.RESULT_DEPTH_ALT_REF_PT_TXT,     /* OK */
      /* ANALYTICAL_METHOD_LIST_AGENCY,  */
      /* ANALYTICAL_METHOD_LIST_VER,  */
      /* SMPRP_TRANSPORT_STORAGE_DESC,  */
         fa_regular_result_no_source.SOURCE_SYSTEM,                  /* might be handy */
      /* SOURCE_UID, */
      /* ETL_ID, */
      /* HORIZONTAL_ACCURACY_MEASURE, */
      /* LAB_ACCRED_AUTHORITY, */
      /* METHOD_SPECIATION, */
         fa_regular_result_no_source.LAB_SAMP_PRP_METHOD_ID,  /* OK */
      /* LAB_SAMP_PRP_METHOD_CONTEXT, */
      /* LAB_SAMP_PRP_METHOD_QUAL_TYPE, */
         fa_regular_result_no_source.LAB_SAMP_PRP_START_DATE_TIME,  /* OK */
      /* LAB_SAMP_PRP_START_TMZONE, */
      /* LAB_SAMP_PRP_END_DATE_TIME, */
      /* LAB_SAMP_PRP_END_TMZONE, */
      /* LAB_SAMP_PRP_DILUTION_FACTOR, */
      /* SAMPLING_POINT_NAME, */
      /* LAST_TRANSACTION_ID, */
      /* FK_DATE_LC */
         di_activity_matrix.matrix_name
      from fa_regular_result_no_source
           left join di_activity_matrix@storetw on fk_act_matrix = di_activity_matrix.pk_isn';

      cleanup(1) := 'drop table FA_REGULAR_RESULT' || suffix || ' cascade constraints purge';

      execute immediate '
      insert /*+ append nologging */ into fa_regular_result' || suffix ||
        '(
         PK_ISN,                      /* might be handy*/
         ORGANIZATION_ID,             /* OK */
      /* ORGANIZATION_IS_NUMBER,   */
         STATION_ID,                        /* OK */
      /* STATION_NAME,                      */
         ACTIVITY_START_DATE_TIME,          /* OK */
         ACT_START_TIME_ZONE,          /* OK */
         TRIP_ID,          /* OK */
      /* TRIP_NAME,                         */
      /* STATION_VISIT_ID,  */
         CHARACTERISTIC_GROUP_TYPE,    /* OK */
         CHARACTERISTIC_NAME,          /* leave it just in case */
         RESULT_VALUE,                 /* OK */
         RESULT_UNIT,                  /* OK */
         RESULT_VALUE_TEXT,            /* OK */
         SAMPLE_FRACTION_TYPE,         /* OK */
         RESULT_VALUE_TYPE,            /* OK */
         STATISTIC_TYPE,               /* OK */
         RESULT_VALUE_STATUS,          /* OK */
         WEIGHT_BASIS_TYPE,          /* OK */
         TEMPERATURE_BASIS_LEVEL,          /* OK */
         DURATION_BASIS,          /* OK */
         ANALYTICAL_PROCEDURE_SOURCE,      /* OK */
         ANALYTICAL_PROCEDURE_ID,         /* OK */
      /* LAB_ID,                       */
         LAB_NAME,                        /* OK */
      /* LAB_CERTIFIED, */
      /* LAB_BATCH_ID, */
         ANALYSIS_DATE_TIME,
      /* ANALYSIS_TIME_ZONE, */
      /* LOWER_QUANTITATION_LIMIT, */
      /* UPPER_QUANTITATION_LIMIT, */
         DETECTION_LIMIT,                 /* OK */
         DETECTION_LIMIT_UNIT,                 /* OK */
         DETECTION_LIMIT_DESCRIPTION,                 /* OK */
         LAB_REMARK,                     /* OK */
      /* DISTANCE_MEASURE_FROM,              */
      /* DISTANCE_MEASURE_TO,                */
         PARTICLE_SIZE,                  /* OK */
      /* REPLICATE_ANALYSIS_COUNT,     */
         PRECISION,                      /* OK */
      /* CONFIDENCE_LEVEL,             */
      /* DILUTION_INDICATOR,           */
      /* RECOVERY_INDICATOR,           */
      /* CORRECTION_INDICATOR,           */
      /* STN_LATITUDE,  */
      /* STN_LONGITUDE,  */
      /* STN_HDATUM,  */
      /* STN_STD_LATITUDE,  */
      /* STN_STD_LONGITUDE,  */
      /* STN_STD_HDATUM,  */
      /* HYDROLOGIC_UNIT_CODE, */
      /* GENERATED_HUC, */
      /* RESULT_IS_NUMBER, */
         ACTIVITY_MEDIUM,     /* Ok */
         FK_STATION,          /* OK */
         FK_ORG,
      /* FK_DB_CAT, */
      /* FK_GEN_DB_CAT, */
         FK_GEO_COUNTY,      /* OK */
         FK_GEO_STATE,       /* OK */
      /* FK_DATE_ACT_START, */
         FK_ACT_MEDIUM,      /* OK */
         FK_ACT_MATRIX,      /* OK */
      /* ACTIVITY_IS_NUMBER, */
         FK_CHAR,
         FK_UNIT_CONVERSION,
         ACTIVITY_ID,          /* OK */
         REPLICATE_NUMBER,          /* OK */
         ACTIVITY_TYPE,          /* OK */
      /* ACTIVITY_CATEGORY, */
      /* ACTIVITY_INTENT,   */
      /* LOCATION_POINT_TYPE, */
      /* POINT_SEQUENCE_NUMBER, */
      /* WELL_NUMBER,  */
      /* PIPE_NUMBER,  */
         ACTIVITY_STOP_DATE_TIME,          /* OK */
         ACT_STOP_TIME_ZONE,          /* OK */
      /* ACTIVITY_REL_DEPTH,   */
         ACTIVITY_DEPTH,          /* OK */
         ACTIVITY_DEPTH_UNIT,          /* OK */
         ACTIVITY_UPPER_DEPTH,          /* OK */
         ACTIVITY_LOWER_DEPTH,          /* OK */
         UPR_LWR_DEPTH_UNIT,            /* OK */
         FIELD_PROCEDURE_ID,            /* OK */
      /* GEAR_CONFIG_ID, */
      /* ACTIVITY_LATITUDE, */
      /* ACTIVITY_LONGITUDE, */
      /* ACT_STD_LATITUDE,  */
      /* ACT_STD_LONGITUDE,  */
      /* ACT_STD_HDATUM,  */
      /* STD_VALUE,  */
      /* STD_UNIT,  */
      /* FK_ACT_MAD_HDATUM,  */
      /* FK_ACT_MAD_HMETHOD,  */
      /* ACTIVITY_ISN, */
      /* VISIT_START_DATE_TIME, */
      /* VISIT_START_TIME_ZONE, */
      /* VISIT_STOP_DATE_TIME, */
      /* VISIT_STOP_TIME_ZONE, */
      /* ACTIVITY_MATRIX, */
      /* FIELD_SET, */
      /* POINT_NAME, */
      /* SGO_INDICATOR, */
      /* MAP_SCALE, */
         FIELD_GEAR_ID,       /* OK */
      /* BIAS, */
      /* CONF_LVL_CORR_BIAS, */
         RESULT_COMMENT,      /* OK */
      /* TEXT_RESULT,  */
      /* CAS_NUMBER, */
      /* EPA_REG_NUMBER, */
         ITIS_NUMBER,   /* OK */
      /* CONTAINER_DESC, */
      /* TEMP_PRESERVN_TYPE, */
      /* PRESRV_STRGE_PRCDR, */
      /* PORTABLE_DATA_LOGGER, */
      /* FK_STN_ACT_PT,  */
      /* FK_STATN_TYPES, */
      /* LAST_USERID,  */
      /* LAST_CHANGE_DATE,  */
      /* BLOB_ID,    */
      /* BLOB_TITLE,    */
      /* ACT_BLOB_ID,    */
      /* ACT_BLOB_TITLE,    */
         ACTIVITY_COMMENT,    /* OK */
         ACTIVITY_DEPTH_REF_POINT,   /* OK */
         PROJECT_ID,   /* OK */
      /* TRIBAL_WATER_QUALITY_MEASURE, */
         RESULT_MEAS_QUAL_CODE,  /* OK */
         ACTIVITY_COND_ORG_TEXT,    /* OK */
         RESULT_DEPTH_MEAS_VALUE,     /* OK */
         RESULT_DEPTH_MEAS_UNIT_CODE,     /* OK */
         RESULT_DEPTH_ALT_REF_PT_TXT,     /* OK */
      /* ANALYTICAL_METHOD_LIST_AGENCY,  */
      /* ANALYTICAL_METHOD_LIST_VER,  */
      /* SMPRP_TRANSPORT_STORAGE_DESC,  */
         SOURCE_SYSTEM,                  /* might be handy */
      /* SOURCE_UID, */
      /* ETL_ID, */
      /* HORIZONTAL_ACCURACY_MEASURE, */
      /* LAB_ACCRED_AUTHORITY, */
      /* METHOD_SPECIATION, */
         LAB_SAMP_PRP_METHOD_ID,  /* OK */
      /* LAB_SAMP_PRP_METHOD_CONTEXT, */
      /* LAB_SAMP_PRP_METHOD_QUAL_TYPE, */
         LAB_SAMP_PRP_START_DATE_TIME,  /* OK */
      /* LAB_SAMP_PRP_START_TMZONE, */
      /* LAB_SAMP_PRP_END_DATE_TIME, */
      /* LAB_SAMP_PRP_END_TMZONE, */
      /* LAB_SAMP_PRP_DILUTION_FACTOR, */
      /* SAMPLING_POINT_NAME, */
      /* LAST_TRANSACTION_ID, */
      /* FK_DATE_LC */
         matrix_name
       )
       SELECT
         fa_regular_result.PK_ISN,                      /* might be handy*/
         fa_regular_result.ORGANIZATION_ID,             /* OK */
      /* ORGANIZATION_IS_NUMBER,   */
         fa_regular_result.organization_id || ''-'' || fa_regular_result.station_id STATION_ID,                        /* OK */
      /* STATION_NAME,                      */
         fa_regular_result.ACTIVITY_START_DATE_TIME,          /* OK */
         fa_regular_result.ACT_START_TIME_ZONE,          /* OK */
         fa_regular_result.TRIP_ID,          /* OK */
      /* TRIP_NAME,                         */
      /* STATION_VISIT_ID,  */
         fa_regular_result.CHARACTERISTIC_GROUP_TYPE,    /* OK */
         fa_regular_result.CHARACTERISTIC_NAME,          /* leave it just in case */
         fa_regular_result.RESULT_VALUE,                 /* OK */
         fa_regular_result.RESULT_UNIT,                  /* OK */
         fa_regular_result.RESULT_VALUE_TEXT,            /* OK */
         fa_regular_result.SAMPLE_FRACTION_TYPE,         /* OK */
         fa_regular_result.RESULT_VALUE_TYPE,            /* OK */
         fa_regular_result.STATISTIC_TYPE,               /* OK */
         fa_regular_result.RESULT_VALUE_STATUS,          /* OK */
         fa_regular_result.WEIGHT_BASIS_TYPE,          /* OK */
         fa_regular_result.TEMPERATURE_BASIS_LEVEL,          /* OK */
         fa_regular_result.DURATION_BASIS,          /* OK */
         fa_regular_result.ANALYTICAL_PROCEDURE_SOURCE,      /* OK */
         fa_regular_result.ANALYTICAL_PROCEDURE_ID,         /* OK */
      /* LAB_ID,                       */
         fa_regular_result.LAB_NAME,                        /* OK */
      /* LAB_CERTIFIED, */
      /* LAB_BATCH_ID, */
         fa_regular_result.ANALYSIS_DATE_TIME,
      /* ANALYSIS_TIME_ZONE, */
      /* LOWER_QUANTITATION_LIMIT, */
      /* UPPER_QUANTITATION_LIMIT, */
         fa_regular_result.DETECTION_LIMIT,                 /* OK */
         fa_regular_result.DETECTION_LIMIT_UNIT,                 /* OK */
         fa_regular_result.DETECTION_LIMIT_DESCRIPTION,                 /* OK */
         fa_regular_result.LAB_REMARK,                     /* OK */
      /* DISTANCE_MEASURE_FROM,              */
      /* DISTANCE_MEASURE_TO,                */
         fa_regular_result.PARTICLE_SIZE,                  /* OK */
      /* REPLICATE_ANALYSIS_COUNT,     */
         fa_regular_result.PRECISION,                      /* OK */
      /* CONFIDENCE_LEVEL,             */
      /* DILUTION_INDICATOR,           */
      /* RECOVERY_INDICATOR,           */
      /* CORRECTION_INDICATOR,           */
      /* STN_LATITUDE,  */
      /* STN_LONGITUDE,  */
      /* STN_HDATUM,  */
      /* STN_STD_LATITUDE,  */
      /* STN_STD_LONGITUDE,  */
      /* STN_STD_HDATUM,  */
      /* HYDROLOGIC_UNIT_CODE, */
      /* GENERATED_HUC, */
      /* RESULT_IS_NUMBER, */
         fa_regular_result.ACTIVITY_MEDIUM,     /* Ok */
         fa_regular_result.FK_STATION,          /* OK */
         fa_regular_result.FK_ORG,
      /* FK_DB_CAT, */
      /* FK_GEN_DB_CAT, */
         fa_regular_result.FK_GEO_COUNTY,      /* OK */
         fa_regular_result.FK_GEO_STATE,       /* OK */
      /* FK_DATE_ACT_START, */
         fa_regular_result.FK_ACT_MEDIUM,      /* OK */
         fa_regular_result.FK_ACT_MATRIX,      /* OK */
      /* ACTIVITY_IS_NUMBER, */
         fa_regular_result.FK_CHAR,
         fa_regular_result.FK_UNIT_CONVERSION,
         fa_regular_result.ACTIVITY_ID,          /* OK */
         fa_regular_result.REPLICATE_NUMBER,          /* OK */
         fa_regular_result.ACTIVITY_TYPE,          /* OK */
      /* ACTIVITY_CATEGORY, */
      /* ACTIVITY_INTENT,   */
      /* LOCATION_POINT_TYPE, */
      /* POINT_SEQUENCE_NUMBER, */
      /* WELL_NUMBER,  */
      /* PIPE_NUMBER,  */
         fa_regular_result.ACTIVITY_STOP_DATE_TIME,          /* OK */
         fa_regular_result.ACT_STOP_TIME_ZONE,          /* OK */
      /* ACTIVITY_REL_DEPTH,   */
         fa_regular_result.ACTIVITY_DEPTH,          /* OK */
         fa_regular_result.ACTIVITY_DEPTH_UNIT,          /* OK */
         fa_regular_result.ACTIVITY_UPPER_DEPTH,          /* OK */
         fa_regular_result.ACTIVITY_LOWER_DEPTH,          /* OK */
         fa_regular_result.UPR_LWR_DEPTH_UNIT,            /* OK */
         fa_regular_result.FIELD_PROCEDURE_ID,            /* OK */
      /* GEAR_CONFIG_ID, */
      /* ACTIVITY_LATITUDE, */
      /* ACTIVITY_LONGITUDE, */
      /* ACT_STD_LATITUDE,  */
      /* ACT_STD_LONGITUDE,  */
      /* ACT_STD_HDATUM,  */
      /* STD_VALUE,  */
      /* STD_UNIT,  */
      /* FK_ACT_MAD_HDATUM,  */
      /* FK_ACT_MAD_HMETHOD,  */
      /* ACTIVITY_ISN, */
      /* VISIT_START_DATE_TIME, */
      /* VISIT_START_TIME_ZONE, */
      /* VISIT_STOP_DATE_TIME, */
      /* VISIT_STOP_TIME_ZONE, */
      /* ACTIVITY_MATRIX, */
      /* FIELD_SET, */
      /* POINT_NAME, */
      /* SGO_INDICATOR, */
      /* MAP_SCALE, */
         fa_regular_result.FIELD_GEAR_ID,       /* OK */
      /* BIAS, */
      /* CONF_LVL_CORR_BIAS, */
         fa_regular_result.RESULT_COMMENT,      /* OK */
      /* TEXT_RESULT,  */
      /* CAS_NUMBER, */
      /* EPA_REG_NUMBER, */
         fa_regular_result.ITIS_NUMBER,   /* OK */
      /* CONTAINER_DESC, */
      /* TEMP_PRESERVN_TYPE, */
      /* PRESRV_STRGE_PRCDR, */
      /* PORTABLE_DATA_LOGGER, */
      /* FK_STN_ACT_PT,  */
      /* FK_STATN_TYPES, */
      /* LAST_USERID,  */
      /* LAST_CHANGE_DATE,  */
      /* BLOB_ID,    */
      /* BLOB_TITLE,    */
      /* ACT_BLOB_ID,    */
      /* ACT_BLOB_TITLE,    */
         fa_regular_result.ACTIVITY_COMMENT,    /* OK */
         fa_regular_result.ACTIVITY_DEPTH_REF_POINT,   /* OK */
         fa_regular_result.PROJECT_ID,   /* OK */
      /* TRIBAL_WATER_QUALITY_MEASURE, */
         fa_regular_result.RESULT_MEAS_QUAL_CODE,  /* OK */
         fa_regular_result.ACTIVITY_COND_ORG_TEXT,    /* OK */
         fa_regular_result.RESULT_DEPTH_MEAS_VALUE,     /* OK */
         fa_regular_result.RESULT_DEPTH_MEAS_UNIT_CODE,     /* OK */
         fa_regular_result.RESULT_DEPTH_ALT_REF_PT_TXT,     /* OK */
      /* ANALYTICAL_METHOD_LIST_AGENCY,  */
      /* ANALYTICAL_METHOD_LIST_VER,  */
      /* SMPRP_TRANSPORT_STORAGE_DESC,  */
         fa_regular_result.SOURCE_SYSTEM,                  /* might be handy */
      /* SOURCE_UID, */
      /* ETL_ID, */
      /* HORIZONTAL_ACCURACY_MEASURE, */
      /* LAB_ACCRED_AUTHORITY, */
      /* METHOD_SPECIATION, */
         fa_regular_result.LAB_SAMP_PRP_METHOD_ID,  /* OK */
      /* LAB_SAMP_PRP_METHOD_CONTEXT, */
      /* LAB_SAMP_PRP_METHOD_QUAL_TYPE, */
         fa_regular_result.LAB_SAMP_PRP_START_DATE_TIME,  /* OK */
      /* LAB_SAMP_PRP_START_TMZONE, */
      /* LAB_SAMP_PRP_END_DATE_TIME, */
      /* LAB_SAMP_PRP_END_TMZONE, */
      /* LAB_SAMP_PRP_DILUTION_FACTOR, */
      /* SAMPLING_POINT_NAME, */
      /* LAST_TRANSACTION_ID, */
      /* FK_DATE_LC */
         di_activity_matrix.matrix_name
      from fa_regular_result@storetw
           left join di_activity_matrix@storetw on fk_act_matrix = di_activity_matrix.pk_isn';

     exception
      when others then
         message := 'FAIL to create FA_REGULAR_RESULT: ' || SQLERRM;
         append_email_text(message);
   end create_regular_result;

   procedure create_station
   is
   begin

      append_email_text('creating station...');

      execute immediate
     'create table fa_station' || suffix || ' compress pctfree 0 nologging parallel 4 cache as
      select
         fa_station.organization_id || ''-'' || fa_station.station_id STATION_ID,                /* OK */
         trim(fa_station.station_name) as station_name,              /* OK */
         fa_station.ORGANIZATION_ID,           /* OK */
         /* LOCATION_POINT_TYPE,     */
         /* POINT_SEQUENCE_NUMBER,   */
         /* WELL_NUMBER,             */
         /* PIPE_NUMBER,             */
         fa_station.LATITUDE,                  /* OK */
         fa_station.LONGITUDE,                 /* OK */
         regexp_substr(fa_station.map_scale, ''[[:digit:]]+$'') map_scale,                 /* OK */
         fa_station.ELEVATION,                 /* OK */
      /* HYDROLOGIC_UNIT_CODE,    */
         fa_station.GENERATED_HUC,             /* OK */
      /* RF1_SEGMENT_CODE,        */
      /* RF1_SEGMENT_NAME,        */
      /* RF1_MILEAGE,             */
      /* ON_REACH_IND,            */
      /* NRCS_WATERSHED_ID,       */
      /* OTHER_ESTUARY_NAME,      */
      /* GREAT_LAKE_NAME,         */
      /* OCEAN_NAME,              */
      /* NATV_AMERICAN_LAND_NAME, */
      /* FRS_KEY_IDENTIFIER,      */
      /* STATION_VISITED,         */
      /* STATION_IS_NUMBER,       */
      /* ORGANIZATION_IS_NUMBER,  */
         fa_station.STATION_GROUP_TYPE,        /* OK */
      /* SGO_INDICATOR,           */
      /* WELL_NAME,               */
      /* NAICS_CODE,              */
      /* SPRING_TYPE_IMPROVEMENT, */
      /* SPRING_PERMANENCE,       */
      /* SPRING_USGS_GEOLOGIC_UNIT,  */
      /* SPRING_OTHER_NAME,       */
      /* SPRING_USGS_LITHOLOGIC_UNIT,*/
      /* POINT_NAME,              */
      /* BLOB_TITLE,              */
      /* TSMALP_IS_NUMBER,        */
         trim(fa_station.description_text) as description_text,           /* OK */
      /* LAST_USERID,             */
      /* LAST_CHANGE_DATE,        */
         fa_station.PROJECT_ID,                 /* OK */
      /* TRIBAL_WATER_QUALITY_MEASURE,  */
      /* HORIZONTAL_ACCURACY,     */
      /* FK_DB_CAT,               */
      /* FK_GEN_DB_CAT,           */
         fa_station.FK_GEO_STATE,               /* OK */
         fa_station.FK_GEO_COUNTY,               /* OK */
         fa_station.FK_MAD_HMETHOD,               /* OK */
         fa_station.FK_MAD_HDATUM,               /* OK */
         fa_station.FK_MAD_VMETHOD,               /* OK */
         fa_station.FK_MAD_VDATUM,               /* OK */
      /* STD_LATITUDE,            */
      /* STD_LONGITUDE,           */
      /* FK_STD_HDATUM,           */
         fa_station.FK_ORG,                     /* OK */
      /* FK_STATN_TYPES,          */
      /* FK_ESTRY_PRIMARY,        */
      /* FK_ESTRY_SECONDARY,      */
      /* BLOB_ID,                 */
         fa_station.FK_PRIMARY_TYPE,            /* OK */
      /* FK_SECONDARY_TYPE,       */
      /* FK_HORIZONTAL_DATUM,     */
      /* FK_GEOPOSITIONING_METHOD,*/
      /* FK_ELEVATION_DATUM,      */
      /* FK_ELEVATION_METHOD,     */
      /* FK_PRIMARY_ESTUARY,      */
      /* FK_SECONDARY_ESTUARY,    */
      /* FK_COUNTRY_CODE,         */
      /* FK_STATE_POSTAL_CODE,    */
         fa_station.SOURCE_SYSTEM,
      /* SOURCE_UID,              */
/*         fa_station.GEOM,  */                     /* OK */
      /* WELL_TYPE_NAME,          */
      /* WELL_FORMATION_TYPE,     */
      /* WELL_HOLE_DEPTH,         */
      /* WELL_AQUIFER_NAME,       */
         fa_station.PK_ISN,                     /* OK */
      /* OBJECTID,                */
      /* FK_GEN_GEO_STATE,        */
      /* FK_GEN_GEO_COUNTY,       */
         nvl2(fa_station.elevation, nvl(fa_station.elevation_unit, ''ft''), null) elevation_unit,             /* OK */
         fa_station.HUCTWELVEDIGITCODE,         /* OK */
         fa_station.WGS84_LATITUDE,             /* OK */
         fa_station.WGS84_LONGITUDE,            /* OK */
      /* FK_WGS84_HDATUM,         */
      /* LAST_TRANSACTION_ID,     */
      /* FK_DATE_LC,              */
      /* PROGRAM_INDICATOR,       */
      /* ELEVATION_BK,            */
      /* GEN_HUC_BK,              */
      /* FK_GEN_DB_CAT_BK         */
         di_org.organization_name,
         di_geo_state.country_code country_cd,
         di_geo_state.country_name country_name,
         rtrim(di_geo_state.fips_state_code) state_cd,
         di_geo_state.state_name,
         di_geo_county.fips_county_code county_cd,
         di_geo_county.county_name,
         nvl(lu_mad_hmethod.geopositioning_method, ''Unknown'') geopositioning_method,
         nvl(rtrim(lu_mad_hdatum.id_code), ''Unknown'') hdatum_id_code,
         regexp_substr(fa_station.elevation, ''^[[:digit:]]+'') elevation_value,
         nvl2(fa_station.elevation, lu_mad_vmethod.elevation_method, null) elevation_method,
         nvl2(fa_station.elevation, nvl(lu_mad_vdatum.id_code, ''Unknown''), null) vdatum_id_code
      from fa_station@storetw
           left join di_org@storetw on fk_org = di_org.pk_isn
           left join di_geo_state@storetw on fk_geo_state = di_geo_state.pk_isn
           left join di_geo_county@storetw on fk_geo_county = di_geo_county.pk_isn
           left join lu_mad_hmethod@storetw on fk_mad_hmethod = lu_mad_hmethod.pk_isn
           left join lu_mad_hdatum@storetw on fk_mad_hdatum = lu_mad_hdatum.pk_isn
           left join lu_mad_vmethod@storetw on fk_mad_vmethod = lu_mad_vmethod.pk_isn
           left join lu_mad_vdatum@storetw on fk_mad_vdatum = lu_mad_vdatum.pk_isn';

      cleanup(2) := 'drop table FA_STATION' || suffix || ' cascade constraints purge';
      
      execute immediate 'alter table fa_station' || suffix || ' add (geom mdsys.sdo_geometry)';
      execute immediate 'update fa_station' || suffix || ' set geom = mdsys.sdo_geometry(2001, 8307, mdsys.sdo_point_type(wgs84_longitude, wgs84_latitude, null), null, null)
                          where wgs84_latitude is not null and
                                wgs84_longitude is not null';
      
   exception
      when others then
         message := 'FAIL to create FA_STATION: ' || SQLERRM;
         append_email_text(message);
   end create_station;

  procedure create_summaries
   is
   begin

      append_email_text('creating storet_station_sum...');

      execute immediate    /* seem to be problems with parallel 4 so make it parallel 1 */
     'create table STORET_STATION_SUM' || suffix || ' pctfree 0 cache compress nologging parallel 1 as
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
            cast(nvl(result_count, 0) as number(9)) result_count
         from
            fa_station'    || suffix || ' a
            left join (select fk_station, count(*) result_count from fa_regular_result' || suffix || ' group by fk_station) e
              on a.pk_isn = e.fk_station
         order by
            country_cd,
            state_cd,
            county_cd,
            station_group_type';

      cleanup(3) := 'drop table STORET_STATION_SUM' || suffix || ' cascade constraints purge';

      append_email_text('creating storet_result_sumt...');

      /* temp table -- bug prevents us from doing this all in one... */
      execute immediate
     'create table storet_result_sumt' || suffix || ' pctfree 0 nologging parallel 4
         as
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
            storet_station_sum' || suffix || ' a,
            (select /*+ parallel(4) */
                fk_station,
                activity_medium,
                characteristic_group_type,
                characteristic_name,
                trunc(activity_start_date_time) activity_start_date_time,
                cast(count(*) as number(9)) result_count
             from
                fa_regular_result'  || suffix || ' 
             group by
                fk_station,
                activity_medium,
                characteristic_group_type,
                characteristic_name,
                trunc(activity_start_date_time)) b
         where
             b.fk_station = a.pk_isn(+)';

      cleanup(4) := 'drop table STORET_RESULT_SUMT' || suffix || ' cascade constraints purge';

      append_email_text('creating storet_result_sum...');

      execute immediate
     'create table storetmodern.storet_result_sum' || suffix || ' pctfree 0 cache compress nologging parallel 4
         partition by range(activity_start_date_time)
         (
            partition storet_result_sum_pre_1990 values less than (to_date(''01-JAN-1990'', ''DD-MON-YYYY'')),
            partition storet_result_sum_1990     values less than (to_date(''01-JAN-1991'', ''DD-MON-YYYY'')),
            partition storet_result_sum_1991     values less than (to_date(''01-JAN-1992'', ''DD-MON-YYYY'')),
            partition storet_result_sum_1992     values less than (to_date(''01-JAN-1993'', ''DD-MON-YYYY'')),
            partition storet_result_sum_1993     values less than (to_date(''01-JAN-1994'', ''DD-MON-YYYY'')),
            partition storet_result_sum_1994     values less than (to_date(''01-JAN-1995'', ''DD-MON-YYYY'')),
            partition storet_result_sum_1995     values less than (to_date(''01-JAN-1996'', ''DD-MON-YYYY'')),
            partition storet_result_sum_1996     values less than (to_date(''01-JAN-1997'', ''DD-MON-YYYY'')),
            partition storet_result_sum_1997     values less than (to_date(''01-JAN-1998'', ''DD-MON-YYYY'')),
            partition storet_result_sum_1998     values less than (to_date(''01-JAN-1999'', ''DD-MON-YYYY'')),
            partition storet_result_sum_1999     values less than (to_date(''01-JAN-2000'', ''DD-MON-YYYY'')),
            partition storet_result_sum_2000     values less than (to_date(''01-JAN-2001'', ''DD-MON-YYYY'')),
            partition storet_result_sum_2001     values less than (to_date(''01-JAN-2002'', ''DD-MON-YYYY'')),
            partition storet_result_sum_2002     values less than (to_date(''01-JAN-2003'', ''DD-MON-YYYY'')),
            partition storet_result_sum_2003     values less than (to_date(''01-JAN-2004'', ''DD-MON-YYYY'')),
            partition storet_result_sum_2004     values less than (to_date(''01-JAN-2005'', ''DD-MON-YYYY'')),
            partition storet_result_sum_2005     values less than (to_date(''01-JAN-2006'', ''DD-MON-YYYY'')),
            partition storet_result_sum_2006     values less than (to_date(''01-JAN-2007'', ''DD-MON-YYYY'')),
            partition storet_result_sum_2007     values less than (to_date(''01-JAN-2008'', ''DD-MON-YYYY'')),
            partition storet_result_sum_2008     values less than (to_date(''01-JAN-2009'', ''DD-MON-YYYY'')),
            partition storet_result_sum_2009     values less than (to_date(''01-JAN-2010'', ''DD-MON-YYYY'')),
            partition storet_result_sum_2010     values less than (to_date(''01-JAN-2011'', ''DD-MON-YYYY'')),
            partition storet_result_sum_2011     values less than (to_date(''01-JAN-2012'', ''DD-MON-YYYY'')),
            partition storet_result_sum_last     values less than (maxvalue)
         )
         as
         select
            fk_station,
            station_id,
            country_cd,
            state_cd,
            county_cd,
            station_group_type,
            organization_id,
            generated_huc,
            activity_medium,
            characteristic_group_type,
            characteristic_name,
            activity_start_date_time,
            result_count
         from
            storet_result_sumt' || suffix || '
         order by
             fk_station,
             station_id,
             activity_medium,
             characteristic_group_type,
             characteristic_name';

      cleanup(5) := 'drop table STORET_RESULT_SUM' || suffix || ' cascade constraints purge';

      append_email_text('creating storet_result_ct_sum...');

      execute immediate
     'create table storet_result_ct_sum' || suffix || ' pctfree 0 cache compress nologging parallel 4
         partition by list(characteristic_group_type)
         (
            partition storet_result_ct_sum_phys values (''Physical''),
            partition storet_result_ct_sum_radi values (''Radiochemical''),
            partition storet_result_ct_sum_org1 values (''Organics, Pesticide''),
            partition storet_result_ct_sum_micr values (''Microbiological''),
            partition storet_result_ct_sum_org2 values (''Organics, PCBs''),
            partition storet_result_ct_sum_nutr values (''Nutrient''),
            partition storet_result_ct_sum_ino1 values (''Inorganics, Minor, Non-metals''),
            partition storet_result_ct_sum_nota values (''Not Assigned''),
            partition storet_result_ct_sum_ino2 values (''Inorganics, Minor, Metals''),
            partition storet_result_ct_sum_org3 values (''Organics, Other''),
            partition storet_result_ct_sum_def  values (default)
        )
        as select /*+ full(a) parallel(a, 4) */
            fk_station,
            station_id,
            country_cd,
            state_cd,
            county_cd,
            station_group_type,
            organization_id,
            generated_huc,
            activity_medium,
            characteristic_group_type,
            characteristic_name,
            cast(sum(result_count) as number(9)) result_count
         from
            storet_result_sum' || suffix || ' a
         group by
            fk_station,
            station_id,
            country_cd,
            state_cd,
            county_cd,
            station_group_type,
            organization_id,
            generated_huc,
            activity_medium,
            characteristic_group_type,
            characteristic_name
         order by
            fk_station,
            station_id,
            activity_medium,
            characteristic_group_type,
            characteristic_name';

      cleanup(6) := 'drop table STORET_RESULT_CT_SUM' || suffix || ' cascade constraints purge';

      execute immediate
     'create table storetmodern.storet_result_nr_sum' || suffix || ' pctfree 0 cache compress nologging parallel 4
         partition by range(activity_start_date_time)
         (
            partition storet_result_nr_sum_pre_1990 values less than (to_date(''01-JAN-1990'', ''DD-MON-YYYY'')),
            partition storet_result_nr_sum_1990     values less than (to_date(''01-JAN-1991'', ''DD-MON-YYYY'')),
            partition storet_result_nr_sum_1991     values less than (to_date(''01-JAN-1992'', ''DD-MON-YYYY'')),
            partition storet_result_nr_sum_1992     values less than (to_date(''01-JAN-1993'', ''DD-MON-YYYY'')),
            partition storet_result_nr_sum_1993     values less than (to_date(''01-JAN-1994'', ''DD-MON-YYYY'')),
            partition storet_result_nr_sum_1994     values less than (to_date(''01-JAN-1995'', ''DD-MON-YYYY'')),
            partition storet_result_nr_sum_1995     values less than (to_date(''01-JAN-1996'', ''DD-MON-YYYY'')),
            partition storet_result_nr_sum_1996     values less than (to_date(''01-JAN-1997'', ''DD-MON-YYYY'')),
            partition storet_result_nr_sum_1997     values less than (to_date(''01-JAN-1998'', ''DD-MON-YYYY'')),
            partition storet_result_nr_sum_1998     values less than (to_date(''01-JAN-1999'', ''DD-MON-YYYY'')),
            partition storet_result_nr_sum_1999     values less than (to_date(''01-JAN-2000'', ''DD-MON-YYYY'')),
            partition storet_result_nr_sum_2000     values less than (to_date(''01-JAN-2001'', ''DD-MON-YYYY'')),
            partition storet_result_nr_sum_2001     values less than (to_date(''01-JAN-2002'', ''DD-MON-YYYY'')),
            partition storet_result_nr_sum_2002     values less than (to_date(''01-JAN-2003'', ''DD-MON-YYYY'')),
            partition storet_result_nr_sum_2003     values less than (to_date(''01-JAN-2004'', ''DD-MON-YYYY'')),
            partition storet_result_nr_sum_2004     values less than (to_date(''01-JAN-2005'', ''DD-MON-YYYY'')),
            partition storet_result_nr_sum_2005     values less than (to_date(''01-JAN-2006'', ''DD-MON-YYYY'')),
            partition storet_result_nr_sum_2006     values less than (to_date(''01-JAN-2007'', ''DD-MON-YYYY'')),
            partition storet_result_nr_sum_2007     values less than (to_date(''01-JAN-2008'', ''DD-MON-YYYY'')),
            partition storet_result_nr_sum_2008     values less than (to_date(''01-JAN-2009'', ''DD-MON-YYYY'')),
            partition storet_result_nr_sum_2009     values less than (to_date(''01-JAN-2010'', ''DD-MON-YYYY'')),
            partition storet_result_nr_sum_2010     values less than (to_date(''01-JAN-2011'', ''DD-MON-YYYY'')),
            partition storet_result_nr_sum_2011     values less than (to_date(''01-JAN-2012'', ''DD-MON-YYYY'')),
            partition storet_result_nr_sum_last     values less than (maxvalue)
         )
        as select /*+ full(a) parallel(a, 4) */
            fk_station,
            activity_medium,
            characteristic_group_type,
            characteristic_name,
            activity_start_date_time,
            cast(sum(result_count) as number(9)) result_count
         from
            storet_result_sum' || suffix || ' a
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

      cleanup(7) := 'drop table STORET_RESULT_NR_SUM' || suffix || ' cascade constraints purge';

      append_email_text('creating storet_lctn_loc...');

      execute immediate
     'create table storet_lctn_loc' || suffix || ' compress pctfree 0 nologging parallel 1 as
      select /*+ parallel(4) */ distinct
             country_cd,
             state_cd state_fips,
             organization_id,
             organization_name
       from fa_station' || suffix ;

      cleanup(8) := 'drop table storet_lctn_loc' || suffix || ' cascade constraints purge';
      
   exception
      when others then
         message := 'FAIL to create summaries: ' || SQLERRM;
         append_email_text(message);
   end create_summaries;

   procedure create_lookups
   is
   begin

      append_email_text('creating lookups...');

      execute immediate
     'create table DI_ACTIVITY_MATRIX' || suffix || ' compress pctfree 0 nologging as
      select
         PK_ISN,
         MATRIX_CODE,
         MATRIX_NAME,
         MATRIX_DESCRIPTION
      FROM
         DI_ACTIVITY_MATRIX@storetw';

      cleanup(9) := 'drop table DI_ACTIVITY_MATRIX' || suffix || ' cascade constraints purge';

      execute immediate
     'create table DI_ACTIVITY_MEDIUM' || suffix || ' compress pctfree 0 nologging as
      select
         PK_ISN,
         ACTIVITY_MEDIUM
      FROM
         DI_ACTIVITY_MEDIUM@storetw';

      cleanup(10) := 'drop table DI_ACTIVITY_MEDIUM' || suffix || ' cascade constraints purge';

      execute immediate
     'create table DI_CHARACTERISTIC' || suffix || ' compress pctfree 0 nologging as
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
         DI_CHARACTERISTIC@storetw';

      cleanup(11) := 'drop table DI_CHARACTERISTIC' || suffix || ' cascade constraints purge';

      execute immediate
     'create table DI_GEO_COUNTY' || suffix || ' compress pctfree 0 nologging as
      select
          PK_ISN,
          FK_GEO_STATE,
          FIPS_COUNTY_CODE,
          COUNTY_NAME
      FROM
         DI_GEO_COUNTY@storetw';

      cleanup(12) := 'drop table DI_GEO_COUNTY' || suffix || ' cascade constraints purge';

      execute immediate
     'create table DI_GEO_STATE' || suffix || ' compress pctfree 0 nologging as
      select
         PK_ISN,
         FIPS_STATE_CODE,
         STATE_POSTAL_CODE,
         STATE_NAME,
         COUNTRY_CODE,
         COUNTRY_NAME
      FROM
         DI_GEO_STATE@storetw';

      cleanup(13) := 'drop table DI_GEO_STATE' || suffix || ' cascade constraints purge';

      execute immediate
     'create table DI_ORG' || suffix || ' compress pctfree 0 nologging as
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
         DI_ORG@storetw';

      cleanup(14) := 'drop table DI_ORG' || suffix || ' cascade constraints purge';

      execute immediate
     'create table DI_STATN_TYPES' || suffix || ' compress pctfree 0 nologging as
      select
         PK_ISN,
         PRIMARY_TYPE,
         SECONDARY_TYPE,
         SORT_ORDER,
         SGO_INDICATOR,
         STATION_GROUP_TYPE
      FROM
         DI_STATN_TYPES@storetw';

      cleanup(15) := 'drop table DI_STATN_TYPES' || suffix || ' cascade constraints purge';

      execute immediate
     'create table LU_MAD_HMETHOD' || suffix || ' compress pctfree 0 nologging as
      select
         PK_ISN,
         ID_CODE,
         GEOPOSITIONING_METHOD
      FROM
         LU_MAD_HMETHOD@storetw';

      cleanup(16) := 'drop table LU_MAD_HMETHOD' || suffix || ' cascade constraints purge';

      execute immediate
     'create table LU_MAD_HDATUM' || suffix || ' compress pctfree 0 nologging as
      select
         PK_ISN,
         ID_CODE,
         HORIZONTAL_DATUM
      FROM
         LU_MAD_HDATUM@storetw';

      cleanup(17) := 'drop table LU_MAD_HDATUM' || suffix || ' cascade constraints purge';

      execute immediate
     'create table LU_MAD_VMETHOD' || suffix || ' compress pctfree 0 nologging as
      select
         PK_ISN,
         ID_CODE,
         ELEVATION_METHOD
      FROM
         LU_MAD_VMETHOD@storetw';

      cleanup(18) := 'drop table LU_MAD_VMETHOD' || suffix || ' cascade constraints purge';

      execute immediate
     'create table LU_MAD_VDATUM' || suffix || ' compress pctfree 0 nologging as
      select
         PK_ISN,
         ID_CODE,
         ELEVATION_DATUM
      FROM
         LU_MAD_VDATUM@storetw';

      cleanup(19) := 'drop table LU_MAD_VDATUM' || suffix || ' cascade constraints purge';

      execute immediate
     'create table MT_WH_CONFIG' || suffix || ' compress pctfree 0 nologging as
      select
         PARAMETER_NAME,
         PARAMETER_VALUE,
         APPLICATION,
         DESCRIPTION
      FROM
         MT_WH_CONFIG@storetw';

      cleanup(20) := 'drop table MT_WH_CONFIG' || suffix || ' cascade constraints purge';

      execute immediate
     'create table storet_sum' || suffix || ' compress pctfree 0 nologging as
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
      from /* TODO remove join*/
         fa_regular_result' || suffix || '  result,
         fa_station'        || suffix || '  station,
         di_geo_state'      || suffix || '  state,
         di_geo_county'     || suffix || '  county
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
         /* took out cast(trim(result.characteristic_group_type) as varchar2(80)) ,  */
         cast(trim(station.generated_huc) as varchar2(8)) ,
         cast(trim(station.huctwelvedigitcode) as varchar2(12)) ';

      cleanup(21) := 'drop table STORET_SUM' || suffix || ' cascade constraints purge';
      
      execute immediate
      'create table characteristicname' || suffix || ' compress pctfree 0 nologging as
       select code_value,
              cast(null as varchar2(4000 char)) description,
              rownum sort_order
         from (select distinct characteristic_name code_value
                 from fa_regular_result' || suffix || '
                where characteristic_name is not null
                   order by 1)';
      cleanup(22) := 'drop table characteristicname' || suffix || ' cascade constraints purge';

      append_email_text('creating characteristictype...');
      execute immediate
      'create table characteristictype' || suffix || ' compress pctfree 0 nologging as
       select code_value,
              cast(null as varchar2(4000 char)) description,
              rownum sort_order
         from (select distinct characteristic_group_type code_value
                 from fa_regular_result' || suffix || '
                   order by 1)';
      cleanup(23) := 'drop table characteristictype' || suffix || ' cascade constraints purge';

      append_email_text('creating country...');
      execute immediate
      'create table country' || suffix || ' compress pctfree 0 nologging as
       select code_value,
              description,
              rownum sort_order
         from (select distinct country_cd code_value,
                      country_name description
                 from fa_station' || suffix || '
                   order by country_name desc)';
      cleanup(24) := 'drop table country' || suffix || ' cascade constraints purge';

      append_email_text('creating county...');
      execute immediate
      'create table county' || suffix || ' compress pctfree 0 nologging as
       select code_value,
              description,
              country_cd,
              state_cd,
              rownum sort_order
         from (select distinct country_cd||'':''||state_cd|| '':''||county_cd code_value,
                      country_cd||'', ''||state_name||'', ''||county_name description,
                      country_cd,
                      state_cd,
                      county_cd
                 from fa_station' || suffix || '
                   order by country_cd desc,
                            state_cd,
                            county_cd)';
     cleanup(25) := 'drop table county' || suffix || ' cascade constraints purge';

      execute immediate
      'create table organization' || suffix || ' compress pctfree 0 nologging as
       select code_value,
              description,
              rownum sort_order
         from (select distinct organization_id code_value,
                               organization_name description
                 from fa_station' || suffix || '
                    order by 1)';
      cleanup(26) := 'drop table organization' || suffix || ' cascade constraints purge';

            
      execute immediate
      'create table samplemedia' || suffix || ' compress pctfree 0 nologging as
       select code_value,
              cast(null as varchar2(4000 char)) description,
              rownum sort_order
         from (select distinct activity_medium as code_value
                 from fa_regular_result' || suffix || '
                where fk_act_medium is not null
                   order by activity_medium)';
      cleanup(27) := 'drop table samplemedia' || suffix || ' cascade constraints purge';

            
      execute immediate
      'create table sitetype' || suffix || ' compress pctfree 0 nologging as
       select code_value,
              cast(null as varchar2(4000 char)) description,
              rownum sort_order
         from (select distinct station_group_type code_value
                 from fa_station' || suffix || '
                   order by 1)';
      cleanup(28) := 'drop table sitetype' || suffix || ' cascade constraints purge';

      append_email_text('creating state...');
      execute immediate
      'create table state' || suffix || ' compress pctfree 0 nologging as
       select code_value,
              description_with_country,
              description_with_out_country,
              country_cd,
              rownum sort_order
         from (select distinct country_cd||'':''||state_cd code_value,
                      country_cd||'', ''||state_name description_with_country,
                      state_name description_with_out_country,
                      country_cd,
                      state_cd
                 from fa_station' || suffix || '
                   order by country_cd desc,
                            state_cd)';
      cleanup(29) := 'drop table state' || suffix || ' cascade constraints purge';

   exception
      when others then
         message := 'FAIL to create a lookup: ' || SQLERRM;
         append_email_text(message);
   end create_lookups;

   procedure create_index
   is
      stmt            varchar2(32000);
      table_name      varchar2(   80);
   begin

      append_email_text('creating indexes....');

      table_name := 'FA_STATION' || suffix;

      stmt := 'alter table ' || table_name || ' add constraint pk_' || table_name || ' primary key (pk_isn) using index nologging';
      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index fa_station_org_sta' || suffix || ' on ' ||
               table_name || ' (organization_id || ''-'' || station_id) parallel 4 nologging';
      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index fa_station_fk_geo_state' || suffix || ' on ' ||
               table_name || ' (fk_geo_state) parallel 4 nologging';
      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create index fa_station_fk_geo_county' || suffix || ' on ' ||
               table_name || ' (fk_geo_county) parallel 4 nologging';
      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create index fa_station_stn_grp_type' || suffix || ' on ' ||
               table_name || ' (station_group_type) parallel 4 nologging';
      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create index fa_station_org_id' || suffix || ' on ' ||
               table_name || ' (organization_id) parallel 4 nologging';
      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create index fa_station_gen_huc' || suffix || ' on ' ||
               table_name || ' (generated_huc) parallel 4 nologging';
      append_email_text(stmt);
      execute immediate stmt;

      table_name := 'FA_REGULAR_RESULT' || suffix;

      stmt := 'create bitmap index fa_reg_fk_char' || suffix || ' on ' ||
               table_name || ' (FK_CHAR) local parallel 4 nologging ';
      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index fa_reg_act_med' || suffix || ' on ' ||
               table_name || ' (FK_ACT_MEDIUM) local parallel 4 nologging ';
      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index fa_reg_fk_station' || suffix || ' on ' ||
               table_name || ' (fk_station) local parallel 4 nologging';
      execute immediate stmt;

      stmt := 'create bitmap index fa_reg_fk_org' || suffix || ' on ' ||
               table_name || ' (fk_org) local parallel 4 nologging';
      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index fa_reg_activity_id' || suffix || ' on ' ||
               table_name || ' (activity_id) local parallel 4 nologging';
      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index fa_reg_char_name' || suffix || ' on ' ||
               table_name || ' (characteristic_name) local parallel 4 nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index fa_reg_activity_medium' || suffix || ' on ' ||
               table_name || ' (activity_medium) local parallel 4 nologging';

      append_email_text(stmt);
      execute immediate stmt;

      /* note: this view seems to use the invoker rather than the definer.
               so, must run as STORETMODERN, despite the fact that everything
               else in this large package would work as any user with rights
               to execute the package */
      delete from user_sdo_geom_metadata where table_name = 'FA_STATION' || suffix;
      insert INTO USER_SDO_GEOM_METADATA VALUES('FA_STATION' || suffix, 'GEOM',
                  MDSYS.SDO_DIM_ARRAY( MDSYS.SDO_DIM_ELEMENT('X', -180, 180, 0.005), MDSYS.SDO_DIM_ELEMENT('Y', -90, 90, 0.005)), 8307);
      commit work;

      stmt := 'create index fa_station_geom' || suffix || ' on ' ||
              'FA_STATION' || suffix || ' (GEOM) INDEXTYPE IS MDSYS.SPATIAL_INDEX PARAMETERS (''SDO_INDX_DIMS=2 LAYER_GTYPE="POINT"'')';
      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'alter table ' || 'DI_ACTIVITY_MATRIX' || suffix || ' add constraint pk_di_activity_matrix' || suffix ||
              ' primary key (pk_isn) using index nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'alter table ' || 'DI_ACTIVITY_MEDIUM' || suffix || ' add constraint pk_di_activity_medium' || suffix ||
              ' primary key (pk_isn) using index nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'alter table ' || 'DI_CHARACTERISTIC' || suffix || ' add constraint pk_di_characteristic' || suffix ||
              ' primary key (pk_isn) using index nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'alter table ' || 'DI_GEO_COUNTY' || suffix || ' add constraint pk_di_geo_county' || suffix ||
              ' primary key (pk_isn) using index nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'alter table ' || 'DI_GEO_STATE' || suffix || ' add constraint pk_di_geo_state' || suffix ||
              ' primary key (pk_isn) using index nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'alter table ' || 'DI_ORG' || suffix || ' add constraint pk_di_org' || suffix ||
              ' primary key (pk_isn) using index nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'alter table ' || 'DI_STATN_TYPES' || suffix || ' add constraint pk_di_statn_types' || suffix ||
              ' primary key (pk_isn) using index nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'alter table ' || 'LU_MAD_HMETHOD' || suffix || ' add constraint pk_lu_mad_hmethod' || suffix ||
              ' primary key (pk_isn) using index nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'alter table ' || 'LU_MAD_HDATUM' || suffix || ' add constraint pk_lu_mad_hdatum' || suffix ||
              ' primary key (pk_isn) using index nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'alter table ' || 'LU_MAD_VMETHOD' || suffix || ' add constraint pk_lu_mad_vmethod' || suffix ||
              ' primary key (pk_isn) using index nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'alter table ' || 'LU_MAD_VDATUM' || suffix || ' add constraint pk_lu_mad_vdatum' || suffix ||
              ' primary key (pk_isn) using index nologging';

      append_email_text(stmt);
      execute immediate stmt;

      table_name := 'STORET_STATION_SUM' || suffix;
      stmt := 'create bitmap index storet_station_sum_1' || suffix || ' on ' ||
               table_name || ' (station_id          ) nologging';

      append_email_text(stmt);
      execute immediate stmt;

      delete from user_sdo_geom_metadata where table_name = 'STORET_STATION_SUM' || suffix;

      insert INTO USER_SDO_GEOM_METADATA VALUES('STORET_STATION_SUM' || suffix, 'GEOM',
                  MDSYS.SDO_DIM_ARRAY( MDSYS.SDO_DIM_ELEMENT('X', -180, 180, 0.005), MDSYS.SDO_DIM_ELEMENT('Y', -90, 90, 0.005)), 8307);
      commit work;

      stmt := 'create        index storet_station_sum_2' || suffix || ' on ' ||
               table_name || ' (geom                ) INDEXTYPE IS MDSYS.SPATIAL_INDEX PARAMETERS (''SDO_INDX_DIMS=2 LAYER_GTYPE="POINT"'')';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_station_sum_3' || suffix || ' on ' ||
               table_name || ' (state_cd, county_cd) nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_station_sum_4' || suffix || ' on ' ||
               table_name || ' (station_group_type  ) nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_station_sum_5' || suffix || ' on ' ||
               table_name || ' (organization_id     ) nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_station_sum_6' || suffix || ' on ' ||
               table_name || ' (generated_huc       ) nologging';

      append_email_text(stmt);
      execute immediate stmt;

      table_name := 'STORET_RESULT_SUM' || suffix;

      stmt := 'create bitmap index storet_result_sum_1' || suffix || ' on ' ||
               table_name || ' (fk_station               ) local nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_sum_2' || suffix || ' on ' ||
               table_name || ' (state_cd, county_cd) local nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_sum_3' || suffix || ' on ' ||
               table_name || ' (station_group_type       ) local nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_sum_4' || suffix || ' on ' ||
               table_name || ' (organization_id          ) local nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_sum_5' || suffix || ' on ' ||
               table_name || ' (generated_huc            ) local nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_sum_6' || suffix || ' on ' ||
               table_name || ' (activity_medium          ) local nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_sum_7' || suffix || ' on ' ||
               table_name || ' (characteristic_group_type) local nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_sum_8' || suffix || ' on ' ||
               table_name || ' (display_name             ) local nologging';


      table_name := 'STORET_RESULT_CT_SUM' || suffix;

      stmt := 'create bitmap index storet_result_ct_sum_1' || suffix || ' on ' ||
               table_name || ' (fk_station               ) local nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_ct_sum_2' || suffix || ' on ' ||
               table_name || ' (state_cd, county_cd) local nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_ct_sum_3' || suffix || ' on ' ||
               table_name || ' (station_group_type       ) local nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_ct_sum_4' || suffix || ' on ' ||
               table_name || ' (organization_id          ) local nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_ct_sum_5' || suffix || ' on ' ||
               table_name || ' (generated_huc            ) local nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_ct_sum_6' || suffix || ' on ' ||
               table_name || ' (activity_medium          ) local nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_ct_sum_7' || suffix || ' on ' ||
               table_name || ' (characteristic_group_type) local nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_ct_sum_8' || suffix || ' on ' ||
               table_name || ' (display_name             ) local nologging';

      table_name := 'STORET_RESULT_NR_SUM' || suffix;

      stmt := 'create bitmap index storet_result_nr_sum_1' || suffix || ' on ' ||
               table_name || ' (fk_station               ) local nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_nr_sum_2' || suffix || ' on ' ||
               table_name || ' (activity_medium          ) local nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_nr_sum_3' || suffix || ' on ' ||
               table_name || ' (characteristic_group_type) local nologging';

      append_email_text(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_nr_sum_4' || suffix || ' on ' ||
               table_name || ' (display_name             ) local nologging';

      append_email_text(stmt);
      execute immediate stmt;



      append_email_text('grants...');
      execute immediate 'grant select on fa_station'           || suffix || ' to storetuser, nwq_stg';
      execute immediate 'grant select on fa_regular_result'    || suffix || ' to storetuser, nwq_stg';
      execute immediate 'grant select on di_activity_matrix'   || suffix || ' to storetuser, nwq_stg';
      execute immediate 'grant select on di_activity_medium'   || suffix || ' to storetuser, nwq_stg';
      execute immediate 'grant select on di_characteristic'    || suffix || ' to storetuser, nwq_stg';
      execute immediate 'grant select on di_geo_county'        || suffix || ' to storetuser, nwq_stg';
      execute immediate 'grant select on di_geo_state'         || suffix || ' to storetuser, nwq_stg';
      execute immediate 'grant select on di_org'               || suffix || ' to storetuser, nwq_stg';
      execute immediate 'grant select on di_statn_types'       || suffix || ' to storetuser, nwq_stg';
      execute immediate 'grant select on lu_mad_hmethod'       || suffix || ' to storetuser, nwq_stg';
      execute immediate 'grant select on lu_mad_hdatum'        || suffix || ' to storetuser, nwq_stg';
      execute immediate 'grant select on lu_mad_vmethod'       || suffix || ' to storetuser, nwq_stg';
      execute immediate 'grant select on lu_mad_vdatum'        || suffix || ' to storetuser, nwq_stg';
      execute immediate 'grant select on mt_wh_config'         || suffix || ' to storetuser, nwq_stg';
      execute immediate 'grant select on storet_sum'           || suffix || ' to storetuser, nwq_stg';
      execute immediate 'grant select on storet_station_sum'   || suffix || ' to storetuser, nwq_stg';
      execute immediate 'grant select on storet_result_sum'    || suffix || ' to storetuser, nwq_stg';
      execute immediate 'grant select on storet_result_ct_sum' || suffix || ' to storetuser, nwq_stg';
      execute immediate 'grant select on storet_result_nr_sum' || suffix || ' to storetuser, nwq_stg';
      execute immediate 'grant select on storet_lctn_loc'      || suffix || ' to storetuser, nwq_stg';
      execute immediate 'grant select on characteristicname'   || suffix || ' to storetuser, nwq_stg';
      execute immediate 'grant select on organization'         || suffix || ' to storetuser, nwq_stg';
      execute immediate 'grant select on samplemedia'          || suffix || ' to storetuser, nwq_stg';
      execute immediate 'grant select on sitetype'             || suffix || ' to storetuser, nwq_stg';

      append_email_text('analyze fa_station...');  /* takes about 1.5 minutes*/
      dbms_stats.gather_table_stats('STORETMODERN', 'FA_STATION'          || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      append_email_text('analyze fa_regular_result...');  /* takes about 50 minutes */
      dbms_stats.gather_table_stats('STORETMODERN', 'FA_REGULAR_RESULT'   || suffix, null,  10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      append_email_text('analyze di_activity_medium...');
      dbms_stats.gather_table_stats('STORETMODERN', 'DI_ACTIVITY_MEDIUM'  || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      append_email_text('analyze di_characteristic...');
      dbms_stats.gather_table_stats('STORETMODERN', 'DI_CHARACTERISTIC'   || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      append_email_text('analyze di_geo_county...');
      dbms_stats.gather_table_stats('STORETMODERN', 'DI_GEO_COUNTY'       || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      append_email_text('analyze di_geo_state...');
      dbms_stats.gather_table_stats('STORETMODERN', 'DI_GEO_STATE'        || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      append_email_text('analyze di_org...');
      dbms_stats.gather_table_stats('STORETMODERN', 'DI_ORG'              || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      append_email_text('analyze di_statn_types...');
      dbms_stats.gather_table_stats('STORETMODERN', 'DI_STATN_TYPES'      || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      append_email_text('analyze lu_mad_hmethod...');
      dbms_stats.gather_table_stats('STORETMODERN', 'LU_MAD_HMETHOD'      || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      append_email_text('analyze lu_mad_hdatum...');
      dbms_stats.gather_table_stats('STORETMODERN', 'LU_MAD_HDATUM'       || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      append_email_text('analyze lu_mad_vmethod...');
      dbms_stats.gather_table_stats('STORETMODERN', 'LU_MAD_VMETHOD'      || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      append_email_text('analyze lu_mad_vdatum...');
      dbms_stats.gather_table_stats('STORETMODERN', 'LU_MAD_VDATUM'       || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      append_email_text('analyze di_activity_matrix...');
      dbms_stats.gather_table_stats('STORETMODERN', 'DI_ACTIVITY_MATRIX'  || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      append_email_text('analyze mt_wh_config...');
      dbms_stats.gather_table_stats('STORETMODERN', 'MT_WH_CONFIG'        || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      append_email_text('analyze storet_sum...');
      dbms_stats.gather_table_stats('STORETMODERN', 'STORET_SUM'          || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      append_email_text('analyze storet_station_sum...');
      dbms_stats.gather_table_stats('STORETMODERN', 'STORET_STATION_SUM'  || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      append_email_text('analyze storet_result_sum...');
      dbms_stats.gather_table_stats('STORETMODERN', 'STORET_RESULT_SUM'   || suffix, null,  10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      append_email_text('analyze storet_result_ct_sum...');
      dbms_stats.gather_table_stats('STORETMODERN', 'STORET_RESULT_CT_SUM'|| suffix, null,  10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      append_email_text('analyze storet_result_nr_sum...');
      dbms_stats.gather_table_stats('STORETMODERN', 'STORET_RESULT_NR_SUM'|| suffix, null,  10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      append_email_text('analyze storet_lctn_loc...');
      dbms_stats.gather_table_stats('STORETMODERN', 'STORET_LCTN_LOC'     || suffix, null,  10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      append_email_text('analyze characteristicname...');
      dbms_stats.gather_table_stats('STORETMODERN', 'CHARACTERISTICNAME'  || suffix, null,  10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      append_email_text('analyze organization...');
      dbms_stats.gather_table_stats('STORETMODERN', 'ORGANIZATION'        || suffix, null,  10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      append_email_text('analyze samplemedia...');
      dbms_stats.gather_table_stats('STORETMODERN', 'SAMPLEMEDIA'         || suffix, null,  10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      append_email_text('analyze sitetype...');
      dbms_stats.gather_table_stats('STORETMODERN', 'SITETYPE'            || suffix, null,  10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);


   exception
      when others then
         message := 'FAIL with index: ' || stmt || '  --> ' || SQLERRM;
         append_email_text(message);
   end create_index;

   procedure validate
   is
      old_rows     int;
      new_rows     int;
      index_count  int;
      grant_count  int;
      type cursor_type is ref cursor;
      c            cursor_type;
      query        varchar2(4000);
      pass_fail    varchar2(15);
      situation    varchar2(200);
   begin

      append_email_text('validating...');

      select count(*) into old_rows from fa_regular_result;
      query := 'select count(*) from fa_regular_result' || suffix;
      open c for query;
      fetch c into new_rows;
      close c;

      if new_rows > 110000000 and new_rows > old_rows - 10000000 then
         pass_fail := 'PASS';
      else
         pass_fail := 'FAIL';
      	 $IF $$empty_db $THEN
      	    pass_fail := 'PASS empty_db';
      	 $END
      end if;
      situation := pass_fail || ': table comparison for fa_regular_result: was ' || trim(to_char(old_rows, '999,999,999')) || ', now ' || trim(to_char(new_rows, '999,999,999'));
      append_email_text(situation);
      if pass_fail = 'FAIL' and message is null then
         message := situation;
      end if;

      select count(*) into old_rows from fa_station;
      query := 'select count(*) from fa_station' || suffix;
      open c for query;
      fetch c into new_rows;
      close c;

      if new_rows > 590000 and new_rows > old_rows - 20000 then
         pass_fail := 'PASS';
      else
         pass_fail := 'FAIL';
      	 $IF $$empty_db $THEN
      	    pass_fail := 'PASS empty_db';
      	 $END
      end if;
      situation := pass_fail || ': table comparison for fa_station: was ' || trim(to_char(old_rows, '999,999,999')) || ', now ' || trim(to_char(new_rows, '999,999,999'));
      append_email_text(situation);
      if pass_fail = 'FAIL' and message is null then
         message := situation;
      end if;

      select count(*) into old_rows from di_activity_matrix;
      query := 'select count(*) from di_activity_matrix' || suffix;
      open c for query;
      fetch c into new_rows;
      close c;

      if new_rows > 80 and new_rows > old_rows - 5 then
         pass_fail := 'PASS';
      else
         pass_fail := 'FAIL';
      	 $IF $$empty_db $THEN
      	    pass_fail := 'PASS empty_db';
      	 $END
      end if;
      situation := pass_fail || ': table comparison for di_activity_matrix: was ' || trim(to_char(old_rows, '999,999,999')) || ', now ' || trim(to_char(new_rows, '999,999,999'));
      append_email_text(situation);
      if pass_fail = 'FAIL' and message is null then
         message := situation;
      end if;

      select count(*) into old_rows from di_activity_medium;
      query := 'select count(*) from di_activity_medium' || suffix;
      open c for query;
      fetch c into new_rows;
      close c;

      if new_rows > 8 and new_rows > old_rows - 2 then
         pass_fail := 'PASS';
      else
         pass_fail := 'FAIL';
      	 $IF $$empty_db $THEN
      	    pass_fail := 'PASS empty_db';
      	 $END
      end if;
      situation := pass_fail || ': table comparison for di_activity_medium: was ' || trim(to_char(old_rows, '999,999,999')) || ', now ' || trim(to_char(new_rows, '999,999,999'));
      append_email_text(situation);
      if pass_fail = 'FAIL' and message is null then
         message := situation;
      end if;

      select count(*) into old_rows from di_characteristic;
      query := 'select count(*) from di_characteristic' || suffix;
      open c for query;
      fetch c into new_rows;
      close c;

      if new_rows > 17000 and new_rows > old_rows - 500 then
         pass_fail := 'PASS';
      else
         pass_fail := 'FAIL';
      	 $IF $$empty_db $THEN
      	    pass_fail := 'PASS empty_db';
      	 $END
      end if;
      situation := pass_fail || ': table comparison for di_characteristic: was ' || trim(to_char(old_rows, '999,999,999')) || ', now ' || trim(to_char(new_rows, '999,999,999'));
      append_email_text(situation);
      if pass_fail = 'FAIL' and message is null then
         message := situation;
      end if;

      select count(*) into old_rows from di_geo_county;
      query := 'select count(*) from di_geo_county' || suffix;
      open c for query;
      fetch c into new_rows;
      close c;

      if new_rows > 3400 and new_rows > old_rows - 50 then
         pass_fail := 'PASS';
      else
         pass_fail := 'FAIL';
      	 $IF $$empty_db $THEN
      	    pass_fail := 'PASS empty_db';
      	 $END
      end if;
      situation := pass_fail || ': table comparison for di_geo_county: was ' || trim(to_char(old_rows, '999,999,999')) || ', now ' || trim(to_char(new_rows, '999,999,999'));
      append_email_text(situation);
      if pass_fail = 'FAIL' and message is null then
         message := situation;
      end if;

      select count(*) into old_rows from di_geo_state;
      query := 'select count(*) from di_geo_state' || suffix;
      open c for query;
      fetch c into new_rows;
      close c;

      if new_rows > 100 and new_rows > old_rows - 10 then
         pass_fail := 'PASS';
      else
         pass_fail := 'FAIL';
      	 $IF $$empty_db $THEN
      	    pass_fail := 'PASS empty_db';
      	 $END
      end if;
      situation := pass_fail || ': table comparison for di_geo_state: was ' || trim(to_char(old_rows, '999,999,999')) || ', now ' || trim(to_char(new_rows, '999,999,999'));
      append_email_text(situation);
      if pass_fail = 'FAIL' and message is null then
         message := situation;
      end if;

      select count(*) into old_rows from di_org;
      query := 'select count(*) from di_org' || suffix;
      open c for query;
      fetch c into new_rows;
      close c;

      if new_rows > 1000 and new_rows > old_rows - 50 then
         pass_fail := 'PASS';
      else
         pass_fail := 'FAIL';
      	 $IF $$empty_db $THEN
      	    pass_fail := 'PASS empty_db';
      	 $END
      end if;
      situation := pass_fail || ': table comparison for di_org: was ' || trim(to_char(old_rows, '999,999,999')) || ', now ' || trim(to_char(new_rows, '999,999,999'));
      append_email_text(situation);
      if pass_fail = 'FAIL' and message is null then
         message := situation;
      end if;

      select count(*) into old_rows from di_statn_types;
      query := 'select count(*) from di_statn_types' || suffix;
      open c for query;
      fetch c into new_rows;
      close c;

      if new_rows > 70 and new_rows > old_rows - 5 then
         pass_fail := 'PASS';
      else
         pass_fail := 'FAIL';
      	 $IF $$empty_db $THEN
      	    pass_fail := 'PASS empty_db';
      	 $END
      end if;
      situation := pass_fail || ': table comparison for di_statn_types: was ' || trim(to_char(old_rows, '999,999,999')) || ', now ' || trim(to_char(new_rows, '999,999,999'));
      append_email_text(situation);
      if pass_fail = 'FAIL' and message is null then
         message := situation;
      end if;

      select count(*) into old_rows from lu_mad_hmethod;
      query := 'select count(*) from lu_mad_hmethod' || suffix;
      open c for query;
      fetch c into new_rows;
      close c;

      if new_rows > 38 and new_rows > old_rows - 5 then
         pass_fail := 'PASS';
      else
         pass_fail := 'FAIL';
      	 $IF $$empty_db $THEN
      	    pass_fail := 'PASS empty_db';
      	 $END
      end if;
      situation := pass_fail || ': table comparison for lu_mad_hmethod: was ' || trim(to_char(old_rows, '999,999,999')) || ', now ' || trim(to_char(new_rows, '999,999,999'));
      append_email_text(situation);
      if pass_fail = 'FAIL' and message is null then
         message := situation;
      end if;

      select count(*) into old_rows from lu_mad_hdatum;
      query := 'select count(*) from lu_mad_hdatum' || suffix;
      open c for query;
      fetch c into new_rows;
      close c;

      if new_rows > 15 and new_rows > old_rows - 2 then
         pass_fail := 'PASS';
      else
         pass_fail := 'FAIL';
      	 $IF $$empty_db $THEN
      	    pass_fail := 'PASS empty_db';
      	 $END
      end if;
      situation := pass_fail || ': table comparison for lu_mad_hdatum: was ' || trim(to_char(old_rows, '999,999,999')) || ', now ' || trim(to_char(new_rows, '999,999,999'));
      append_email_text(situation);
      if pass_fail = 'FAIL' and message is null then
         message := situation;
      end if;

      select count(*) into old_rows from lu_mad_vmethod;
      query := 'select count(*) from lu_mad_vmethod' || suffix;
      open c for query;
      fetch c into new_rows;
      close c;

      if new_rows > 14 and new_rows > old_rows - 2 then
         pass_fail := 'PASS';
      else
         pass_fail := 'FAIL';
      	 $IF $$empty_db $THEN
      	    pass_fail := 'PASS empty_db';
      	 $END
      end if;
      situation := pass_fail || ': table comparison for lu_mad_vmethod: was ' || trim(to_char(old_rows, '999,999,999')) || ', now ' || trim(to_char(new_rows, '999,999,999'));
      append_email_text(situation);
      if pass_fail = 'FAIL' and message is null then
         message := situation;
      end if;

      select count(*) into old_rows from lu_mad_vdatum;
      query := 'select count(*) from lu_mad_vdatum' || suffix;
      open c for query;
      fetch c into new_rows;
      close c;

      if new_rows > 5 and new_rows > old_rows - 2 then
         pass_fail := 'PASS';
      else
         pass_fail := 'FAIL';
      	 $IF $$empty_db $THEN
      	    pass_fail := 'PASS empty_db';
      	 $END
      end if;
      situation := pass_fail || ': table comparison for lu_mad_vdatum: was ' || trim(to_char(old_rows, '999,999,999')) || ', now ' || trim(to_char(new_rows, '999,999,999'));
      append_email_text(situation);
      if pass_fail = 'FAIL' and message is null then
         message := situation;
      end if;

      select count(*) into old_rows from mt_wh_config;
      query := 'select count(*) from mt_wh_config' || suffix;
      open c for query;
      fetch c into new_rows;
      close c;

      if new_rows > 30 and new_rows > old_rows - 2 then
         pass_fail := 'PASS';
      else
         pass_fail := 'FAIL';
      	 $IF $$empty_db $THEN
      	    pass_fail := 'PASS empty_db';
      	 $END
      end if;
      situation := pass_fail || ': table comparison for mt_wh_config: was ' || trim(to_char(old_rows, '999,999,999')) || ', now ' || trim(to_char(new_rows, '999,999,999'));
      append_email_text(situation);
      if pass_fail = 'FAIL' and message is null then
         message := situation;
      end if;

      select count(*) into old_rows from storet_sum;
      query := 'select count(*) from storet_sum' || suffix;
      open c for query;
      fetch c into new_rows;
      close c;

      if new_rows > 10000 then
         pass_fail := 'PASS';
      else
         pass_fail := 'FAIL';
      	 $IF $$empty_db $THEN
      	    pass_fail := 'PASS empty_db';
      	 $END
      end if;
      situation := pass_fail || ': table comparison for storet_sum: was ' || trim(to_char(old_rows, '999,999,999')) || ', now ' || trim(to_char(new_rows, '999,999,999'));
      append_email_text(situation);
      if pass_fail = 'FAIL' and message is null then
         message := situation;
      end if;

      query := 'select count(*) from user_indexes where ' || table_list || 
               ' and substr(table_name, -5) = substr(:current_suffix, 2)';
      open  c for query using suffix;
      fetch c into index_count;
      close c;

      if index_count < 54 then  /* there are exactly 54 as of 01MAY2013 */
         pass_fail := 'FAIL';
      else
         pass_fail := 'PASS';
      end if;
      situation := pass_fail || ': found ' || to_char(index_count) || ' indexes.';
      append_email_text(situation);
      if pass_fail = 'FAIL' and message is null then
         message := situation;
      end if;

      query := 'select count(*) from user_tab_privs where ' || table_list || 
               ' and substr(table_name, -5) = substr(:current_suffix, 2)';
      open  c for query using suffix;
      fetch c into grant_count;
      close c;

      if grant_count < 28 then
         pass_fail := 'FAIL';
      else
         pass_fail := 'PASS';
      end if;
      situation := pass_fail || ': found ' || to_char(grant_count) || ' grants.';
      append_email_text(situation);
      if pass_fail = 'FAIL' and message is null then
         message := situation;
      end if;

   exception
      when others then
         message := 'FAIL validation with query problem: ' || query || ' ' || SQLERRM;
         append_email_text(message);

   end validate;

   procedure install
   is
   begin

      append_email_text('installing...');

      execute immediate 'create or replace synonym fa_station           for fa_station'           || suffix;
      execute immediate 'create or replace synonym fa_regular_result    for fa_regular_result'    || suffix;
      execute immediate 'create or replace synonym di_activity_matrix   for di_activity_matrix'   || suffix;
      execute immediate 'create or replace synonym di_activity_medium   for di_activity_medium'   || suffix;
      execute immediate 'create or replace synonym di_characteristic    for di_characteristic'    || suffix;
      execute immediate 'create or replace synonym di_geo_county        for di_geo_county'        || suffix;
      execute immediate 'create or replace synonym di_geo_state         for di_geo_state'         || suffix;
      execute immediate 'create or replace synonym di_org               for di_org'               || suffix;
      execute immediate 'create or replace synonym di_statn_types       for di_statn_types'       || suffix;
      execute immediate 'create or replace synonym lu_mad_hmethod       for lu_mad_hmethod'       || suffix;
      execute immediate 'create or replace synonym lu_mad_hdatum        for lu_mad_hdatum'        || suffix;
      execute immediate 'create or replace synonym lu_mad_vmethod       for lu_mad_vmethod'       || suffix;
      execute immediate 'create or replace synonym lu_mad_vdatum        for lu_mad_vdatum'        || suffix;
      execute immediate 'create or replace synonym mt_wh_config         for mt_wh_config'         || suffix;
      execute immediate 'create or replace synonym storet_sum           for storet_sum'           || suffix;
      execute immediate 'create or replace synonym storet_station_sum   for storet_station_sum'   || suffix;
      execute immediate 'create or replace synonym storet_result_sum    for storet_result_sum'    || suffix;
      execute immediate 'create or replace synonym storet_result_ct_sum for storet_result_ct_sum' || suffix;
      execute immediate 'create or replace synonym storet_result_nr_sum for storet_result_nr_sum' || suffix;
      execute immediate 'create or replace synonym characteristicname   for characteristicname'   || suffix;
      execute immediate 'create or replace synonym organization         for organization'         || suffix;
      execute immediate 'create or replace synonym samplemedia          for samplemedia'          || suffix;
      execute immediate 'create or replace synonym sitetype             for sitetype'             || suffix;
      
      execute immediate 'create or replace synonym storet_lctn_loc_new  for storet_lctn_loc'      || suffix;
      execute immediate 'create or replace synonym storet_lctn_loc_old  for storet_lctn_loc_'
                          || to_char(to_number(substr(suffix, 2) - 1), 'fm00000');

   exception
      when others then
         message := 'FAIL with synonyms: ' || SQLERRM;
         append_email_text(message);

   end install;

   procedure drop_old_stuff
   is
      to_drop cursor_type;
      drop_query varchar2(4000) := 'select table_name from user_tables where ' || table_list ||
            ' and substr(table_name, -5) <= to_char(to_number(substr(:current_suffix, 2) - 2), ''fm00000'')' ||
               ' order by case when table_name like ''FA_STATION%'' then 2 else 1 end, table_name';

      to_nocache cursor_type;
      nocache_query varchar2(4000) := 'select table_name from user_tables where ' || table_list ||
            ' and substr(table_name, -5) <= to_char(to_number(substr(:current_suffix, 2) - 1), ''fm00000'')' ||
               ' order by case when table_name like ''FA_STATION%'' then 2 else 1 end, table_name';

      drop_name    varchar2(30);
      nocache_name varchar2(30);
      stmt         varchar2(80);
   begin

      append_email_text('drop_old_stuff...');

      open to_drop for drop_query using suffix;
      loop
         fetch to_drop into drop_name;
         exit when to_drop%NOTFOUND;
         stmt := 'drop table ' || drop_name || ' cascade constraints purge';
         append_email_text('CLEANUP old stuff: ' || stmt);
         execute immediate stmt;
      end loop;
      close to_drop;

      open to_nocache for nocache_query using suffix;
      loop
         fetch to_nocache into nocache_name;
         exit when to_nocache%NOTFOUND;
         stmt := 'alter table ' || nocache_name || ' nocache';
         append_email_text('CLEANUP old stuff: ' || stmt);
         execute immediate stmt;
      end loop;
      close to_nocache;

   exception
      when others then
         message := 'tried to drop ' || drop_name || ' : ' || SQLERRM;
         append_email_text(message);

   end drop_old_stuff;

   procedure main(mesg in out varchar2, success_notify in varchar2, failure_notify in varchar2) is
      email_subject varchar2(  80);
      email_notify  varchar2( 400);
      k int;
   begin
      message := null;
      dbms_output.enable(100000);

      for k in 1 .. 29 loop
         cleanup(k) := NULL;
      end loop;
      append_email_text('started storet table transformation.');
      determine_suffix;
      if message is null then create_regular_result; end if;
      if message is null then create_station;        end if;
      if message is null then create_lookups;        end if;
      if message is null then create_summaries;      end if;
      if message is null then create_index;          end if;
      if message is null then validate;              end if;
      if message is null then
         install;
      else
         append_email_text('completed. (failed)');
         dbms_output.put_line('errors occurred.');
         email_subject := 'storet load FAILED';
         email_text := email_subject || lf || lf || email_text;
         email_notify := failure_notify;
         for k in 1 .. 29 loop
            if cleanup(k) is not null then
               append_email_text('CLEANUP: ' || cleanup(k));
               execute immediate cleanup(k);
            end if;
         end loop;
      end if;

      if message is null then
         drop_old_stuff;
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
      end if;
      
      $IF $$ci_db $THEN
         dbms_output.put_line('Not emailing from ci database.');
         dbms_output.put_line(email_text);
	  $ELSE
         utl_mail.send@witrans(sender => 'bheck@usgs.gov', recipients => email_notify, subject => email_subject, message => email_text);
      $END
      mesg := message;

   end main;
end create_storet_objects;
/
