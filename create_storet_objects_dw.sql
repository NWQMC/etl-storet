create or replace package create_storet_objects
   authid definer
   as
   procedure main(p_dblink in varchar2);
end create_storet_objects;
/

create or replace package body create_storet_objects
      /*-----------------------------------------------------------------------------------
        package create_storet_objects                                created by barry, 11/2011

        A lot of this package was borrowed from create_nad_objects()

        This package is run after 14 staging tables are loaded into a staging database
        (storetmodern on __stage) from the expdp files downloaded from EPA.  Most of the tables
        are copied "as is" into __DW.

        -----------------------------------------------------------------------------------*/
   as
   suffix varchar2(10);
   type cleanuptable is table of varchar2(80) index by binary_integer;
   cleanup cleanuptable;
   table_list varchar2(4000 char) := 'translate(table_name, ''0123456789'', ''0000000000'') in ' ||
                                     '(''FA_REGULAR_RESULT_00000'',''FA_STATION_00000'',''DI_ACTIVITY_MATRIX_00000'',''DI_ACTIVITY_MEDIUM_00000'',' ||
                                      '''DI_CHARACTERISTIC_00000'',''DI_GEO_COUNTY_00000'',''DI_GEO_STATE_00000'',''DI_ORG_00000'',' ||
                                      '''DI_STATN_TYPES_00000'',''LU_MAD_HMETHOD_00000'',''LU_MAD_HDATUM_00000'',''LU_MAD_VMETHOD_00000'',' ||
                                      '''LU_MAD_VDATUM_00000'',''MT_WH_CONFIG_00000'',''STORET_SUM_00000'',''STORET_STATION_SUM_00000'',''STORET_RESULT_SUMT_00000'',' ||
                                      '''STORET_RESULT_SUM_00000'',''STORET_RESULT_CT_SUM_00000'',''STORET_RESULT_NR_SUM_00000'',''STORET_LCTN_LOC_00000'',' ||
                                      '''CHARACTERISTICNAME_00000'',''CHARACTERISTICTYPE_00000'',''COUNTRY_00000'',''COUNTY_00000'',''ORGANIZATION_00000'',' ||
                                      '''SAMPLEMEDIA_00000'',''STATE_00000'',''SITETYPE_00000'')';
                                      
   type cursor_type is ref cursor;

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


      dbms_output.put_line(systimestamp || ' using ''' || suffix || ''' for suffix.');

      open drop_remnants for query using suffix;
      loop
         fetch drop_remnants into drop_name;
         exit when drop_remnants%NOTFOUND;
         stmt := 'drop table ' || drop_name || ' cascade constraints purge';
         dbms_output.put_line(systimestamp || ' CLEANUP remnants: ' || stmt);
         execute immediate stmt;
      end loop;
   end determine_suffix;

   procedure create_regular_result(p_dblink in varchar2)
   is
   begin

      dbms_output.put_line(systimestamp || ' creating regular_result...');

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
      select *
        from fa_regular_result@' || p_dblink;

      cleanup(1) := 'drop table fa_regular_result' || suffix || ' cascade constraints purge';
   end create_regular_result;

   procedure create_station(p_dblink in varchar2)
   is
   begin

      dbms_output.put_line(systimestamp || ' creating station...');

      execute immediate
     'create table fa_station' || suffix || ' compress pctfree 0 nologging parallel 4 cache as
      select *
        from fa_station@' || p_dblink;

      cleanup(2) := 'drop table fa_station' || suffix || ' cascade constraints purge';
   end create_station;

  procedure create_summaries(p_dblink in varchar2)
   is
   begin

      dbms_output.put_line(systimestamp || ' creating storet_station_sum...');

      execute immediate    /* seem to be problems with parallel 4 so make it parallel 1 */
     'create table storet_station_sum' || suffix || ' pctfree 0 cache compress nologging parallel 1 as
         select *
           from storet_station_sum@' || p_dblink;

      cleanup(3) := 'drop table STORET_STATION_SUM' || suffix || ' cascade constraints purge';

      dbms_output.put_line(systimestamp || ' creating storet_result_sum...');

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
         select *
           from storet_result_sum@' || p_dblink;

      cleanup(4) := 'drop table storet_result_sum' || suffix || ' cascade constraints purge';

      dbms_output.put_line(systimestamp || ' creating storet_result_ct_sum...');

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
        as select *
         from storet_result_ct_sum@' || p_dblink;

      cleanup(5) := 'drop table storet_result_ct_sum' || suffix || ' cascade constraints purge';

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
        as select *
            from storet_result_nr_sum@' || p_dblink;

      cleanup(6) := 'drop table storet_result_nr_sum' || suffix || ' cascade constraints purge';

      dbms_output.put_line(systimestamp || ' creating storet_lctn_loc...');

      execute immediate
     'create table storet_lctn_loc' || suffix || ' compress pctfree 0 nologging parallel 1 as
      select *
        from storet_lctn_loc@' || p_dblink;

      cleanup(7) := 'drop table storet_lctn_loc' || suffix || ' cascade constraints purge';
   end create_summaries;

   procedure create_lookups(p_dblink in varchar2)
   is
   begin

      dbms_output.put_line(systimestamp || ' creating di_activity_matrix');
      execute immediate
     'create table di_activity_matrix' || suffix || ' compress pctfree 0 nologging as
      select *
        from di_activity_matrix@' || p_dblink;
      cleanup(8) := 'drop table di_activity_matrix' || suffix || ' cascade constraints purge';

      dbms_output.put_line(systimestamp || ' creating di_activity_medium');
      execute immediate
     'create table di_activity_medium' || suffix || ' compress pctfree 0 nologging as
      select *
        from di_activity_medium@' || p_dblink;
      cleanup(9) := 'drop table di_activity_medium' || suffix || ' cascade constraints purge';

      dbms_output.put_line(systimestamp || ' creating di_characteristic');
      execute immediate
     'create table di_characteristic' || suffix || ' compress pctfree 0 nologging as
      select *
        from di_characteristic@' || p_dblink;
      cleanup(10) := 'drop table di_characteristic' || suffix || ' cascade constraints purge';

      dbms_output.put_line(systimestamp || ' creating di_geo_county');
      execute immediate
     'create table di_geo_county' || suffix || ' compress pctfree 0 nologging as
      select *
        from di_geo_county@' || p_dblink;
      cleanup(11) := 'drop table di_geo_county' || suffix || ' cascade constraints purge';

      dbms_output.put_line(systimestamp || ' creating di_geo_state');
      execute immediate
     'create table di_geo_state' || suffix || ' compress pctfree 0 nologging as
      select *
        from di_geo_state@' || p_dblink;
      cleanup(12) := 'drop table di_geo_state' || suffix || ' cascade constraints purge';

      dbms_output.put_line(systimestamp || ' creating di_org');
      execute immediate
     'create table di_org' || suffix || ' compress pctfree 0 nologging as
      select *
        from di_org@' || p_dblink;
      cleanup(13) := 'drop table di_org' || suffix || ' cascade constraints purge';

      dbms_output.put_line(systimestamp || ' creating di_statn_types');
      execute immediate
     'create table di_statn_types' || suffix || ' compress pctfree 0 nologging as
      select *
        from di_statn_types@' || p_dblink;
      cleanup(14) := 'drop table di_statn_types' || suffix || ' cascade constraints purge';

      dbms_output.put_line(systimestamp || ' creating lu_mad_hmethod');
      execute immediate
     'create table lu_mad_hmethod' || suffix || ' compress pctfree 0 nologging as
      select *
        from lu_mad_hmethod@' || p_dblink;
      cleanup(15) := 'drop table lu_mad_hmethod' || suffix || ' cascade constraints purge';

      dbms_output.put_line(systimestamp || ' creating lu_mad_hdatum');
      execute immediate
     'create table lu_mad_hdatum' || suffix || ' compress pctfree 0 nologging as
      select *
        from lu_mad_hdatum@' || p_dblink;
      cleanup(16) := 'drop table lu_mad_hdatum' || suffix || ' cascade constraints purge';

      dbms_output.put_line(systimestamp || ' creating lu_mad_vmethod');
      execute immediate
     'create table lu_mad_vmethod' || suffix || ' compress pctfree 0 nologging as
      select *
        from lu_mad_vmethod@' || p_dblink;
      cleanup(17) := 'drop table lu_mad_vmethod' || suffix || ' cascade constraints purge';

      dbms_output.put_line(systimestamp || ' creating lu_mad_vdatum');
      execute immediate
     'create table lu_mad_vdatum' || suffix || ' compress pctfree 0 nologging as
      select *
        from lu_mad_vdatum@' || p_dblink;
      cleanup(18) := 'drop table lu_mad_vdatum' || suffix || ' cascade constraints purge';

      dbms_output.put_line(systimestamp || ' creating mt_wh_config');
      execute immediate
     'create table mt_wh_config' || suffix || ' compress pctfree 0 nologging as
      select *
        from mt_wh_config@' || p_dblink;
      cleanup(19) := 'drop table mt_wh_config' || suffix || ' cascade constraints purge';

      dbms_output.put_line(systimestamp || ' creating storet_sum');
      execute immediate
     'create table storet_sum' || suffix || ' compress pctfree 0 nologging as
      select *
        from storet_sum@' || p_dblink;
      cleanup(20) := 'drop table storet_sum' || suffix || ' cascade constraints purge';
      
      dbms_output.put_line(systimestamp || ' creating characteristicname');
      execute immediate
      'create table characteristicname' || suffix || ' compress pctfree 0 nologging as
       select *
         from characteristicname@' || p_dblink;
      cleanup(21) := 'drop table characteristicname' || suffix || ' cascade constraints purge';

      dbms_output.put_line(systimestamp || ' creating characteristictype');
      execute immediate
      'create table characteristictype' || suffix || ' compress pctfree 0 nologging as
       select *
         from characteristictype@' || p_dblink;
      cleanup(22) := 'drop table characteristictype' || suffix || ' cascade constraints purge';

      dbms_output.put_line(systimestamp || ' creating country');
      execute immediate
      'create table country' || suffix || ' compress pctfree 0 nologging as
       select *
         from country@' || p_dblink;
      cleanup(23) := 'drop table country' || suffix || ' cascade constraints purge';

      dbms_output.put_line(systimestamp || ' creating county');
      execute immediate
      'create table county' || suffix || ' compress pctfree 0 nologging as
       select *
         from county@' || p_dblink;
      cleanup(24) := 'drop table county' || suffix || ' cascade constraints purge';

      dbms_output.put_line(systimestamp || ' creating organization');
      execute immediate
      'create table organization' || suffix || ' compress pctfree 0 nologging as
       select *
         from organization@' || p_dblink;
      cleanup(25) := 'drop table organization' || suffix || ' cascade constraints purge';

      dbms_output.put_line(systimestamp || ' creating samplemedia');
      execute immediate
      'create table samplemedia' || suffix || ' compress pctfree 0 nologging as
       select *
         from samplemedia@' || p_dblink;
      cleanup(26) := 'drop table samplemedia' || suffix || ' cascade constraints purge';
            
      dbms_output.put_line(systimestamp || ' creating sitetype');
      execute immediate
      'create table sitetype' || suffix || ' compress pctfree 0 nologging as
       select *
         from sitetype@' || p_dblink;
      cleanup(27) := 'drop table sitetype' || suffix || ' cascade constraints purge';
            
      dbms_output.put_line(systimestamp || ' creating state');
      execute immediate
      'create table state' || suffix || ' compress pctfree 0 nologging as
       select *
         from state@' || p_dblink;
      cleanup(28) := 'drop table state' || suffix || ' cascade constraints purge';
   end create_lookups;

   procedure create_index
   is
      stmt            varchar2(32000);
      table_name      varchar2(   80);
   begin

      dbms_output.put_line(systimestamp || ' creating indexes....');

      table_name := 'FA_STATION' || suffix;

      stmt := 'alter table ' || table_name || ' add constraint pk_' || table_name || ' primary key (pk_isn) using index nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index fa_station_org_sta' || suffix || ' on ' ||
               table_name || ' (organization_id || ''-'' || station_id) parallel 4 nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index fa_station_fk_geo_state' || suffix || ' on ' ||
               table_name || ' (fk_geo_state) parallel 4 nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create index fa_station_fk_geo_county' || suffix || ' on ' ||
               table_name || ' (fk_geo_county) parallel 4 nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create index fa_station_stn_grp_type' || suffix || ' on ' ||
               table_name || ' (station_group_type) parallel 4 nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create index fa_station_org_id' || suffix || ' on ' ||
               table_name || ' (organization_id) parallel 4 nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create index fa_station_gen_huc' || suffix || ' on ' ||
               table_name || ' (generated_huc) parallel 4 nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      table_name := 'FA_REGULAR_RESULT' || suffix;

      stmt := 'create bitmap index fa_reg_fk_char' || suffix || ' on ' ||
               table_name || ' (FK_CHAR) local parallel 4 nologging ';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index fa_reg_act_med' || suffix || ' on ' ||
               table_name || ' (FK_ACT_MEDIUM) local parallel 4 nologging ';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index fa_reg_fk_station' || suffix || ' on ' ||
               table_name || ' (fk_station) local parallel 4 nologging';
      execute immediate stmt;

      stmt := 'create bitmap index fa_reg_fk_org' || suffix || ' on ' ||
               table_name || ' (fk_org) local parallel 4 nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index fa_reg_activity_id' || suffix || ' on ' ||
               table_name || ' (activity_id) local parallel 4 nologging';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index fa_reg_char_name' || suffix || ' on ' ||
               table_name || ' (characteristic_name) local parallel 4 nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index fa_reg_activity_medium' || suffix || ' on ' ||
               table_name || ' (activity_medium) local parallel 4 nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      /* note: this view seems to use the invoker rather than the definer.
               so, must run as STORETMODERN, despite the fact that everything
               else in this large package would work as any user with rights
               to execute the package */
      delete from user_sdo_geom_metadata where table_name = 'FA_STATION' || suffix;
      insert INTO USER_SDO_GEOM_METADATA VALUES('FA_STATION' || suffix, 'GEOM',
                  MDSYS.SDO_DIM_ARRAY( MDSYS.SDO_DIM_ELEMENT('X', -180, 180, 0.005), MDSYS.SDO_DIM_ELEMENT('Y', -90, 90, 0.005)), 8307);
      commit;

      stmt := 'create index fa_station_geom' || suffix || ' on ' ||
              'FA_STATION' || suffix || ' (GEOM) INDEXTYPE IS MDSYS.SPATIAL_INDEX PARAMETERS (''SDO_INDX_DIMS=2 LAYER_GTYPE="POINT"'')';
      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'alter table ' || 'DI_ACTIVITY_MATRIX' || suffix || ' add constraint pk_di_activity_matrix' || suffix ||
              ' primary key (pk_isn) using index nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'alter table ' || 'DI_ACTIVITY_MEDIUM' || suffix || ' add constraint pk_di_activity_medium' || suffix ||
              ' primary key (pk_isn) using index nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'alter table ' || 'DI_CHARACTERISTIC' || suffix || ' add constraint pk_di_characteristic' || suffix ||
              ' primary key (pk_isn) using index nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'alter table ' || 'DI_GEO_COUNTY' || suffix || ' add constraint pk_di_geo_county' || suffix ||
              ' primary key (pk_isn) using index nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'alter table ' || 'DI_GEO_STATE' || suffix || ' add constraint pk_di_geo_state' || suffix ||
              ' primary key (pk_isn) using index nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'alter table ' || 'DI_ORG' || suffix || ' add constraint pk_di_org' || suffix ||
              ' primary key (pk_isn) using index nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'alter table ' || 'DI_STATN_TYPES' || suffix || ' add constraint pk_di_statn_types' || suffix ||
              ' primary key (pk_isn) using index nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'alter table ' || 'LU_MAD_HMETHOD' || suffix || ' add constraint pk_lu_mad_hmethod' || suffix ||
              ' primary key (pk_isn) using index nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'alter table ' || 'LU_MAD_HDATUM' || suffix || ' add constraint pk_lu_mad_hdatum' || suffix ||
              ' primary key (pk_isn) using index nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'alter table ' || 'LU_MAD_VMETHOD' || suffix || ' add constraint pk_lu_mad_vmethod' || suffix ||
              ' primary key (pk_isn) using index nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'alter table ' || 'LU_MAD_VDATUM' || suffix || ' add constraint pk_lu_mad_vdatum' || suffix ||
              ' primary key (pk_isn) using index nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      table_name := 'STORET_STATION_SUM' || suffix;
      stmt := 'create bitmap index storet_station_sum_1' || suffix || ' on ' ||
               table_name || ' (station_id          ) nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      delete from user_sdo_geom_metadata where table_name = 'STORET_STATION_SUM' || suffix;
      insert INTO USER_SDO_GEOM_METADATA VALUES('STORET_STATION_SUM' || suffix, 'GEOM',
                  MDSYS.SDO_DIM_ARRAY( MDSYS.SDO_DIM_ELEMENT('X', -180, 180, 0.005), MDSYS.SDO_DIM_ELEMENT('Y', -90, 90, 0.005)), 8307);
      commit;

      stmt := 'create        index storet_station_sum_2' || suffix || ' on ' ||
               table_name || ' (geom                ) INDEXTYPE IS MDSYS.SPATIAL_INDEX PARAMETERS (''SDO_INDX_DIMS=2 LAYER_GTYPE="POINT"'')';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_station_sum_3' || suffix || ' on ' ||
               table_name || ' (state_cd, county_cd) nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_station_sum_4' || suffix || ' on ' ||
               table_name || ' (station_group_type  ) nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_station_sum_5' || suffix || ' on ' ||
               table_name || ' (organization_id     ) nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_station_sum_6' || suffix || ' on ' ||
               table_name || ' (generated_huc       ) nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      table_name := 'STORET_RESULT_SUM' || suffix;

      stmt := 'create bitmap index storet_result_sum_1' || suffix || ' on ' ||
               table_name || ' (fk_station               ) local nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_sum_2' || suffix || ' on ' ||
               table_name || ' (state_cd, county_cd) local nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_sum_3' || suffix || ' on ' ||
               table_name || ' (station_group_type       ) local nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_sum_4' || suffix || ' on ' ||
               table_name || ' (organization_id          ) local nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_sum_5' || suffix || ' on ' ||
               table_name || ' (generated_huc            ) local nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_sum_6' || suffix || ' on ' ||
               table_name || ' (activity_medium          ) local nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_sum_7' || suffix || ' on ' ||
               table_name || ' (characteristic_group_type) local nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_sum_8' || suffix || ' on ' ||
               table_name || ' (characteristic_name      ) local nologging';


      table_name := 'STORET_RESULT_CT_SUM' || suffix;

      stmt := 'create bitmap index storet_result_ct_sum_1' || suffix || ' on ' ||
               table_name || ' (fk_station               ) local nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_ct_sum_2' || suffix || ' on ' ||
               table_name || ' (state_cd, county_cd) local nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_ct_sum_3' || suffix || ' on ' ||
               table_name || ' (station_group_type       ) local nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_ct_sum_4' || suffix || ' on ' ||
               table_name || ' (organization_id          ) local nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_ct_sum_5' || suffix || ' on ' ||
               table_name || ' (generated_huc            ) local nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_ct_sum_6' || suffix || ' on ' ||
               table_name || ' (activity_medium          ) local nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_ct_sum_7' || suffix || ' on ' ||
               table_name || ' (characteristic_group_type) local nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_ct_sum_8' || suffix || ' on ' ||
               table_name || ' (characteristic_name      ) local nologging';

      table_name := 'STORET_RESULT_NR_SUM' || suffix;

      stmt := 'create bitmap index storet_result_nr_sum_1' || suffix || ' on ' ||
               table_name || ' (fk_station               ) local nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_nr_sum_2' || suffix || ' on ' ||
               table_name || ' (activity_medium          ) local nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_nr_sum_3' || suffix || ' on ' ||
               table_name || ' (characteristic_group_type) local nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;

      stmt := 'create bitmap index storet_result_nr_sum_4' || suffix || ' on ' ||
               table_name || ' (characteristic_name             ) local nologging';

      dbms_output.put_line(stmt);
      execute immediate stmt;



      dbms_output.put_line(systimestamp || ' grants...');
      execute immediate 'grant select on fa_station'           || suffix || ' to storetuser';
      execute immediate 'grant select on fa_regular_result'    || suffix || ' to storetuser';
      execute immediate 'grant select on di_activity_matrix'   || suffix || ' to storetuser';
      execute immediate 'grant select on di_activity_medium'   || suffix || ' to storetuser';
      execute immediate 'grant select on di_characteristic'    || suffix || ' to storetuser';
      execute immediate 'grant select on di_geo_county'        || suffix || ' to storetuser';
      execute immediate 'grant select on di_geo_state'         || suffix || ' to storetuser';
      execute immediate 'grant select on di_org'               || suffix || ' to storetuser';
      execute immediate 'grant select on di_statn_types'       || suffix || ' to storetuser';
      execute immediate 'grant select on lu_mad_hmethod'       || suffix || ' to storetuser';
      execute immediate 'grant select on lu_mad_hdatum'        || suffix || ' to storetuser';
      execute immediate 'grant select on lu_mad_vmethod'       || suffix || ' to storetuser';
      execute immediate 'grant select on lu_mad_vdatum'        || suffix || ' to storetuser';
      execute immediate 'grant select on mt_wh_config'         || suffix || ' to storetuser';
      execute immediate 'grant select on storet_sum'           || suffix || ' to storetuser';
      execute immediate 'grant select on storet_station_sum'   || suffix || ' to storetuser';
      execute immediate 'grant select on storet_result_sum'    || suffix || ' to storetuser';
      execute immediate 'grant select on storet_result_ct_sum' || suffix || ' to storetuser';
      execute immediate 'grant select on storet_result_nr_sum' || suffix || ' to storetuser';
      execute immediate 'grant select on storet_lctn_loc'      || suffix || ' to storetuser';
      execute immediate 'grant select on characteristicname'   || suffix || ' to storetuser';
      execute immediate 'grant select on characteristictype'   || suffix || ' to storetuser';
      execute immediate 'grant select on country'              || suffix || ' to storetuser';
      execute immediate 'grant select on county'               || suffix || ' to storetuser';
      execute immediate 'grant select on organization'         || suffix || ' to storetuser';
      execute immediate 'grant select on samplemedia'          || suffix || ' to storetuser';
      execute immediate 'grant select on sitetype'             || suffix || ' to storetuser';
      execute immediate 'grant select on state'                || suffix || ' to storetuser';

      dbms_output.put_line(systimestamp || ' analyze fa_station...');  /* takes about 1.5 minutes*/
      dbms_stats.gather_table_stats('STORETMODERN', 'FA_STATION'          || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line(systimestamp || ' analyze fa_regular_result...');  /* takes about 50 minutes */
      dbms_stats.gather_table_stats('STORETMODERN', 'FA_REGULAR_RESULT'   || suffix, null,  10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line(systimestamp || ' analyze di_activity_medium...');
      dbms_stats.gather_table_stats('STORETMODERN', 'DI_ACTIVITY_MEDIUM'  || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line(systimestamp || ' analyze di_characteristic...');
      dbms_stats.gather_table_stats('STORETMODERN', 'DI_CHARACTERISTIC'   || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line(systimestamp || ' analyze di_geo_county...');
      dbms_stats.gather_table_stats('STORETMODERN', 'DI_GEO_COUNTY'       || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line(systimestamp || ' analyze di_geo_state...');
      dbms_stats.gather_table_stats('STORETMODERN', 'DI_GEO_STATE'        || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line(systimestamp || ' analyze di_org...');
      dbms_stats.gather_table_stats('STORETMODERN', 'DI_ORG'              || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line(systimestamp || ' analyze di_statn_types...');
      dbms_stats.gather_table_stats('STORETMODERN', 'DI_STATN_TYPES'      || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line(systimestamp || ' analyze lu_mad_hmethod...');
      dbms_stats.gather_table_stats('STORETMODERN', 'LU_MAD_HMETHOD'      || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line(systimestamp || ' analyze lu_mad_hdatum...');
      dbms_stats.gather_table_stats('STORETMODERN', 'LU_MAD_HDATUM'       || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line(systimestamp || ' analyze lu_mad_vmethod...');
      dbms_stats.gather_table_stats('STORETMODERN', 'LU_MAD_VMETHOD'      || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line(systimestamp || ' analyze lu_mad_vdatum...');
      dbms_stats.gather_table_stats('STORETMODERN', 'LU_MAD_VDATUM'       || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line(systimestamp || ' analyze di_activity_matrix...');
      dbms_stats.gather_table_stats('STORETMODERN', 'DI_ACTIVITY_MATRIX'  || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line(systimestamp || ' analyze mt_wh_config...');
      dbms_stats.gather_table_stats('STORETMODERN', 'MT_WH_CONFIG'        || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line(systimestamp || ' analyze storet_sum...');
      dbms_stats.gather_table_stats('STORETMODERN', 'STORET_SUM'          || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line(systimestamp || ' analyze storet_station_sum...');
      dbms_stats.gather_table_stats('STORETMODERN', 'STORET_STATION_SUM'  || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line(systimestamp || ' analyze storet_result_sum...');
      dbms_stats.gather_table_stats('STORETMODERN', 'STORET_RESULT_SUM'   || suffix, null,  10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line(systimestamp || ' analyze storet_result_ct_sum...');
      dbms_stats.gather_table_stats('STORETMODERN', 'STORET_RESULT_CT_SUM'|| suffix, null,  10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line(systimestamp || ' analyze storet_result_nr_sum...');
      dbms_stats.gather_table_stats('STORETMODERN', 'STORET_RESULT_NR_SUM'|| suffix, null,  10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line(systimestamp || ' analyze storet_lctn_loc...');
      dbms_stats.gather_table_stats('STORETMODERN', 'STORET_LCTN_LOC'     || suffix, null,  10, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line(systimestamp || ' analyze characteristicname...');
      dbms_stats.gather_table_stats('STORETMODERN', 'CHARACTERISTICNAME'  || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line(systimestamp || ' analyze characteristictype...');
      dbms_stats.gather_table_stats('STORETMODERN', 'CHARACTERISTICTYPE'  || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line(systimestamp || ' analyze country...');
      dbms_stats.gather_table_stats('STORETMODERN', 'COUNTRY'             || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line(systimestamp || ' analyze county...');
      dbms_stats.gather_table_stats('STORETMODERN', 'COUNTY'              || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line(systimestamp || ' analyze organization...');
      dbms_stats.gather_table_stats('STORETMODERN', 'ORGANIZATION'        || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line(systimestamp || ' analyze samplemedia...');
      dbms_stats.gather_table_stats('STORETMODERN', 'SAMPLEMEDIA'         || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line(systimestamp || ' analyze sitetype...');
      dbms_stats.gather_table_stats('STORETMODERN', 'SITETYPE'            || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
      dbms_output.put_line(systimestamp || ' analyze state...');
      dbms_stats.gather_table_stats('STORETMODERN', 'STATE'               || suffix, null, 100, false, 'FOR ALL COLUMNS SIZE AUTO', 1, 'ALL', true);
   end create_index;

   function validate return boolean
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
      message      varchar2(200);
   begin

      dbms_output.put_line(systimestamp || ' validating...');

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
      dbms_output.put_line(situation);
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
      dbms_output.put_line(situation);
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
      dbms_output.put_line(situation);
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
      dbms_output.put_line(situation);
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
      dbms_output.put_line(situation);
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
      dbms_output.put_line(situation);
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
      dbms_output.put_line(situation);
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
      dbms_output.put_line(situation);
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
      dbms_output.put_line(situation);
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
      dbms_output.put_line(situation);
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
      dbms_output.put_line(situation);
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
      dbms_output.put_line(situation);
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
      dbms_output.put_line(situation);
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
      dbms_output.put_line(situation);
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
      dbms_output.put_line(situation);
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
      dbms_output.put_line(situation);
      if pass_fail = 'FAIL' and message is null then
         message := situation;
      end if;

      query := 'select count(*) from user_tab_privs where ' || table_list || 
               ' and substr(table_name, -5) = substr(:current_suffix, 2)';
      open  c for query using suffix;
      fetch c into grant_count;
      close c;

      if grant_count < 23 then /* without nwq_stg there are 24... */
         pass_fail := 'FAIL';
      else
         pass_fail := 'PASS';
      end if;
      situation := pass_fail || ': found ' || to_char(grant_count) || ' grants.';
      dbms_output.put_line(situation);
      if pass_fail = 'FAIL' and message is null then
         message := situation;
      end if;

      return message is null;
   end validate;

   procedure install
   is
   begin

      dbms_output.put_line(systimestamp || ' installing...');

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
      execute immediate 'create or replace synonym characteristictype   for characteristictype'   || suffix;
      execute immediate 'create or replace synonym country              for country'              || suffix;
      execute immediate 'create or replace synonym county               for county'               || suffix;
      execute immediate 'create or replace synonym state                for state'                || suffix;
     
      execute immediate 'create or replace synonym storet_lctn_loc_new  for storet_lctn_loc'      || suffix;
      execute immediate 'create or replace synonym storet_lctn_loc_old  for storet_lctn_loc_'
                          || to_char(to_number(substr(suffix, 2) - 1), 'fm00000');
   end install;

   procedure drop_old_stuff
   is
      to_drop cursor_type;
      drop_query varchar2(4000) := 'select table_name from user_tables where ' || table_list ||
            ' and substr(table_name, -5) <= to_char(to_number(substr(:current_suffix, 2) - 2), ''fm00000'')' ||
            ' and substr(table_name, -5) <> ''00000''' ||
               ' order by case when table_name like ''FA_STATION%'' then 2 else 1 end, table_name';

      to_nocache cursor_type;
      nocache_query varchar2(4000) := 'select table_name from user_tables where ' || table_list ||
            ' and substr(table_name, -5) <= to_char(to_number(substr(:current_suffix, 2) - 1), ''fm00000'')' ||
               ' order by case when table_name like ''FA_STATION%'' then 2 else 1 end, table_name';

      drop_name    varchar2(30);
      nocache_name varchar2(30);
      stmt         varchar2(80);
   begin

      dbms_output.put_line(systimestamp || ' drop_old_stuff...');

      open to_drop for drop_query using suffix;
      loop
         fetch to_drop into drop_name;
         exit when to_drop%NOTFOUND;
         stmt := 'drop table ' || drop_name || ' cascade constraints purge';
         dbms_output.put_line(systimestamp || ' CLEANUP old stuff: ' || stmt);
         execute immediate stmt;
         if drop_name like '%STATION%' then
            stmt := 'delete from user_sdo_geom_metadata where table_name = ''' || drop_name || '''';
            dbms_output.put_line(systimestamp || ' CLEANUP old stuff: ' || stmt);
            execute immediate stmt;
         end if;
      end loop;
      close to_drop;

      open to_nocache for nocache_query using suffix;
      loop
         fetch to_nocache into nocache_name;
         exit when to_nocache%NOTFOUND;
         stmt := 'alter table ' || nocache_name || ' nocache';
         dbms_output.put_line(systimestamp || ' CLEANUP old stuff: ' || stmt);
         execute immediate stmt;
      end loop;
      close to_nocache;
   end drop_old_stuff;

   procedure main(p_dblink in varchar2) is
      k int;
   begin
      dbms_output.enable(100000);

      for k in 1 .. 30 loop
         cleanup(k) := NULL;
      end loop;
      dbms_output.put_line(systimestamp || ' started storet table transformation.');
      determine_suffix;
      create_regular_result(p_dblink);
      create_station(p_dblink);
      create_lookups(p_dblink);
      create_summaries(p_dblink);
      create_index;
      if validate then
         install;
         drop_old_stuff;
      else
         raise_application_error(-20666, 'Job failed.');
      end if;
      
   exception
      when others then
         for k in 1 .. 30 loop
            if cleanup(k) is not null then
               dbms_output.put_line('CLEANUP: ' || cleanup(k));
               execute immediate cleanup(k);
            end if;
         end loop;
         raise_application_error(-20666, 'Job failed. ' || sqlerrm);

   end main;
end create_storet_objects;
/
