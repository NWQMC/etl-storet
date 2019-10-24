with temp_result as (select fa_regular_result."PK_ISN" + 100000000000 result_id,
                fa_regular_result."ORGANIZATION_ID" organization_id,
                fa_regular_result."STATION_ID" station_id,
                fa_regular_result."ACTIVITY_START_DATE_TIME" activity_start_date_time,
                fa_regular_result."ACT_START_TIME_ZONE" act_start_time_zone,
                nullif(trim(fa_regular_result."TRIP_ID"), '') trip_id,
                di_characteristic."CHARACTERISTIC_GROUP_TYPE" characteristic_group_type,
                di_characteristic."DISPLAY_NAME" characteristic_name,
                fa_regular_result."RESULT_VALUE" result_value,
                case
                  when fa_regular_result."RESULT_VALUE" is not null
                    then nullif(trim(fa_regular_result."RESULT_UNIT"), '')
                end result_unit,
                case
                  when fa_regular_result."RESULT_VALUE" is null
                    then nullif(trim(fa_regular_result."RESULT_VALUE_TEXT"), '')
                end result_value_text,
                fa_regular_result."SAMPLE_FRACTION_TYPE" sample_fraction_type,
                nullif(trim(fa_regular_result."RESULT_VALUE_TYPE"), '') result_value_type,
                nullif(trim(fa_regular_result."STATISTIC_TYPE"), '') statistic_type,
                fa_regular_result."RESULT_VALUE_STATUS" result_value_status,
                nullif(trim(fa_regular_result."WEIGHT_BASIS_TYPE"), '') weight_basis_type,
                nullif(trim(fa_regular_result."TEMPERATURE_BASIS_LEVEL"), '') temperature_basis_level,
                nullif(trim(fa_regular_result."DURATION_BASIS"), '') duration_basis,
                nullif(trim(fa_regular_result."ANALYTICAL_PROCEDURE_SOURCE"), '') analytical_procedure_source,
                nullif(trim(fa_regular_result."ANALYTICAL_PROCEDURE_ID"), '') analytical_procedure_id,
                fa_regular_result."LAB_NAME" lab_name,
                fa_regular_result."ANALYSIS_DATE_TIME" analysis_date_time,
                fa_regular_result."DETECTION_LIMIT" detection_limit,
                fa_regular_result."DETECTION_LIMIT_UNIT" detection_limit_unit,
                fa_regular_result."DETECTION_LIMIT_DESCRIPTION" detection_limit_description,
                fa_regular_result."LAB_REMARK" lab_remark,
                fa_regular_result."PARTICLE_SIZE" particle_size,
                fa_regular_result."PRECISION" "precision",
                di_activity_medium."ACTIVITY_MEDIUM" activity_medium,
                fa_regular_result."FK_STATION" fk_station,
                fa_regular_result."FK_ORG" fk_org,
                fa_regular_result."FK_GEO_COUNTY" fk_geo_county,
                fa_regular_result."FK_GEO_STATE" fk_geo_state,
                fa_regular_result."FK_ACT_MEDIUM" fk_act_medium,
                fa_regular_result."FK_ACT_MATRIX" fk_act_matrix,
                fa_regular_result."FK_CHAR" fk_char,
                fa_regular_result."FK_UNIT_CONVERSION" fk_unit_conversion,
                nullif(trim(fa_regular_result."ACTIVITY_ID"), '') activity_id,
                fa_regular_result."REPLICATE_NUMBER" replicate_number,
                fa_regular_result."ACTIVITY_TYPE" activity_type,
                fa_regular_result."ACTIVITY_STOP_DATE_TIME" activity_stop_date_time,
                fa_regular_result."ACT_STOP_TIME_ZONE" act_stop_time_zone,
                fa_regular_result."ACTIVITY_DEPTH" activity_depth,
                fa_regular_result."ACTIVITY_DEPTH_UNIT" activity_depth_unit,
                fa_regular_result."ACTIVITY_UPPER_DEPTH" activity_upper_depth,
                fa_regular_result."ACTIVITY_LOWER_DEPTH" activity_lower_depth,
                fa_regular_result."UPR_LWR_DEPTH_UNIT" upr_lwr_depth_unit,
                coalesce(nullif(trim(fa_regular_result."FIELD_PROCEDURE_ID"), ''), 'USEPA') field_procedure_id,
                coalesce(nullif(trim(fa_regular_result."FIELD_GEAR_ID"), ''), 'Unknown') field_gear_id,
                fa_regular_result."RESULT_COMMENT" result_comment,
                fa_regular_result."ITIS_NUMBER" itis_number,
                fa_regular_result."ACTIVITY_COMMENT" activity_comment,
                fa_regular_result."ACTIVITY_DEPTH_REF_POINT" activity_depth_ref_point,
                coalesce(nullif(trim(fa_regular_result."PROJECT_ID"), ''), 'EPA') project_id, --not tested
                fa_regular_result."RESULT_MEAS_QUAL_CODE" result_meas_qual_code,
                fa_regular_result."ACTIVITY_COND_ORG_TEXT" activity_cond_org_text,
                fa_regular_result."RESULT_DEPTH_MEAS_VALUE" result_depth_meas_value,
                case
                  when fa_regular_result."RESULT_DEPTH_MEAS_VALUE" is not null --not tested
                    then fa_regular_result."RESULT_DEPTH_MEAS_UNIT_CODE"
                end result_depth_meas_unit_code,
                case
                  when fa_regular_result."RESULT_DEPTH_MEAS_VALUE" is not null --not tested
                    then fa_regular_result."RESULT_DEPTH_ALT_REF_PT_TXT"
                end result_depth_alt_ref_pt_txt,
                fa_regular_result."SOURCE_SYSTEM" source_system,
                fa_regular_result."LAB_SAMP_PRP_METHOD_ID" lab_samp_prp_method_id,
                fa_regular_result."LAB_SAMP_PRP_START_DATE_TIME" lab_samp_prp_start_date_time,
                di_activity_matrix."MATRIX_NAME" matrix_name,
                case
                  when fa_regular_result."ACTIVITY_UPPER_DEPTH" is not null
                    then fa_regular_result."UPR_LWR_DEPTH_UNIT"
                end activity_upper_depth_unit,
                case
                  when fa_regular_result."ACTIVITY_LOWER_DEPTH" is not null
                    then fa_regular_result."UPR_LWR_DEPTH_UNIT"
                end activity_lower_depth_unit,
                case
                  when coalesce(fa_regular_result."ACTIVITY_UPPER_DEPTH", fa_regular_result."ACTIVITY_LOWER_DEPTH") is not null
                    then fa_regular_result."ACTIVITY_DEPTH_REF_POINT"
                end activity_uprlwr_depth_ref_pt,
                btrim(fa_regular_result."DETECTION_LIMIT", ' ') myql,
                case
                  when fa_regular_result."DETECTION_LIMIT" is not null
                    then fa_regular_result."DETECTION_LIMIT_UNIT"
                end myqlunits,
                case
                  when fa_regular_result."DETECTION_LIMIT" is not null
                    then fa_regular_result."DETECTION_LIMIT_DESCRIPTION"
                end myqldesc,
                case
                  when nemi.method_id is not null
                    then
                      case nemi.method_type
                        when 'analytical'
                          then 'https://www.nemi.gov/methods/method_summary/' || method_id || '/'
                         when 'statistical'
                           then 'https://www.nemi.gov/methods/sams_method_summary/' || method_id || '/'
                      end
                  else
                    null
                end nemi_url,
                fa_regular_result."ACTIVITY_ISN" activity_isn
           from storetw_dump."FA_REGULAR_RESULT" fa_regular_result
                left join storetw_dump."DI_ACTIVITY_MATRIX" di_activity_matrix
                  on "FK_ACT_MATRIX" = di_activity_matrix."PK_ISN"
                left join storetw_dump."DI_CHARACTERISTIC" di_characteristic
                  on "FK_CHAR" = di_characteristic."PK_ISN"
                left join storetw_dump."DI_ACTIVITY_MEDIUM" di_activity_medium
                  on "FK_ACT_MEDIUM" = di_activity_medium."PK_ISN"
                left join wqx.nemi_wqp_to_epa_crosswalk nemi
                  on trim(fa_regular_result."ANALYTICAL_PROCEDURE_SOURCE") = nemi.analytical_procedure_source and
                     trim(fa_regular_result."ANALYTICAL_PROCEDURE_ID") = nemi.analytical_procedure_id
          where "SOURCE_SYSTEM" is null
        )
insert
  into result_no_source (data_source_id,
                         data_source,
                         station_id,
                         site_id,
                         event_date,
                         analytical_method,
                         activity,
                         characteristic_name,
                         characteristic_type,
                         sample_media,
                         organization,
                         site_type,
                         huc,
                         governmental_unit_code,
                         organization_name,
                         activity_type_code,
                         activity_media_subdiv_name,   --not tested
                         activity_start_time,
                         act_start_time_zone,
                         activity_stop_date,           --not tested
                         activity_stop_time,           --not tested
                         act_stop_time_zone,           --not tested
                         activity_depth,               --not tested
                         activity_depth_unit,          --not tested
                         activity_depth_ref_point,     --not tested
                         activity_upper_depth,         --not tested
                         activity_upper_depth_unit,    --not tested
                         activity_lower_depth,         --not tested
                         activity_lower_depth_unit,    --not tested
                         project_id,
                         activity_conducting_org,      --no values in db/not tested
                         activity_comment,             --not tested
                         sample_collect_method_id,
                         sample_collect_method_ctx,
                         sample_collect_method_name,
                         sample_collect_equip_name,
                         result_id,
                         result_detection_condition_tx,
                         sample_fraction_type,
                         result_measure_value,
                         result_unit,
                         result_meas_qual_code,        --no values in db/not tested
                         result_value_status,
                         statistic_type,               --not tested
                         result_value_type,
                         weight_basis_type,            --not tested
                         duration_basis,               --not tested
                         temperature_basis_level,      --not tested
                         particle_size,                --not tested
                         precision,                    --not tested
                         result_comment,
                         result_depth_meas_value,      --no values in db/not tested
                         result_depth_meas_unit_code,  --no values in db/not tested
                         result_depth_alt_ref_pt_txt,  --no values in db/not tested
                         sample_tissue_taxonomic_name, --no values in db/not tested
                         analytical_procedure_id,
                         analytical_procedure_source,
                         analytical_method_name,       --no values in db/not tested
                         lab_name,
                         analysis_date_time,
                         lab_remark,                   --not tested
                         detection_limit,
                         detection_limit_unit,
                         detection_limit_desc,         --not tested
                         analysis_prep_date_tx,        --no values in db/not tested
                         activity_id
                        )
select 3 data_source_id,
       'STORET' data_source,
       s.station_id,
       s.site_id,
       date_trunc('day', r.activity_start_date_time) event_date,
       r.nemi_url analytical_method,
       concat_ws('-', nullif(trim(s.organization), ''), r.activity_id, r.trip_id, r.replicate_number) activity,
       r.characteristic_name,
       r.characteristic_group_type characteristic_type,
       r.activity_medium sample_media,
       s.organization,
       s.site_type site_type,
       s.huc,
       s.governmental_unit_code,
       s.organization_name,
       r.activity_type activity_type_code,
       r.matrix_name activity_media_subdiv_name,
       to_char(r.activity_start_date_time, 'hh24:mi:ss') activity_start_time,
       r.act_start_time_zone,
       to_char(r.activity_stop_date_time, 'yyyy-mm-dd') activity_stop_date,
       to_char(r.activity_stop_date_time, 'hh24:mi:ss') activity_stop_time,
       r.act_stop_time_zone,
       r.activity_depth,
       r.activity_depth_unit,
       r.activity_depth_ref_point,
       r.activity_upper_depth,
       r.activity_upper_depth_unit,
       r.activity_lower_depth,
       r.activity_lower_depth_unit,
       r.project_id,
       r.activity_cond_org_text activity_conducting_org,
       r.activity_comment,
       r.field_procedure_id sample_collect_method_id,
       r.field_procedure_id sample_collect_method_ctx,
       r.field_procedure_id sample_collect_method_name,
       r.field_gear_id sample_collect_equip_name,
       r.result_id,
       r.result_value_text result_detection_condition_tx,
       r.sample_fraction_type,
       rtrim(to_char(r.result_value, 'FM999999999990.9999999999'), '.') result_measure_value,
       r.result_unit,
       r.result_meas_qual_code,
       r.result_value_status,
       r.statistic_type,
       r.result_value_type,
       r.weight_basis_type,
       r.duration_basis,
       r.temperature_basis_level,
       r.particle_size,
       r.precision,
       r.result_comment,
       r.result_depth_meas_value,
       r.result_depth_meas_unit_code,
       r.result_depth_alt_ref_pt_txt,
       r.itis_number sample_tissue_taxonomic_name,
       r.analytical_procedure_id,
       r.analytical_procedure_source,
       r.lab_samp_prp_method_id analytical_method_name,
       r.lab_name,
       to_char(r.analysis_date_time, 'yyyy-mm-dd') analysis_date_time,
       r.lab_remark,
       r.myql,
       r.myqlunits,
       r.myqldesc,
       to_char(r.lab_samp_prp_start_date_time, 'yyyy-mm-dd') analysis_prep_date_tx,
       r.activity_isn + 100000000000
  from  temp_result r,
        station_no_source s
  where r.fk_station = s.station_id
