with temp_result as (select fa_regular_result.pk_isn + 100000000000 result_id,
                fa_regular_result.organization_id,
                fa_regular_result.station_id,
                fa_regular_result.activity_start_date_time,
                fa_regular_result.act_start_time_zone,
                nullif(trim(fa_regular_result.trip_id), '') trip_id,
                di_characteristic.characteristic_group_type,
                di_characteristic.display_name characteristic_name,
                fa_regular_result.result_value,
                case
                  when fa_regular_result.result_value is not null
                    then nullif(trim(fa_regular_result.result_unit), '')
                end result_unit,
                case
                  when fa_regular_result.result_value is null
                    then nullif(trim(fa_regular_result.result_value_text), '')
                end result_value_text,
                fa_regular_result.sample_fraction_type,
                nullif(trim(fa_regular_result.result_value_type), '') result_value_type,
                nullif(trim(fa_regular_result.statistic_type), '') statistic_type,
                fa_regular_result.result_value_status,
                nullif(trim(fa_regular_result.weight_basis_type), '') weight_basis_type,
                nullif(trim(fa_regular_result.temperature_basis_level), '') temperature_basis_level,
                nullif(trim(fa_regular_result.duration_basis), '') duration_basis,
                nullif(trim(fa_regular_result.analytical_procedure_source), '') analytical_procedure_source,
                nullif(trim(fa_regular_result.analytical_procedure_id), '') analytical_procedure_id,
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
                nullif(trim(fa_regular_result.activity_id), '') activity_id,
                fa_regular_result.replicate_number,
                fa_regular_result.activity_type,
                fa_regular_result.activity_stop_date_time,
                fa_regular_result.act_stop_time_zone,
                fa_regular_result.activity_depth,
                fa_regular_result.activity_depth_unit,
                fa_regular_result.activity_upper_depth,
                fa_regular_result.activity_lower_depth,
                fa_regular_result.upr_lwr_depth_unit,
                coalesce(nullif(trim(fa_regular_result.field_procedure_id), ''), 'USEPA') field_procedure_id,
                coalesce(nullif(trim(fa_regular_result.field_gear_id), ''), 'Unknown') field_gear_id,
                fa_regular_result.result_comment,
                fa_regular_result.itis_number,
                fa_regular_result.activity_comment,
                fa_regular_result.activity_depth_ref_point,
                coalesce(nullif(trim(fa_regular_result.project_id), ''), 'EPA') project_id, --not tested
                fa_regular_result.result_meas_qual_code,
                fa_regular_result.activity_cond_org_text,
                fa_regular_result.result_depth_meas_value,
                case
                  when fa_regular_result.result_depth_meas_value is not null --not tested
                    then fa_regular_result.result_depth_meas_unit_code
                end result_depth_meas_unit_code,
                case
                  when fa_regular_result.result_depth_meas_value is not null --not tested
                    then fa_regular_result.result_depth_alt_ref_pt_txt
                end result_depth_alt_ref_pt_txt,
                fa_regular_result.source_system,
                fa_regular_result.lab_samp_prp_method_id,
                fa_regular_result.lab_samp_prp_start_date_time,
                di_activity_matrix.matrix_name,
                case
                  when fa_regular_result.activity_upper_depth is not null
                    then fa_regular_result.upr_lwr_depth_unit
                end activity_upper_depth_unit,
                case
                  when fa_regular_result.activity_lower_depth is not null
                    then fa_regular_result.upr_lwr_depth_unit
                end activity_lower_depth_unit,
                case
                  when coalesce(fa_regular_result.activity_upper_depth, fa_regular_result.activity_lower_depth) is not null
                    then fa_regular_result.activity_depth_ref_point
                end activity_uprlwr_depth_ref_pt,
                substring(fa_regular_result.detection_limit, '[[:space:]].*') myql,
                case
                  when fa_regular_result.detection_limit is not null
                    then fa_regular_result.detection_limit_unit
                end myqlunits,
                case
                  when fa_regular_result.detection_limit is not null
                    then fa_regular_result.detection_limit_description
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
                fa_regular_result.activity_isn
           from storetw.fa_regular_result
                left join storetw.di_activity_matrix
                  on fk_act_matrix = di_activity_matrix.pk_isn
                left join storetw.di_characteristic
                  on fk_char = di_characteristic.pk_isn
                left join storetw.di_activity_medium
                  on fk_act_medium = di_activity_medium.pk_isn
                left join wqx.nemi_wqp_to_epa_crosswalk nemi
                  on trim(storetw.fa_regular_result.analytical_procedure_source) = nemi.analytical_procedure_source and
                     trim(storetw.fa_regular_result.analytical_procedure_id) = nemi.analytical_procedure_id
          where source_system is null
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
       r.result_value result_measure_value,
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
