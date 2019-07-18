insert
  into activity_no_source (data_source_id,
                           data_source,
                           station_id, 
                           site_id,
                           event_date,
                           activity,
                           sample_media,
                           organization,
                           site_type,
                           huc,
                           governmental_unit_code,
                           organization_name,
                           activity_id,
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
                           sample_aqfr_name,
                           hydrologic_condition_name,
                           hydrologic_event_name,
                           sample_collect_method_id,
                           sample_collect_method_ctx,
                           sample_collect_method_name,
                           sample_collect_equip_name
                          )
select distinct data_source_id,
                data_source,
                station_id, 
                site_id,
                event_date,
                activity,
                sample_media,
                organization,
                site_type,
                huc,
                governmental_unit_code,
                organization_name,
                activity_id,
                activity_type_code,
                activity_media_subdiv_name,
                activity_start_time,
                act_start_time_zone,
                activity_stop_date,
                activity_stop_time,
                act_stop_time_zone,
                activity_depth,
                activity_depth_unit,
                activity_depth_ref_point,
                activity_upper_depth,
                activity_upper_depth_unit,
                activity_lower_depth,
                activity_lower_depth_unit,
                project_id,
                activity_conducting_org,
                activity_comment,
                sample_aqfr_name,
                hydrologic_condition_name,
                hydrologic_event_name,
                sample_collect_method_id,
                sample_collect_method_ctx,
                sample_collect_method_name,
                sample_collect_equip_name
  from result_no_source
