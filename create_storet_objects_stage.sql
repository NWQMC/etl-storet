create or replace package create_storet_objects
	authid definer
as
	procedure monthly;
end create_storet_objects;
/

create or replace package body create_storet_objects
/*-----------------------------------------------------------------------------------
package create_storet_objects modified by barry, 04/2015

-----------------------------------------------------------------------------------*/
as
	procedure create_regular_result_no_src
	is
	begin
		dbms_output.put_line(systimestamp || ' creating station_no_source...');
		execute immediate 'truncate table station_no_source';
		execute immediate q'!insert /*+ append */ into station_no_source
                               (data_source_id,
                                data_source,
				station_id,
				site_id,
				organization,
				site_type,
				huc,
                                governmental_unit_code,
				geom,
				station_name,
				organization_name,
				description_text,
				latitude,
				longitude,
				map_scale,
				geopositioning_method,
				hdatum_id_code,
				elevation_value,
				elevation_unit,
				elevation_method,
				vdatum_id_code,
				station_type_name)
	        select /*+ parallel (4) */
                                3 data_source_id,
                                cast('STORET' as varchar2(4000 char)) data_source,
				storetw.fa_station.pk_isn station_id,
				storetw.fa_station.organization_id || '-' || storetw.fa_station.station_id site_id,
				storetw.fa_station.organization_id organization,
				storetw.fa_station.station_group_type site_type,
				storetw.fa_station.generated_huc huc,
                                di_geo_state.country_code || ':' || rtrim(di_geo_state.fips_state_code) || ':' || di_geo_county.fips_county_code governmental_unit_code,
				sdo_cs.transform(storetw.fa_station.geom, 4269) geom,
				trim(storetw.fa_station.station_name) station_name,
				di_org.organization_name,
				trim(storetw.fa_station.description_text) description_text,
				storetw.fa_station.latitude,
				storetw.fa_station.longitude,
				regexp_substr(storetw.fa_station.map_scale, '[[:digit:]]+$') map_scale,
				nvl(lu_mad_hmethod.geopositioning_method, 'Unknown') geopositioning_method,
				nvl(rtrim(lu_mad_hdatum.id_code), 'Unknown') hdatum_id_code,
				regexp_substr(storetw.fa_station.elevation, '^[[:digit:]]+') elevation_value,
				nvl2(storetw.fa_station.elevation, nvl(storetw.fa_station.elevation_unit, 'ft'), null) elevation_unit,
				nvl2(storetw.fa_station.elevation, lu_mad_vmethod.elevation_method, null) elevation_method,
				nvl2(storetw.fa_station.elevation, nvl(lu_mad_vdatum.id_code, 'Unknown'), null) vdatum_id_code,
				storetw.fa_station.fk_primary_type station_type_name
			from
				storetw.fa_station
				left join storetw.di_org
					on fk_org = di_org.pk_isn
				left join storetw.di_geo_state
					on fk_geo_state = di_geo_state.pk_isn
				left join storetw.di_geo_county
					on fk_geo_county = di_geo_county.pk_isn
				left join storetw.lu_mad_hmethod
					on fk_mad_hmethod = lu_mad_hmethod.pk_isn
				left join storetw.lu_mad_hdatum
					on fk_mad_hdatum = lu_mad_hdatum.pk_isn
				left join storetw.lu_mad_vmethod
					on fk_mad_vmethod = lu_mad_vmethod.pk_isn
				left join storetw.lu_mad_vdatum
					on fk_mad_vdatum = lu_mad_vdatum.pk_isn
			where storetw.fa_station.location_point_type = '*POINT OF RECORD'!';

		dbms_output.put_line(systimestamp || ' creating pc_result_no_source...');
		execute immediate 'truncate table pc_result_no_source';
		execute immediate q'!insert /*+ append */ into pc_result_no_source
      (data_source_id,
       data_source,
       station_id,
       site_id,
       event_date,
       analytical_method,
       p_code,
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
       sample_collect_equip_name,
       result_id,
       result_detection_condition_tx,
       sample_fraction_type,
       result_measure_value,
       result_unit,
       result_meas_qual_code,
       result_value_status,
       statistic_type,
       result_value_type,
       weight_basis_type,
       duration_basis,
       temperature_basis_level,
       particle_size,
       precision,
       result_comment,
       result_depth_meas_value,
       result_depth_meas_unit_code,
       result_depth_alt_ref_pt_txt,
       sample_tissue_taxonomic_name,
       sample_tissue_anatomy_name,
       analytical_procedure_id,
       analytical_procedure_source,
       analytical_method_name,
       analytical_method_citation,
       lab_name,
       analysis_date_time,
       lab_remark,
       detection_limit,
       detection_limit_unit,
       detection_limit_desc,
       analysis_prep_date_tx)
   select /*+ full(r) use_hash(r) parallel(r, 4) full(s) use_hash(s) parallel(s, 4) */
       3 data_source_id,
       cast('STORET' as varchar2(4000 char)) data_source,
       s.station_id,
       s.site_id,
       cast(trunc(r.activity_start_date_time) as date) event_date,
       cast(r.nemi_url as varchar2(4000 char)) analytical_method,
       cast(null as varchar2(4000 char)) p_code,
       cast(s.organization || '-' || r.activity_id || nvl2(r.trip_id,'-' || r.trip_id,null) || 
          nvl2(r.replicate_number,'-' || r.replicate_number,null) as varchar2(4000 char)) activity,
       cast(r.characteristic_name as varchar2(4000 char)) characteristic_name,
       cast(r.characteristic_group_type as varchar2(4000 char)) characteristic_type,
       cast(r.activity_medium as varchar2(4000 char)) sample_media,
       s.organization,
       cast(s.site_type as varchar2(4000 char)) site_type,
       s.huc,
       s.governmental_unit_code,
       s.organization_name,
       cast(r.activity_type as varchar2(4000 char)) activity_type_code,
       cast(r.matrix_name as varchar2(4000 char)) activity_media_subdiv_name,
       cast(to_char(r.activity_start_date_time, 'hh24:mi:ss') as varchar2(4000 char)) activity_start_time,
       cast(r.act_start_time_zone as varchar2(4000 char)) act_start_time_zone,
       cast(to_char(r.activity_stop_date_time, 'yyyy-mm-dd') as varchar2(4000 char)) activity_stop_date,
       cast(to_char(r.activity_stop_date_time, 'hh24:mi:ss') as varchar2(4000 char)) activity_stop_time,
       cast(r.act_stop_time_zone as varchar2(4000 char)) act_stop_time_zone,
       cast(r.activity_depth as varchar2(4000 char)) activity_depth,
       cast(r.activity_depth_unit as varchar2(4000 char)) activity_depth_unit,
       cast(r.activity_depth_ref_point as varchar2(4000 char)) activity_depth_ref_point,
       cast(r.activity_upper_depth as varchar2(4000 char)) activity_upper_depth,
       cast(r.activity_upper_depth_unit as varchar2(4000 char)) activity_upper_depth_unit,
       cast(r.activity_lower_depth as varchar2(4000 char)) activity_lower_depth,
       cast(r.activity_lower_depth_unit as varchar2(4000 char)) activity_lower_depth_unit,
       cast(r.project_id as varchar2(4000 char)) project_id,
       cast(r.activity_cond_org_text as varchar2(4000 char)) activity_conducting_org,
       cast(r.activity_comment as varchar2(4000 char)) activity_comment,
       cast(null as varchar2(4000 char)) sample_aqfr_name,
       cast(null as varchar2(4000 char)) hydrologic_condition_name,
       cast(null as varchar2(4000 char)) hydrologic_event_name,
       cast(r.field_procedure_id as varchar2(4000 char)) sample_collect_method_id,
       cast(r.field_procedure_id as varchar2(4000 char)) sample_collect_method_ctx,
       cast(r.field_procedure_id as varchar2(4000 char)) sample_collect_method_name,
       cast(r.field_gear_id as varchar2(4000 char)) sample_collect_equip_name,
       cast(rownum as varchar2(4000 char)) result_id,
       cast(r.result_value_text as varchar2(4000 char)) result_detection_condition_tx,
       cast(r.sample_fraction_type as varchar2(4000 char)) sample_fraction_type,
       cast(r.result_value as varchar2(4000 char)) result_measure_value,
       cast(r.result_unit as varchar2(4000 char)) result_unit,
       cast(r.result_meas_qual_code as varchar2(4000 char)) result_meas_qual_code,
       cast(r.result_meas_qual_code as varchar2(4000 char)) result_value_status,
       cast(r.statistic_type as varchar2(4000 char)) statistic_type,
       cast(r.result_value_type as varchar2(4000 char)) result_value_type,
       cast(r.weight_basis_type as varchar2(4000 char)) weight_basis_type,
       cast(r.duration_basis as varchar2(4000 char)) duration_basis,
       cast(r.temperature_basis_level as varchar2(4000 char)) temperature_basis_level,
       cast(r.particle_size as varchar2(4000 char)) particle_size,
       cast(r.precision as varchar2(4000 char)) precision,
       cast(r.result_comment as varchar2(4000 char)) result_comment,
       cast(r.result_depth_meas_value as varchar2(4000 char)) result_depth_meas_value,
       cast(r.result_depth_meas_unit_code as varchar2(4000 char)) result_depth_meas_unit_code,
       cast(r.result_depth_alt_ref_pt_txt as varchar2(4000 char)) result_depth_alt_ref_pt_txt,
       cast(r.itis_number as varchar2(4000 char)) sample_tissue_taxonomic_name,
       cast(null as varchar2(4000 char)) sample_tissue_anatomy_name,
       cast(r.analytical_procedure_id as varchar2(4000 char)) analytical_procedure_id,
       cast(r.analytical_procedure_source as varchar2(4000 char)) analytical_procedure_source,
       cast(r.lab_samp_prp_method_id as varchar2(4000 char)) analytical_method_name,
       cast(null as varchar2(4000 char)) analytical_method_citation,
       cast(r.lab_name as varchar2(4000 char)) lab_name,
       cast(to_char(r.analysis_date_time, 'yyyy-mm-dd') as varchar2(4000 char)) analysis_date_time,
       cast(r.lab_remark as varchar2(4000 char)) lab_remark,
       cast(r.myql as varchar2(4000 char)) myql,
       cast(r.myqlunits as varchar2(4000 char)) myqlunits,
       cast(r.myqldesc as varchar2(4000 char)) myqldesc,
       cast(to_char(r.lab_samp_prp_start_date_time, 'yyyy-mm-dd') as varchar2(4000 char)) analysis_prep_date_tx
  from 
     (
			select /*+ parallel (4) */
				storetw.fa_regular_result.pk_isn,
				storetw.fa_regular_result.organization_id,
				storetw.fa_regular_result.station_id,
				storetw.fa_regular_result.activity_start_date_time,
				storetw.fa_regular_result.act_start_time_zone,
				storetw.fa_regular_result.trip_id,
				di_characteristic.characteristic_group_type,
				di_characteristic.display_name characteristic_name,
				storetw.fa_regular_result.result_value,
				nvl2(storetw.fa_regular_result.result_value, storetw.fa_regular_result.result_unit, null) result_unit,
				nvl2(storetw.fa_regular_result.result_value, null, storetw.fa_regular_result.result_value_text) result_value_text,
				storetw.fa_regular_result.sample_fraction_type,
				storetw.fa_regular_result.result_value_type,
				storetw.fa_regular_result.statistic_type,
				storetw.fa_regular_result.result_value_status,
				storetw.fa_regular_result.weight_basis_type,
				storetw.fa_regular_result.temperature_basis_level,
				storetw.fa_regular_result.duration_basis,
				trim(storetw.fa_regular_result.analytical_procedure_source) analytical_procedure_source,
				trim(storetw.fa_regular_result.analytical_procedure_id) analytical_procedure_id,
				storetw.fa_regular_result.lab_name,
				storetw.fa_regular_result.analysis_date_time,
				storetw.fa_regular_result.detection_limit,
				storetw.fa_regular_result.detection_limit_unit,
				storetw.fa_regular_result.detection_limit_description,
				storetw.fa_regular_result.lab_remark,
				storetw.fa_regular_result.particle_size,
				storetw.fa_regular_result.precision,
				di_activity_medium.activity_medium,
				storetw.fa_regular_result.fk_station,
				storetw.fa_regular_result.fk_org,
				storetw.fa_regular_result.fk_geo_county,
				storetw.fa_regular_result.fk_geo_state,
				storetw.fa_regular_result.fk_act_medium,
				storetw.fa_regular_result.fk_act_matrix,
				storetw.fa_regular_result.fk_char,
				storetw.fa_regular_result.fk_unit_conversion,
				storetw.fa_regular_result.activity_id,
				storetw.fa_regular_result.replicate_number,
				storetw.fa_regular_result.activity_type,
				storetw.fa_regular_result.activity_stop_date_time,
				storetw.fa_regular_result.act_stop_time_zone,
				storetw.fa_regular_result.activity_depth,
				storetw.fa_regular_result.activity_depth_unit,
				storetw.fa_regular_result.activity_upper_depth,
				storetw.fa_regular_result.activity_lower_depth,
				storetw.fa_regular_result.upr_lwr_depth_unit,
				nvl(storetw.fa_regular_result.field_procedure_id, 'USEPA') field_procedure_id,
				nvl(storetw.fa_regular_result.field_gear_id, 'Unknown') field_gear_id,
				storetw.fa_regular_result.result_comment,
				storetw.fa_regular_result.itis_number,
				storetw.fa_regular_result.activity_comment,
				storetw.fa_regular_result.activity_depth_ref_point,
				nvl(storetw.fa_regular_result.project_id, 'EPA') project_id,
				storetw.fa_regular_result.result_meas_qual_code,
				storetw.fa_regular_result.activity_cond_org_text,
				storetw.fa_regular_result.result_depth_meas_value,
				nvl2(storetw.fa_regular_result.result_depth_meas_value, storetw.fa_regular_result.result_depth_meas_unit_code, null) result_depth_meas_unit_code,
				nvl2(storetw.fa_regular_result.result_depth_meas_value, storetw.fa_regular_result.result_depth_alt_ref_pt_txt, null) result_depth_alt_ref_pt_txt,
				storetw.fa_regular_result.source_system,
				storetw.fa_regular_result.lab_samp_prp_method_id,
				storetw.fa_regular_result.lab_samp_prp_start_date_time,
				di_activity_matrix.matrix_name,
				nvl2(storetw.fa_regular_result.activity_upper_depth, storetw.fa_regular_result.upr_lwr_depth_unit, null) activity_upper_depth_unit,
				nvl2(storetw.fa_regular_result.activity_lower_depth, storetw.fa_regular_result.upr_lwr_depth_unit, null) activity_lower_depth_unit,
				nvl2(coalesce(storetw.fa_regular_result.activity_upper_depth, storetw.fa_regular_result.activity_lower_depth), storetw.fa_regular_result.activity_depth_ref_point, null) activity_uprlwr_depth_ref_pt,
				regexp_replace(storetw.fa_regular_result.detection_limit, '[[:space:]].*') myql,
				nvl2(storetw.fa_regular_result.detection_limit, storetw.fa_regular_result.detection_limit_unit, null) myqlunits,
				nvl2(storetw.fa_regular_result.detection_limit, storetw.fa_regular_result.detection_limit_description, null) myqldesc,
				cast(case
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
					end as varchar2(256 char)) nemi_url
			from
				storetw.fa_regular_result
				left join storetw.di_activity_matrix
					on fk_act_matrix = di_activity_matrix.pk_isn
				left join storetw.di_characteristic
					on fk_char = di_characteristic.pk_isn
				left join storetw.di_activity_medium
					on fk_act_medium = di_activity_medium.pk_isn
				left join wqp_nemi_epa_crosswalk nemi
					on trim(storetw.fa_regular_result.analytical_procedure_source) = nemi.analytical_procedure_source
					and trim(storetw.fa_regular_result.analytical_procedure_id) = nemi.analytical_procedure_id
                        where source_system is null
      ) r,
      station_no_source s
  where     
       r.fk_station = s.station_id
  order by r.fk_station!';
		
		commit;

	end create_regular_result_no_src;
	
	
	procedure monthly
	is
	begin
		dbms_output.enable(100000);
		dbms_output.put_line(systimestamp || ' started storet table transformation.');
		create_regular_result_no_src;
		dbms_output.put_line(systimestamp || ' completed. (success)');
	end monthly;
end create_storet_objects;
/
