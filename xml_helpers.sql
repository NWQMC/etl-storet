create or replace package xml_helpers as
  function organization (organization_id   di_org.organization_id%type,
                         organization_name di_org.organization_name%type)
    return clob deterministic;

    function biological_activity (organization_id                fa_biological_result.organization_id%type,
                                activity_id                    fa_biological_result.activity_id%type,
                                trip_id                        fa_biological_result.trip_id%type,
                                replicate_number               fa_biological_result.replicate_number%type,
                                activity_type                  fa_biological_result.activity_type%type,
                                activity_medium                fa_biological_result.activity_medium%type,
                                activity_start_date_time       fa_biological_result.activity_start_date_time%type,
                                act_start_time_zone            fa_biological_result.act_start_time_zone%type,
                                activity_stop_date_time        fa_biological_result.activity_stop_date_time%type,
                                act_stop_time_zone             fa_biological_result.act_stop_time_zone%type,
                                activity_rel_depth             fa_biological_result.activity_rel_depth%type,
                                activity_depth                 fa_biological_result.activity_depth%type,
                                activity_depth_unit            fa_biological_result.activity_depth_unit%type,
                                activity_upper_depth           fa_biological_result.activity_upper_depth%type,
                                upr_lwr_depth_unit             fa_biological_result.upr_lwr_depth_unit%type,
                                activity_lower_depth           fa_biological_result.activity_lower_depth%type,
                                activity_depth_ref_point       fa_biological_result.activity_depth_ref_point%type,
                                project_id                     fa_biological_result.project_id%type,
                                activity_cond_org_text         fa_biological_result.activity_cond_org_text%type,
                                station_id                     fa_biological_result.station_id%type,
                                activity_comment               fa_biological_result.activity_comment%type,
                                activity_latitude              fa_biological_result.activity_latitude%type,
                                activity_longitude             fa_biological_result.activity_longitude%type,
                                map_scale                      fa_biological_result.map_scale%type,
                                horizontal_accuracy_measure    fa_biological_result.horizontal_accuracy_measure%type, 
                                fk_act_mad_hmethod             fa_biological_result.fk_act_mad_hmethod%type,
                                fk_act_mad_hdatum              fa_biological_result.fk_act_mad_hdatum%type,
                                activity_community             fa_biological_result.activity_community%type,
                                sampling_duration              fa_biological_result.sampling_duration%type,
                                place_in_series                fa_biological_result.place_in_series%type,
                                reach_length                   fa_biological_result.reach_length%type,
                                reach_width                    fa_biological_result.reach_width%type,
                                pass_count                     fa_biological_result.pass_count%type,
                                trap_net_comment               fa_biological_result.trap_net_comment%type,
                                non_tow_current_speed          fa_biological_result.non_tow_current_speed%type,
                                non_tow_net_surface_area       fa_biological_result.non_tow_net_surface_area%type,
                                tow_net_surface_area           fa_biological_result.tow_net_surface_area%type,
                                non_tow_net_mesh_size          fa_biological_result.non_tow_net_mesh_size%type,
                                tow_net_mesh_size              fa_biological_result.tow_net_mesh_size%type,
                                boat_speed                     fa_biological_result.boat_speed%type,
                                toxicity_test_type             fa_biological_result.toxicity_test_type%type,
                                field_procedure_id             fa_biological_result.field_procedure_id%type,
                                field_gear_id                  fa_biological_result.field_gear_id%type,
                                field_prep_procedure_id        fa_biological_result.field_prep_procedure_id%type,
                                container_desc                 fa_biological_result.container_desc%type,
                                presrv_strge_prcdr             fa_biological_result.presrv_strge_prcdr%type,
                                temp_preservn_type             fa_biological_result.temp_preservn_type%type,
                                smprp_transport_storage_desc   fa_biological_result.smprp_transport_storage_desc%type                                
                               )
    return clob deterministic;

/*  function regular_result (result_value_text            fa_regular_result.result_value_text%type,
                           characteristic_name          fa_regular_result.characteristic_name%type,
                           sample_fraction_type         fa_regular_result.sample_fraction_type%type,
                           result_value                 fa_regular_result.result_value%type,
                           result_unit                  fa_regular_result.result_unit%type,
                           result_meas_qual_code        fa_regular_result.result_meas_qual_code%type,
                           result_value_status          fa_regular_result.result_value_status%type,
                           statistic_type               fa_regular_result.statistic_type%type,
                           result_value_type            fa_regular_result.result_value_type%type,
                           weight_basis_type            fa_regular_result.weight_basis_type%type,
                           duration_basis               fa_regular_result.duration_basis%type,
                           temperature_basis_level      fa_regular_result.temperature_basis_level%type,
                           particle_size                fa_regular_result.particle_size%type,
                           precision                    fa_regular_result.precision%type,
                           result_comment               fa_regular_result.result_comment%type,
                           result_depth_meas_value      fa_regular_result.result_depth_meas_value%type,
                           result_depth_meas_unit_code  fa_regular_result.result_depth_meas_unit_code%type,
                           result_depth_alt_ref_pt_txt  fa_regular_result.result_depth_alt_ref_pt_txt%type,
                           itis_number                  fa_regular_result.itis_number%type,
                           analytical_procedure_id      fa_regular_result.analytical_procedure_id%type,
                           analytical_procedure_source  fa_regular_result.analytical_procedure_source%type,
                           lab_samp_prp_method_id       fa_regular_result.lab_samp_prp_method_id%type,
                           lab_name                     fa_regular_result.lab_name%type,
                           analysis_date_time           fa_regular_result.analysis_date_time%type,
                           lab_remark                   fa_regular_result.lab_remark%type,
                           myqldesc                     fa_regular_result.myqldesc%type,
                           myql                         fa_regular_result.myql%type,
                           myqlunits                    fa_regular_result.myqlunits%type,
                           lab_samp_prp_start_date_time fa_regular_result.lab_samp_prp_start_date_time%type
                          )
    return clob deterministic;*/

  function biological_result (result_value_text             fa_biological_result.result_value_text%type,
                              characteristic_name           fa_biological_result.characteristic_name%type,
                              sample_fraction_type          fa_biological_result.sample_fraction_type%type,
                              result_value                  fa_biological_result.result_value%type,
                              result_unit                   fa_biological_result.result_unit%type,
                              result_meas_qual_code         fa_biological_result.result_meas_qual_code%type,
                              result_value_status           fa_biological_result.result_value_status%type,
                              statistic_type                fa_biological_result.statistic_type%type,
                              result_value_type             fa_biological_result.result_value_type%type,
                              weight_basis_type             fa_biological_result.weight_basis_type%type,
                              duration_basis                fa_biological_result.duration_basis%type,
                              temperature_basis_level       fa_biological_result.temperature_basis_level%type,
                              particle_size                 fa_biological_result.particle_size%type,
                              precision                     fa_biological_result.precision%type,
                              bias                          fa_biological_result.bias%type,
                              confidence_level              fa_biological_result.confidence_level%type,
                              result_comment                fa_biological_result.result_comment%type,
                              result_depth_meas_value       fa_biological_result.result_depth_meas_value%type,
                              result_depth_meas_unit_code   fa_biological_result.result_depth_meas_unit_code%type,
                              result_depth_alt_ref_pt_txt   fa_biological_result.result_depth_alt_ref_pt_txt%type,
                              sampling_point_name           fa_biological_result.sampling_point_name%type,
                              activity_intent               fa_biological_result.activity_intent%type,
                              individual_number             fa_biological_result.individual_number%type,
                              activity_subject_taxon        fa_biological_result.activity_subject_taxon%type,
                              species_id                    fa_biological_result.species_id%type,
                              biopart_name                  fa_biological_result.biopart_name%type,
                              result_group_summary_ct_wt    fa_biological_result.result_group_summary_ct_wt%type,
                              cell_form                     fa_biological_result.cell_form%type,
                              cell_shape                    fa_biological_result.cell_shape%type,
                              habit                         fa_biological_result.habit%type,
                              voltinism                     fa_biological_result.voltinism%type,
                              pollution_tolerance           fa_biological_result.pollution_tolerance%type,
                              pollution_tolerance_scale     fa_biological_result.pollution_tolerance_scale%type,
                              trophic_level                 fa_biological_result.trophic_level%type,
                              feeding_group                 fa_biological_result.feeding_group%type,
                              analytical_procedure_id       fa_biological_result.analytical_procedure_id%type,
                              analytical_procedure_source   fa_biological_result.analytical_procedure_source%type,
                              analytical_method_list_agency fa_biological_result.analytical_method_list_agency%type,
                              lab_name                      fa_biological_result.lab_name%type,
                              analysis_date_time            fa_biological_result.analysis_date_time%type,
                              analysis_time_zone            fa_biological_result.analysis_time_zone%type,
                              analysis_end_date_time        fa_biological_result.analysis_end_date_time%type,
                              analysis_end_time_zone        fa_biological_result.analysis_end_time_zone%type,
                              lab_remark                    fa_biological_result.lab_remark%type,
                              all_result_detection_limit    fa_biological_result.all_result_detection_limit%type,
                              detection_limit               fa_biological_result.detection_limit%type,
                              detection_limit_description   fa_biological_result.detection_limit_description%type,
                              detection_limit_unit          fa_biological_result.detection_limit_unit%type,
                              lower_quantitation_limit      fa_biological_result.lower_quantitation_limit%type,
                              upper_quantitation_limit      fa_biological_result.upper_quantitation_limit%type,
                              lab_certified                 fa_biological_result.lab_certified%type,
                              lab_accred_authority          fa_biological_result.lab_accred_authority%type,
                              taxonomist_accred_yn          fa_biological_result.taxonomist_accred_yn%type,
                              taxonomist_accred_authority   fa_biological_result.taxonomist_accred_authority%type
                             )
      return clob deterministic;

  function strip_bad( p_string in varchar2 )
    return varchar2 deterministic;

end xml_helpers;
/

create or replace package body xml_helpers as
  function organization (organization_id   di_org.organization_id%type,
                         organization_name di_org.organization_name%type)
    return clob deterministic is
    rtn clob;
  begin
    select xmlserialize(content (xmlelement("OrganizationDescription",
                                            xmlelement("OrganizationIdentifier", organization_id),
                                            xmlelement("OrganizationFormalName", organization_name)
                                           )
                                ) as clob no indent
                       )
      into rtn
      from dual;
    return rtn;
  end organization;
  
  function biological_activity (organization_id                fa_biological_result.organization_id%type,
                                activity_id                    fa_biological_result.activity_id%type,
                                trip_id                        fa_biological_result.trip_id%type,
                                replicate_number               fa_biological_result.replicate_number%type,
                                activity_type                  fa_biological_result.activity_type%type,
                                activity_medium                fa_biological_result.activity_medium%type,
                                activity_start_date_time       fa_biological_result.activity_start_date_time%type,
                                act_start_time_zone            fa_biological_result.act_start_time_zone%type,
                                activity_stop_date_time        fa_biological_result.activity_stop_date_time%type,
                                act_stop_time_zone             fa_biological_result.act_stop_time_zone%type,
                                activity_rel_depth             fa_biological_result.activity_rel_depth%type,
                                activity_depth                 fa_biological_result.activity_depth%type,
                                activity_depth_unit            fa_biological_result.activity_depth_unit%type,
                                activity_upper_depth           fa_biological_result.activity_upper_depth%type,
                                upr_lwr_depth_unit             fa_biological_result.upr_lwr_depth_unit%type,
                                activity_lower_depth           fa_biological_result.activity_lower_depth%type,
                                activity_depth_ref_point       fa_biological_result.activity_depth_ref_point%type,
                                project_id                     fa_biological_result.project_id%type,
                                activity_cond_org_text         fa_biological_result.activity_cond_org_text%type,
                                station_id                     fa_biological_result.station_id%type,
                                activity_comment               fa_biological_result.activity_comment%type,
                                activity_latitude              fa_biological_result.activity_latitude%type,
                                activity_longitude             fa_biological_result.activity_longitude%type,
                                map_scale                      fa_biological_result.map_scale%type,
                                horizontal_accuracy_measure    fa_biological_result.horizontal_accuracy_measure%type, 
                                fk_act_mad_hmethod             fa_biological_result.fk_act_mad_hmethod%type,
                                fk_act_mad_hdatum              fa_biological_result.fk_act_mad_hdatum%type,
                                activity_community             fa_biological_result.activity_community%type,
                                sampling_duration              fa_biological_result.sampling_duration%type,
                                place_in_series                fa_biological_result.place_in_series%type,
                                reach_length                   fa_biological_result.reach_length%type,
                                reach_width                    fa_biological_result.reach_width%type,
                                pass_count                     fa_biological_result.pass_count%type,
                                trap_net_comment               fa_biological_result.trap_net_comment%type,
                                non_tow_current_speed          fa_biological_result.non_tow_current_speed%type,
                                non_tow_net_surface_area       fa_biological_result.non_tow_net_surface_area%type,
                                tow_net_surface_area           fa_biological_result.tow_net_surface_area%type,
                                non_tow_net_mesh_size          fa_biological_result.non_tow_net_mesh_size%type,
                                tow_net_mesh_size              fa_biological_result.tow_net_mesh_size%type,
                                boat_speed                     fa_biological_result.boat_speed%type,
                                toxicity_test_type             fa_biological_result.toxicity_test_type%type,
                                field_procedure_id             fa_biological_result.field_procedure_id%type,
                                field_gear_id                  fa_biological_result.field_gear_id%type,
                                field_prep_procedure_id        fa_biological_result.field_prep_procedure_id%type,
                                container_desc                 fa_biological_result.container_desc%type,
                                presrv_strge_prcdr             fa_biological_result.presrv_strge_prcdr%type,
                                temp_preservn_type             fa_biological_result.temp_preservn_type%type,
                                smprp_transport_storage_desc   fa_biological_result.smprp_transport_storage_desc%type                                
                               )
    return clob deterministic is
    rtn clob;
  begin
    select xmlserialize(content (xmlelement("Activity",
                                            xmlelement("ActivityDescription",
                                                       xmlelement("ActivityIdentifier", organization_id || '-' ||
                                                                                        activity_id ||
                                                                                        nvl2(trip_id, '-' || trip_id, null) ||
                                                                                        nvl2(replicate_number, '-' || replicate_number, null)
                                                                 ),
                                                       xmlelement("ActivityTypeCode", activity_type),
                                                       xmlelement("ActivityMediaName", activity_medium),
                                                       xmlelement("ActivityStartDate", to_char(activity_start_date_time, 'yyyy-mm-dd')),
                                                       xmlelement("ActivityStartTime",
                                                                  xmlelement("Time", to_char(activity_start_date_time, 'hh24:mi:ss')),
                                                                  xmlelement("TimeZoneCode", act_start_time_zone)
                                                                 ),
                                                       xmlelement("ActivityEndDate", to_char(activity_stop_date_time, 'yyyy-mm-dd')),
                                                       xmlelement("ActivityEndTime",
                                                                  xmlelement("Time", to_char(activity_stop_date_time, 'hh24:mi:ss')),
                                                                  xmlelement("TimeZoneCode", act_stop_time_zone)
                                                                 ),
                                                       xmlelement("ActivityRelativeDepthName", activity_rel_depth),
                                                       xmlelement("ActivityDepthHeightMeasure",
                                                                  xmlelement("MeasureValue", activity_depth),
                                                                  xmlelement("MeasureUnitCode", activity_depth_unit)
                                                                 ),
                                                       xmlelement("ActivityTopDepthHeightMeasure",
                                                                  xmlelement("MeasureValue", activity_upper_depth),
                                                                  xmlelement("MeasureUnitCode", nvl2(activity_upper_depth, upr_lwr_depth_unit, null))
                                                                 ),
                                                       xmlelement("ActivityBottomDepthHeightMeasure",
                                                                  xmlelement("MeasureValue", activity_lower_depth),
                                                                  xmlelement("MeasureUnitCode", nvl2(activity_lower_depth, upr_lwr_depth_unit, null))
                                                                 ),
/*!!!!*/                                                       xmlelement("ActivityDepthAltitudeReferencePointText", activity_depth_ref_point),
/*!!!!*/                                                       xmlelement("ActivityDepthAltitudeReferencePointText", nvl2(coalesce(activity_upper_depth, activity_lower_depth), activity_depth_ref_point, null)),
                                                       xmlelement("ProjectIdentifier", project_id),
                                                       xmlelement("ActivityConductingOrganizationText", activity_cond_org_text),
                                                       xmlelement("MonitoringLocationIdentifier", station_id),
                                                       xmlelement("ActivityCommentText", activity_comment) /*,
                                                       xmlelement("SampleAquifer", ???), --not in spreadsheet
                                                       xmlelement("HydrologicCondition", ???), --not in spreadsheet
                                                       xmlelement("HydrologicEvent", ???) --not in spreadsheet */
                                                      ),
                                            xmlelement("ActivityLocation",
                                                       xmlelement("LatitudeMeasure", activity_latitude),
                                                       xmlelement("LongitudeMeasure", activity_longitude),
                                                       xmlelement("SourceMapScaleNumeric", map_scale),
                                                       xmlelement("HorizontalAccuracyMeasure",
                                                                  xmlelement("MeasureValue", regexp_substr(horizontal_accuracy_measure, '[^~]+', 1, 1)),
                                                                  xmlelement("MeasureUnitCode", regexp_substr(horizontal_accuracy_measure, '[^~]+', 1, 2))
                                                                 ),
                                                       xmlelement("HorizontalCollectionMethodName", fk_act_mad_hmethod),
                                                       xmlelement("HorizontalCoordinateReferenceSystemDatumName", fk_act_mad_hdatum)
                                                      ),
                                            xmlelement("BiologicalActivityDescription",
                                                       xmlelement("AssemblageSampledName", activity_community),
                                                       xmlelement("BiologicalHabitatCollectionInformation",
                                                                  xmlelement("CollectionDuration",
                                                                             xmlelement("MeasureValue", regexp_substr(sampling_duration, '[^~ ]+', 1, 1)),
                                                                             xmlelement("MeasureUnitCode", regexp_substr(sampling_duration, '[^~ ]+', 1, 1))
                                                                            ),/*
                                                                  xmlelement("SamplingComponentName", ),*/
                                                                  xmlelement("SamplingComponentPlaceInSeriesNumeric", place_in_series),
                                                                  xmlelement("ReachLengthMeasure",
                                                                             xmlelement("MeasureValue", reach_length)/*,
                                                                             xmlelement("MeasureUnitCode", )*/
                                                                            ),
                                                                  xmlelement("ReachWidthMeasure",
                                                                             xmlelement("MeasureValue", reach_width)/*,
                                                                             xmlelement("MeasureUnitCode", )*/
                                                                            ),
                                                                  xmlelement("PassCount", pass_count),
                                                                  xmlelement("NetInformation",
                                                                             xmlelement("NetTypeName", trap_net_comment),/*non_tow_current_speed??????
                                                                             xmlelement("NetSurfaceAreaMeasure",
                                                                                        xmlelement("MeasureValue", non_tow_net_surface_area/tow_net_surface_area),??
                                                                                        xmlelement("MeasureUnitCode", )
                                                                                       ),
                                                                             xmlelement("NetMeshSizeMeasure",
                                                                                        xmlelement("MeasureValue", non_tow_net_mesh_size/tow_net_mesh_size),??
                                                                                        xmlelement("MeasureUnitCode", )
                                                                                       ),*/
                                                                             xmlelement("BoatSpeedMeasure",
                                                                                        xmlelement("MeasureValue", boat_speed)/*,
                                                                                        xmlelement("MeasureUnitCode", )*/
                                                                                       ),
                                                                             xmlelement("CurrentSpeedMeasure",
                                                                                        xmlelement("MeasureValue", non_tow_current_speed)/*,
                                                                                        xmlelement("MeasureUnitCode", )*/
                                                                                       )
                                                                            )
                                                                 ),
                                                        xmlelement("ToxicityTestType", toxicity_test_type)
                                                      ),
                                            xmlelement("SampleDescription",
                                                       xmlelement("SampleCollectionMethod",
                                                                  xmlelement("MethodIdentifier", field_procedure_id),/*???*/
                                                                  xmlelement("MethodIdentifierContext", field_procedure_id),
                                                                  xmlelement("MethodName", field_procedure_id)/*,
                                                                  xmlelement("MethodQualifierTypeName", ),
                                                                  xmlelement("MethodDescriptionText", )*/
                                                                 ),
                                                       xmlelement("SampleCollectionEquipmentName", field_gear_id),/*
                                                       xmlelement("SampleCollectionEquipmentCommentText", ),*/
                                                       xmlelement("SamplePreparation",
                                                                  xmlelement("SamplePreparationMethod",/*
                                                                             xmlelement("MethodIdentifier",),*/
                                                                             xmlelement("MethodIdentifierContext", field_prep_procedure_id),
                                                                             xmlelement("MethodName", field_prep_procedure_id)/*,
                                                                             xmlelement("MethodQualifierTypeName", ),
                                                                             xmlelement("MethodDescriptionText", )*/
                                                                            ),
                                                                  xmlelement("SampleContainerTypeName", container_desc),
                                                                  xmlelement("SampleContainerColorName", container_desc),
                                                                  xmlelement("ChemicalPreservativeUsedName", presrv_strge_prcdr),
                                                                  xmlelement("ThermalPreservativeUsedName", temp_preservn_type),
                                                                  xmlelement("SampleTransortStorageDescription", smprp_transport_storage_desc)
                                                                 )
                                                      )/*,
                                            xmlelement("ActivityMetric",
                                                       xmlelement("ActivityMetricType",
                                                                  xmlelement("MetricTypeIdentifier", ),
                                                                  xmlelement("MetricTypeIdentifierContext", ),
                                                                  xmlelement("MetricTypeName", ),
                                                                  xmlelement("MetricTypeCitation",
                                                                             xmlelement("ResourceTitleName", ),
                                                                             xmlelement("ResourceCreatorName", ),
                                                                             xmlelement("ResourceSubjectText", ),
                                                                             xmlelement("ResourcePublisherName", ),
                                                                             xmlelement("ResourceDate", ),
                                                                             xmlelement("ResourceIdentifier", )
                                                                            ),
                                                                  xmlelement("MetricTypeScaleText")
                                                                  xmlelement("FormulaDescriptionText")
                                                                 ),
                                                       xmlelement("MetricValueMeasure",
                                                                  xmlelement("MeasureValue", ),
                                                                  xmlelement("MeasureUnitCode", )
                                                                 ),
                                                       xmlelement("MetricScoreNumeric", ),
                                                       xmlelement("MetricCommentText", ),
                                                       xmlelement("IndexIdentifier", )
                                                      ),
                                            xmlelement("AttachedBinaryObject",
                                                       xmlelement("BinaryObjectFileName", ),
                                                       xmlelement("BinaryObjectFileTypeCode", )
                                                      ),
                                            xmlelement("ResultCount", )*/
                                           )
                                ) as clob no indent
                       )
      into rtn
      from dual;
    return rtn;
  end biological_activity;
/*  
  function regular_result (result_value_text            fa_regular_result.result_value_text%type,
                           characteristic_name          fa_regular_result.characteristic_name%type,
                           sample_fraction_type         fa_regular_result.sample_fraction_type%type,
                           result_value                 fa_regular_result.result_value%type,
                           result_unit                  fa_regular_result.result_unit%type,
                           result_meas_qual_code        fa_regular_result.result_meas_qual_code%type,
                           result_value_status          fa_regular_result.result_value_status%type,
                           statistic_type               fa_regular_result.statistic_type%type,
                           result_value_type            fa_regular_result.result_value_type%type,
                           weight_basis_type            fa_regular_result.weight_basis_type%type,
                           duration_basis               fa_regular_result.duration_basis%type,
                           temperature_basis_level      fa_regular_result.temperature_basis_level%type,
                           particle_size                fa_regular_result.particle_size%type,
                           precision                    fa_regular_result.precision%type,
                           result_comment               fa_regular_result.result_comment%type,
                           result_depth_meas_value      fa_regular_result.result_depth_meas_value%type,
                           result_depth_meas_unit_code  fa_regular_result.result_depth_meas_unit_code%type,
                           result_depth_alt_ref_pt_txt  fa_regular_result.result_depth_alt_ref_pt_txt%type,
                           itis_number                  fa_regular_result.itis_number%type,
                           analytical_procedure_id      fa_regular_result.analytical_procedure_id%type,
                           analytical_procedure_source  fa_regular_result.analytical_procedure_source%type,
                           lab_samp_prp_method_id       fa_regular_result.lab_samp_prp_method_id%type,
                           lab_name                     fa_regular_result.lab_name%type,
                           analysis_date_time           fa_regular_result.analysis_date_time%type,
                           lab_remark                   fa_regular_result.lab_remark%type,
                           myqldesc                     fa_regular_result.myqldesc%type,
                           myql                         fa_regular_result.myql%type,
                           myqlunits                    fa_regular_result.myqlunits%type,
                           lab_samp_prp_start_date_time fa_regular_result.lab_samp_prp_start_date_time%type
                          )
    return clob deterministic is
    rtn clob;
  begin
    select xmlserialize(content (xmlelement("Result",
                                            xmlelement("ResultDescription",
                                                       xmlelement("ResultDetectionConditionText", result_value_text),
                                                       xmlelement("CharacteristicName", characteristic_name),
                                                       xmlelement("ResultSampleFractionText", sample_fraction_type),
                                                       xmlelement("ResultMeasure",
                                                                  xmlelement("ResultMeasureValue", result_value),
                                                                  xmlelement("MeasureUnitCode", result_unit),
                                                                  xmlelement("MeasureQualifierCode", result_meas_qual_code)
                                                                 ),
                                                       xmlelement("ResultStatusIdentifier", result_value_status),
                                                       xmlelement("StatisticalBaseCode", statistic_type),
                                                       xmlelement("ResultValueTypeName", result_value_type),
                                                       xmlelement("ResultWeightBasisText", weight_basis_type),
                                                       xmlelement("ResultTimeBasisText", duration_basis),
                                                       xmlelement("ResultTemperatureBasisText", temperature_basis_level),
                                                       xmlelement("ResultParticleSizeBasisText", particle_size),
                                                       xmlelement("DataQuality",
                                                                  xmlelement("PrecisionValue", precision)
                                                                 ),
                                                       xmlelement("ResultCommentText", result_comment),
                                                       xmlelement("ResultDepthHeightMeasure", 
                                                                  xmlelement("MeasureValue", result_depth_meas_value),
                                                                  xmlelement("MeasureUnitCode", result_depth_meas_unit_code)
                                                                 ),
                                                       xmlelement("ResultDepthAltitudeReferencePointText", result_depth_alt_ref_pt_txt)
                                                      ),
                                            xmlelement("BiologicalResultDescription",
                                                       xmlelement("SubjectTaxonomicName", itis_number)
                                                      ),
                                            xmlelement("ResultAnalyticalMethod",
                                                       xmlelement("MethodIdentifier", analytical_procedure_id),
                                                       xmlelement("MethodIdentifierContext", analytical_procedure_source),
                                                       xmlelement("MethodName", lab_samp_prp_method_id)
                                                      ),
                                            xmlelement("ResultLabInformation",
                                                       xmlelement("LaboratoryName", lab_name),
                                                       xmlelement("AnalysisStartDate", to_char(analysis_date_time, 'yyyy-mm-dd')),
                                                       xmlelement("ResultLaboratoryCommentText", lab_remark),
                                                       xmlelement("ResultDetectionQuantitationLimit",
                                                                  xmlelement("DetectionQuantitationLimitTypeName", myqldesc),
                                                                  xmlelement("DetectionQuantitationLimitMeasure",
                                                                             xmlelement("MeasureValue", myql),
                                                                             xmlelement("MeasureUnitCode", myqlunits)
                                                                            )
                                                                 )
                                                      ),
                                            xmlelement("LabSamplePreparation",
                                                       xmlelement("PreparationStartDate", to_char(lab_samp_prp_start_date_time, 'yyyy-mm-dd'))
                                                      )
                                           )
                                ) as clob no indent
                       )
      into rtn
      from dual;
    return rtn;
  end regular_result;
*/  
  function biological_result (result_value_text                 fa_biological_result.result_value_text%type,
                              characteristic_name               fa_biological_result.characteristic_name%type,
                              sample_fraction_type              fa_biological_result.sample_fraction_type%type,
                              result_value                      fa_biological_result.result_value%type,
                              result_unit                       fa_biological_result.result_unit%type,
                              result_meas_qual_code             fa_biological_result.result_meas_qual_code%type,
                              result_value_status               fa_biological_result.result_value_status%type,
                              statistic_type                    fa_biological_result.statistic_type%type,
                              result_value_type                 fa_biological_result.result_value_type%type,
                              weight_basis_type                 fa_biological_result.weight_basis_type%type,
                              duration_basis                    fa_biological_result.duration_basis%type,
                              temperature_basis_level           fa_biological_result.temperature_basis_level%type,
                              particle_size                     fa_biological_result.particle_size%type,
                              precision                         fa_biological_result.precision%type,
                              bias                              fa_biological_result.bias%type,
                              confidence_level                  fa_biological_result.confidence_level%type,
                              result_comment                    fa_biological_result.result_comment%type,
                              result_depth_meas_value           fa_biological_result.result_depth_meas_value%type,
                              result_depth_meas_unit_code       fa_biological_result.result_depth_meas_unit_code%type,
                              result_depth_alt_ref_pt_txt       fa_biological_result.result_depth_alt_ref_pt_txt%type,
                              sampling_point_name               fa_biological_result.sampling_point_name%type,
                              activity_intent                   fa_biological_result.activity_intent%type,
                              individual_number                 fa_biological_result.individual_number%type,
                              activity_subject_taxon            fa_biological_result.activity_subject_taxon%type,
                              species_id                        fa_biological_result.species_id%type,
                              biopart_name                      fa_biological_result.biopart_name%type,
                              result_group_summary_ct_wt        fa_biological_result.result_group_summary_ct_wt%type,
                              cell_form                         fa_biological_result.cell_form%type,
                              cell_shape                        fa_biological_result.cell_shape%type,
                              habit                             fa_biological_result.habit%type,
                              voltinism                         fa_biological_result.voltinism%type,
                              pollution_tolerance               fa_biological_result.pollution_tolerance%type,
                              pollution_tolerance_scale         fa_biological_result.pollution_tolerance_scale%type,
                              trophic_level                     fa_biological_result.trophic_level%type,
                              feeding_group                     fa_biological_result.feeding_group%type,
                              analytical_procedure_id           fa_biological_result.analytical_procedure_id%type,
                              analytical_procedure_source       fa_biological_result.analytical_procedure_source%type,
                              analytical_method_list_agency     fa_biological_result.analytical_method_list_agency%type,
                              lab_name                          fa_biological_result.lab_name%type,
                              analysis_date_time                fa_biological_result.analysis_date_time%type,
                              analysis_time_zone                fa_biological_result.analysis_time_zone%type,
                              analysis_end_date_time            fa_biological_result.analysis_end_date_time%type,
                              analysis_end_time_zone            fa_biological_result.analysis_end_time_zone%type,
                              lab_remark                        fa_biological_result.lab_remark%type,
                              all_result_detection_limit        fa_biological_result.all_result_detection_limit%type,
                              detection_limit                   fa_biological_result.detection_limit%type,
                              detection_limit_description       fa_biological_result.detection_limit_description%type,
                              detection_limit_unit              fa_biological_result.detection_limit_unit%type,
                              lower_quantitation_limit          fa_biological_result.lower_quantitation_limit%type,
                              upper_quantitation_limit          fa_biological_result.upper_quantitation_limit%type,
                              lab_certified                     fa_biological_result.lab_certified%type,
                              lab_accred_authority              fa_biological_result.lab_accred_authority%type,
                              taxonomist_accred_yn              fa_biological_result.taxonomist_accred_yn%type,
                              taxonomist_accred_authority       fa_biological_result.taxonomist_accred_authority%type,
                              frequency_class                   fa_biological_result.frequency_class%type/*,
                              -- join on fa_biological_result.taxon_detail_citation_id = md_citation.citation_id
                              -- there's no md_citation table and all taxon_detail_citation_id are null
                              md_citation_title                 md_citation.title%type,
                              md_citation_author                md_citation.author%type,
                              md_citation_vol_and_page          md_citation.vol_and_page%type,
                              md_citation_pblshr_org_name       md_citation.pblshr_org_name%type,
                              md_citation_publishing_year       md_citation.publishing_year%type,
                              -- join on fa_biological_result.analytical_procedure_id = md_analytical_proc.procedure_id
                              -- there's no md_analytical_proc table
                              md_analytical_proc_qualifier_type	md_analytical_proc.qualifier_type%type,
                              md_analytical_proc_procedure_desc	md_analytical_proc.procedure_desc%type */
                             )
    return clob deterministic is
    rtn clob;
  begin
    select xmlserialize(content (xmlelement("Result",
                                            xmlelement("ResultDescription", /*
                                                       xmlelement("DataLoggerLineName",), --N/A for bio */
                                                       xmlelement("ResultDetectionConditionText", result_value_text),
                                                       xmlelement("CharacteristicName", characteristic_name), /*
                                                       xmlelement("MethodSpecificationName",), --not mapped */
                                                       xmlelement("ResultSampleFractionText", sample_fraction_type),
                                                       xmlelement("ResultMeasure",
                                                                  xmlelement("ResultMeasureValue", result_value),
                                                                  xmlelement("MeasureUnitCode", result_unit),
                                                                  xmlelement("MeasureQualifierCode", result_meas_qual_code)
                                                                 ),
                                                       xmlelement("ResultStatusIdentifier", result_value_status),
                                                       xmlelement("StatisticalBaseCode", statistic_type),
                                                       xmlelement("ResultValueTypeName", result_value_type),
                                                       xmlelement("ResultWeightBasisText", weight_basis_type),
                                                       xmlelement("ResultTimeBasisText", duration_basis),
                                                       xmlelement("ResultTemperatureBasisText", temperature_basis_level),
                                                       xmlelement("ResultParticleSizeBasisText", particle_size),
                                                       xmlelement("DataQuality",
                                                                  xmlelement("PrecisionValue", precision),
                                                                  xmlelement("BiasValue", bias)/*
                                                                  xmlelement("ConfidenceIntervalValue", ), --"not mapped"
                                                                  xmlelement("UpperConfidenceLimitValue", confidence_level), --"Use 2nd part of ; separated values enclosed in ()" but no values found in data
                                                                  xmlelement("LowerConfidenceLimitValue", confidence_level) --"Use 1st part of ; separated values enclosed in ()" but no values found in data */
                                                                 ),
                                                       xmlelement("ResultCommentText", result_comment),
                                                       xmlelement("ResultDepthHeightMeasure", 
                                                                  xmlelement("MeasureValue", result_depth_meas_value),
                                                                  xmlelement("MeasureUnitCode", result_depth_meas_unit_code)
                                                                 ),
                                                       xmlelement("ResultDepthAltitudeReferencePointText", result_depth_alt_ref_pt_txt), /*
                                                       xmlelement("USGSPCode", non_existent_column), --not in spreadsheet */
                                                       xmlelement("ResultSamplingPointName", sampling_point_name)
                                                      ),
                                            xmlelement("BiologicalResultDescription",
                                                       xmlelement("BiologicalIntentName", activity_intent),
                                                       xmlelement("BiologicalIndividualIdentifier", individual_number),
                                                       xmlelement("SubjectTaxonomicName", activity_subject_taxon),
                                                       xmlelement("UnidentifiedSpeciesIdentifier", species_id),
                                                       xmlelement("SampleTissueAnatomyName", biopart_name),
                                                       xmlelement("GroupSummaryCountWeight",
                                                                  xmlelement("MeasureValue", regexp_substr(result_group_summary_ct_wt, '[^~]+', 1, 1),
                                                                  xmlelement("MeasureUnitCode", regexp_substr(result_group_summary_ct_wt, '[^~]+', 1, 2))
                                                                 ),
                                                       xmlelement("TaxonomicDetails", /* characteritic_description */
                                                                  xmlelement("CellFormName", cell_form),
                                                                  xmlelement("CellShapeName", cell_shape),
                                                                  xmlelement("HabitName", habit),
                                                                  xmlelement("VoltismName", voltinism),
                                                                  xmlelement("TaxonomicPollutionTolerance", pollution_tolerance),
                                                                  xmlelement("TaxonomicPollutionToleranceScaleText", pollution_tolerance_scale),
                                                                  xmlelement("TrophicLevelName", trophic_level),
                                                                  xmlelement("FunctionalFeedingGroupName", feeding_group)/*,
                                                                  xmlelement("TaxonomicDetailsCitation",   
                                                                             xmlelement("ResourceTitleName", md_citation_title),
                                                                             xmlelement("ResourceCreatorName", md_citation_author),
                                                                             xmlelement("ResourceSubjectText", md_citation_vol_and_page),
                                                                             xmlelement("ResourcePublishername", md_citation_pblshr_org_name),
                                                                             xmlelement("ResourceDate", to_char(md_citation_publishing_year, 'yyyy-mm-dd')),
                                                                             xmlelement("ResourceIdentfier",)
                                                                            )*/
                                                                 ),
                                                       xmlelement("FrequenceyClassInformation",
                                                                  xmlelement("FrequencyClassDescriptorCode", regexp_substr(frequency_class, '[^~]+', 1, 2)),
                                                                  xmlelement("FrequencyClassDescriptorUnitCode", regexp_substr(frequency_class, '[^~]+', 1, 5)),
                                                                  xmlelement("LowerClassBoundValue", regexp_substr(frequency_class, '[^~]+', 1, 3)),
                                                                  xmlelement("UpperClassBoundValue", regexp_substr(frequency_class, '[^~]+', 1, 4))
                                                                 )
                                                      ), /*
                                            xmlelement("AttachedBinaryObject", --no data fa_blob.blob_content),*/
                                            xmlelement("ResultAnalyticalMethod",
                                                       xmlelement("MethodIdentifier", analytical_procedure_id),/*???*/
                                                       xmlelement("MethodIdentifierContext", analytical_procedure_source),/*???*/
                                                       xmlelement("MethodName", analytical_method_list_agency)  /*,???
                                                       xmlelement("MethodQualifierTypeName", md_analytical_proc_qualifier_type),
                                                       xmlelement("MethodDescriptionText", md_analytical_proc_procedure_desc), */
                                                      ),
                                            xmlelement("ResultLabInformation",
                                                       xmlelement("LaboratoryName", lab_name),
                                                       xmlelement("AnalysisStartDate", to_char(analysis_date_time, 'yyyy-mm-dd')),
                                                       xmlelement("AnalysisStartTime",
                                                                  xmlelement("Time", to_char(analysis_date_time, 'hh24:mi:ss')),
                                                                  xmlelement("TimeZoneCode", analysis_time_zone)
                                                                 ),
                                                       xmlelement("AnalysisEndDate", to_char(analysis_end_date_time, 'yyyy-mm-dd')),
                                                       xmlelement("AnalysisEndTime",
                                                                  xmlelement("Time", to_char(analysis_end_date_time, 'hh24:mi:ss')),
                                                                  xmlelement("TimeZoneCode", analysis_end_time_zone)
                                                                 ),
                                                       xmlelement("ResultLaboratoryCommentCode", lab_remark), /*
                                                       xmlelement("ResultLaboratoryCommentText", ),
                                                       xmlelement("ResultDetectionQuantitationLimit", --data too messy and confusing
                                                                  xmlelement("DetectionQuantitationLimitTypeName", myqldesc),
                                                                  xmlelement("DetectionQuantitationLimitMeasure",lower_quantitation_limit;upper_quantitation_limit;ALL_RESULT_DETECTION_LIMIT
                                                                             xmlelement("MeasureValue", myql),
                                                                             xmlelement("MeasureUnitCode", myqlunits)
                                                                            )
                                                                 ), */
                                                       xmlelement("LaboratoryAccreditationIndicator", lab_certified),
                                                       xmlelement("LaboratoryAccreditationAuthorityName", lab_accred_authority),
                                                       xmlelement("TaxonomistAccreditationIndicator", taxonomist_accred_yn),
                                                       xmlelement("TaxonomistAccreditationAuthorityName", taxonomist_accred_authority)
                                                      ) /*,
                                            xmlelement("LabSamplePreparation", --not yet implemented
                                                       xmlelement("LabSamplePreparationMethod",
                                                                  xmlelement("MethodIdentifier", lab_samp_prp_method_id),
                                                                  xmlelement("MethodIdentifierContext", lab_samp_prp_method_context),
                                                                  xmlelement("MethodName", ),
                                                                  xmlelement("MethodQualifierTypeName", ),
                                                                  xmlelement("MethodDescriptionText", )
                                                                 ),
                                                       xmlelement("PreparationStartDate", to_char(lab_samp_prp_start_date_time, 'yyyy-mm-dd')),
                                                       xmlelement("PreparationStartTime",
                                                                  xmlelement("Time", to_char(lab_samp_prp_start_date_time, 'hh24:mi:ss')),
                                                                  xmlelement("TimeZoneCode", lab_samp_prp_start_tmzone)
                                                                 ),
                                                       xmlelement("PreparationEndDate", to_char(lab_samp_prp_end_date_time, 'yyyy-mm-dd')),
                                                       xmlelement("PreparationEndTime",
                                                                  xmlelement("Time", to_char(lab_samp_prp_end_date_time, 'hh24:mi:ss')),
                                                                  xmlelement("TimeZoneCode", lab_samp_prp_end_tmzone)
                                                                 ),
                                                       xmlelement("SubstitutionDilutionFactorNumeric", lab_samp_prp_dilution_factor)
                                                      )*/
                                           )
                                ) as clob no indent
                       )
      into rtn
      from dual;
    return rtn;
  end biological_result;

  function strip_bad( p_string in varchar2 )
    return varchar2 deterministic is
  begin
    return regexp_replace(p_string, '[^a-z,_,A-Z,0-9,.,'']', '');
  end strip_bad;

end xml_helpers;
/