create or replace package xml_helpers as
  function organization (organization_id   di_org.organization_id%type,
                         organization_name di_org.organization_name%type)
    return clob deterministic;

  function activity (organization_id                fa_station.organization_id%type,
                     activity_id                    fa_regular_result.activity_id%type,
                     trip_id                        fa_regular_result.trip_id%type,
                     replicate_number               fa_regular_result.replicate_number%type,
                     activity_type                  fa_regular_result.activity_type%type,
                     activity_medium                fa_regular_result.activity_medium%type,
                     matrix_name                    fa_regular_result.matrix_name%type,
                     activity_start_date_time       fa_regular_result.activity_start_date_time%type,
                     act_start_time_zone            fa_regular_result.act_start_time_zone%type,
                     activity_stop_date_time        fa_regular_result.activity_stop_date_time%type,
                     act_stop_time_zone             fa_regular_result.act_stop_time_zone%type,
                     activity_depth                 fa_regular_result.activity_depth%type,
                     activity_depth_unit            fa_regular_result.activity_depth_unit%type,
                     activity_depth_ref_point       fa_regular_result.activity_depth_ref_point%type,
                     activity_upper_depth           fa_regular_result.activity_upper_depth%type,
                     upr_lwr_depth_unit             fa_regular_result.upr_lwr_depth_unit%type,
                     activity_lower_depth           fa_regular_result.activity_lower_depth%type,
                     activity_uprlwr_depth_ref_pt   fa_regular_result.activity_uprlwr_depth_ref_pt%type,
                     project_id                     fa_regular_result.project_id%type,
                     activity_cond_org_text         fa_regular_result.activity_cond_org_text%type,
                     station_id                     fa_regular_result.station_id%type,
                     activity_comment               fa_regular_result.activity_comment%type,
                     field_procedure_id             fa_regular_result.field_procedure_id%type,
                     field_gear_id                  fa_regular_result.field_gear_id%type
                    )
    return clob deterministic;
    
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
    return clob deterministic;
end xml_helpers;
/

create or replace package body xml_helpers as
  function organization (organization_id   di_org.organization_id%type,
                         organization_name di_org.organization_name%type)
    return clob is
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
  
  function activity (organization_id                fa_station.organization_id%type,
                     activity_id                    fa_regular_result.activity_id%type,
                     trip_id                        fa_regular_result.trip_id%type,
                     replicate_number               fa_regular_result.replicate_number%type,
                     activity_type                  fa_regular_result.activity_type%type,
                     activity_medium                fa_regular_result.activity_medium%type,
                     matrix_name                    fa_regular_result.matrix_name%type,
                     activity_start_date_time       fa_regular_result.activity_start_date_time%type,
                     act_start_time_zone            fa_regular_result.act_start_time_zone%type,
                     activity_stop_date_time        fa_regular_result.activity_stop_date_time%type,
                     act_stop_time_zone             fa_regular_result.act_stop_time_zone%type,
                     activity_depth                 fa_regular_result.activity_depth%type,
                     activity_depth_unit            fa_regular_result.activity_depth_unit%type,
                     activity_depth_ref_point       fa_regular_result.activity_depth_ref_point%type,
                     activity_upper_depth           fa_regular_result.activity_upper_depth%type,
                     upr_lwr_depth_unit             fa_regular_result.upr_lwr_depth_unit%type,
                     activity_lower_depth           fa_regular_result.activity_lower_depth%type,
                     activity_uprlwr_depth_ref_pt   fa_regular_result.activity_uprlwr_depth_ref_pt%type,
                     project_id                     fa_regular_result.project_id%type,
                     activity_cond_org_text         fa_regular_result.activity_cond_org_text%type,
                     station_id                     fa_regular_result.station_id%type,
                     activity_comment               fa_regular_result.activity_comment%type,
                     field_procedure_id             fa_regular_result.field_procedure_id%type,
                     field_gear_id                  fa_regular_result.field_gear_id%type
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
                                                       xmlelement("ActivityMediaSubdivisionName", matrix_name),
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
                                                       xmlelement("ActivityDepthHeightMeasure",
                                                                  xmlelement("MeasureValue", activity_depth),
                                                                  xmlelement("MeasureUnitCode", activity_depth_unit)
                                                                 ),
                                                       xmlelement("ActivityDepthAltitudeReferencePointText", activity_depth_ref_point),
                                                       xmlelement("ActivityTopDepthHeightMeasure",
                                                                  xmlelement("MeasureValue", activity_upper_depth),
                                                                  xmlelement("MeasureUnitCode", nvl2(activity_upper_depth, upr_lwr_depth_unit, null))
                                                                 ),
                                                       xmlelement("ActivityBottomDepthHeightMeasure",
                                                                  xmlelement("MeasureValue", activity_lower_depth),
                                                                  xmlelement("MeasureUnitCode", nvl2(activity_lower_depth, upr_lwr_depth_unit, null))
                                                                 ),
                                                       xmlelement("ActivityDepthAltitudeReferencePointText", nvl2(coalesce(activity_upper_depth, activity_lower_depth), activity_depth_ref_point, null)),
                                                       xmlelement("ProjectIdentifier", project_id),
                                                       xmlelement("ActivityConductingOrganizationText", activity_cond_org_text),
                                                       xmlelement("MonitoringLocationIdentifier", station_id),
                                                       xmlelement("ActivityCommentText", activity_comment) /*,
                                                       xmlelement("SampleAquifer", ???),
                                                       xmlelement("HydrologicCondition", ???),
                                                       xmlelement("HydrologicEvent", ???) */
                                                      ),
                                            xmlelement("SampleDescription",
                                                       xmlelement("SampleCollectionMethod",
                                                                  xmlelement("MethodIdentifier", field_procedure_id),
                                                                  xmlelement("MethodIdentifierContext", field_procedure_id),
                                                                  xmlelement("MethodName", field_procedure_id)
                                                                 ),
                                                       xmlelement("SampleCollectionEquipmentName", field_gear_id)
                                                      )
                                           )
                                ) as clob no indent
                       )
      into rtn
      from dual;
    return rtn;
  end activity;
  
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
                                            xmlelement("ResultDescription", /*
                                                       xmlelement("DataLoggerLineName",), */
                                                       xmlelement("ResultDetectionConditionText", result_value_text),
                                                       xmlelement("CharacteristicName", characteristic_name), /* ???vs. cas_number???
                                                       xmlelement("MethodSpecificationName",),*/
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
                                                       xmlelement("ResultDepthAltitudeReferencePointText", result_depth_alt_ref_pt_txt) /*,
                                                       xmlelement("USGSPCode", non_existent_column), */
                                                      ),
                                            xmlelement("BiologicalResultDescription", /*
                                                       xmlelement("BiologicalIntentName",),
                                                       xmlelement("BiologicalIndividualIdentifier", ???activity_intent),*/
                                                       xmlelement("SubjectTaxonomicName", itis_number)  /*, ???vs activity_subject_taxon from biologocal result???
                                                       xmlelement("UnidentifiedSpeciesIdentifier",),
                                                       xmlelement("SampleTissueAnatomyName", biopart_name),
                                                       xmlelement("GroupSummaryCountWeight",
                                                                  xmlelement("MeasureValue",),
                                                                  xmlelement("MeasureUnitCode",)
                                                                 ),
                                                       xmlelement("TaxonomicDetails",
                                                                  xmlelement("CellFormName", cell_form),
                                                                  xmlelement("CellShapeName", cell_shape),
                                                                  xmlelement("HabitName", habit),
                                                                  xmlelement("VoltismName", voltinism),
                                                                  xmlelement("TaxonomicPollutionTolerance", pollution_tolerance),
                                                                  xmlelement("TaxonomicPollutionToleranceScaleText", pollution_tolerance_scale),
                                                                  xmlelement("TrophicLevelName", trophic_level),
                                                                  xmlelement("FunctionalFeedingGroupName",),
                                                                  xmlelement("TaxonomicDetailsCitation",
                                                                             xmlelement("ResourceTitleName",),
                                                                             xmlelement("ResourceCreatorName",),
                                                                             xmlelement("ResourceSubjectTitle",),
                                                                             xmlelement("ResourcePublishername",),
                                                                             xmlelement("ResourceDate",),
                                                                             xmlelement("ResourceIdentfier",)
                                                                            )
                                                                 ),
                                                       xmlelement("FrequenceyClassInformation",
                                                                  xmlelement("FrequencyClassDescriptorCode",),
                                                                  xmlelement("FrequencyClassDescriptorUnitCode",),
                                                                  xmlelement("LowerClassBoundValue",),
                                                                  xmlelement("UpperClassBoundValue",)
                                                                 ) */
                                                      ), /*
                                            xmlelement("AttachedBinaryObject",),*/
                                            xmlelement("ResultAnalyticalMethod",
                                                       xmlelement("MethodIdentifier", analytical_procedure_id),
                                                       xmlelement("MethodIdentifierContext", analytical_procedure_source),
                                                       xmlelement("MethodName", lab_samp_prp_method_id)  /*,
                                                       xmlelement("MethodQualifierTypeName",),
                                                       xmlelement("MethodDescriptionText",), */
                                                      ),
                                            xmlelement("ResultLabInformation",
                                                       xmlelement("LaboratoryName", lab_name),
                                                       xmlelement("AnalysisStartDate", to_char(analysis_date_time, 'yyyy-mm-dd')), /*
                                                       xmlelement("AnalysisStartTime",),
                                                       xmlelement("AnalysisEndDate",),
                                                       xmlelement("AnalysisEndTime",),
                                                       xmlelement("ResultLaboratoryCommentCode",), */
                                                       xmlelement("ResultLaboratoryCommentText", lab_remark),
                                                       xmlelement("ResultDetectionQuantitationLimit",
                                                                  xmlelement("DetectionQuantitationLimitTypeName", myqldesc),
                                                                  xmlelement("DetectionQuantitationLimitMeasure",
                                                                             xmlelement("MeasureValue", myql),
                                                                             xmlelement("MeasureUnitCode", myqlunits)
                                                                            )
                                                                 ) /*,
                                                       xmlelement("LaboratoryAccreditationIndicator",),
                                                       xmlelement("LaboratoryAccreditationAuthorityName", ),
                                                       xmlelement("TaxonomistAccreditationIndicator", taxonomist_accred_yn),
                                                       xmlelement("TaxonomistAccreditationAuthorityName", taxonomist_accred_authority)*/
                                                      ),
                                            xmlelement("LabSamplePreparation", /*
                                                       xmlelement("LabSamplePreparationMethod",),*/
                                                       xmlelement("PreparationStartDate", to_char(lab_samp_prp_start_date_time, 'yyyy-mm-dd')) /*,
                                                       xmlelement("PreparationStartTime",),
                                                       xmlelement("PreparationEndDate",),
                                                       xmlelement("PreparationEndTime",),
                                                       xmlelement("SubstitutionDilutionFactorNumeric", substance_dilution_factor)*/
                                                      )
                                           )
                                ) as clob no indent
                       )
      into rtn
      from dual;
    return rtn;
  end regular_result;

end xml_helpers;
/