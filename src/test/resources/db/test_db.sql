create table if not exists storetw.di_geo_county
(pk_isn            integer
,fk_geo_state      integer
,fips_county_code  character varying(3)
,county_name       character varying(256)
,geom              geometry(point,4269)
);

create table if not exists storetw.di_geo_state
(pk_isn             integer
,fips_state_code    character varying(4)
,state_postal_code  character varying(2)
,state_name         character varying(256)
,country_code       character varying(2)
,country_name       character varying(256)
,geom               geometry(point,4269)
,epa_region_code    character varying(2)
,epa_region_name    character varying(256)
);

create table if not exists storetw.di_org
(pk_isn                    integer
,organization_id           character varying(256)
,organization_name         character varying(256)
,organization_is_number    integer
,organization_type         character varying(256)
,organization_description  character varying(500)
,parent_org                character varying(256)
,last_change_date          timestamp
,source_system             character varying(256)
,tribal_code               character varying(256)
,fk_eparegion              integer
);

create table if not exists storetw.fa_station
(station_id                    character varying(256)
,station_name                  character varying(256)
,organization_id               character varying(256)
,location_point_type           character(16)
,point_sequence_number         integer
,well_number                   character varying(35)
,pipe_number                   character(15)
,latitude                      numeric
,longitude                     numeric
,map_scale                     character varying(20)
,elevation                     character(15)
,hydrologic_unit_code          character(8)
,generated_huc                 character(8)
,rf1_segment_code              character varying(3)
,rf1_segment_name              character varying(30)
,rf1_mileage                   numeric
,on_reach_ind                  character(1)
,nrcs_watershed_id             character(8)
,other_estuary_name            character varying(30)
,great_lake_name               character(15)
,ocean_name                    character(14)
,natv_american_land_name       character varying(256)
,frs_key_identifier            character varying(36)
,station_visited               character(3)
,station_is_number             integer
,organization_is_number        integer
,station_group_type            character varying(256)
,sgo_indicator                 character(1)
,well_name                     character varying(255)
,naics_code                    character varying(30)
,spring_type_improvement       character(14)
,spring_permanence             character(12)
,spring_usgs_geologic_unit     character varying(100)
,spring_other_name             character varying(120)
,spring_usgs_lithologic_unit   character varying(135)
,point_name                    character varying(30)
,blob_title                    character varying(256)
,tsmalp_is_number              numeric
,description_text              character varying(4000)
,last_userid                   character varying(100)
,last_change_date              timestamp
,project_id                    character(8)
,tribal_water_quality_measure  character(1)
,horizontal_accuracy           character varying(256)
,fk_db_cat                     integer
,fk_gen_db_cat                 integer
,fk_geo_state                  integer
,fk_geo_county                 integer
,fk_mad_hmethod                integer
,fk_mad_hdatum                 integer
,fk_mad_vmethod                integer
,fk_mad_vdatum                 integer
,std_latitude                  numeric
,std_longitude                 numeric
,fk_std_hdatum                 integer
,fk_org                        integer
,fk_statn_types                integer
,fk_estry_primary              integer
,fk_estry_secondary            integer
,blob_id                       character varying(25)
,fk_primary_type               character varying(256)
,fk_secondary_type             character varying(256)
,fk_horizontal_datum           character varying(254)
,fk_geopositioning_method      character varying(254)
,fk_elevation_datum            character varying(254)
,fk_elevation_method           character varying(254)
,fk_primary_estuary            character varying(30)
,fk_secondary_estuary          character varying(30)
,fk_country_code               character(2)
,fk_state_postal_code          character(2)
,source_system                 character varying(256)
,source_uid                    integer
,geom                          geometry(point,4269)
,well_type_name                character varying(256)
,well_formation_type           character varying(256)
,well_hole_depth               character varying(256)
,well_aquifer_name             character varying(256)
,pk_isn                        integer
,objectid                      integer
,fk_gen_geo_state              integer
,fk_gen_geo_county             integer
,elevation_unit                character(15)
,gen_huctwelvedigitcode        character varying(12)
,wgs84_latitude                numeric
,wgs84_longitude               numeric
,fk_wgs84_hdatum               integer
,last_transaction_id           character varying(100)
,fk_date_lc                    integer
,program_indicator             character varying(256)
,elevation_bk                  character(15)
,fk_geo_county_bk              integer
,fk_geo_state_bk               integer
,well_hole_depth_unit          character varying(256)
,tribal_land_indicator         character varying(1)
,huctwelvedigitcode            character varying(12)
);

create table if not exists storetw.lu_mad_hdatum
(pk_isn            integer
,id_code           character varying(12)
,horizontal_datum  character varying(254)
);

create table if not exists storetw.lu_mad_hmethod
(pk_isn                 integer
,id_code                character varying(12)
,geopositioning_method  character varying(254)
);

create table if not exists storetw.lu_mad_vdatum
(pk_isn           integer
,id_code          character varying(12)
,elevation_datum  character varying(254)
);

create table if not exists storetw.lu_mad_vmethod
(pk_isn            integer
,id_code           character varying(12)
,elevation_method  character varying(254)
);

create table if not exists storetw.storetw_transition
(org_id     character varying (255)
,date_added date
);


create table if not exists storetw.di_activity_matrix
(pk_isn              integer
,matrix_code         character varying(4)
,matrix_name         character varying(256)
,matrix_description  character varying(120)
);

create table if not exists storetw.di_activity_medium
(pk_isn           integer
,activity_medium  character varying(20)
);

create table if not exists storetw.di_characteristic
(pk_isn                     integer
,search_name                character varying(256)
,display_name               character varying(256)
,characteristic_group_type  character varying(256)
,type_code                  character(1)
,registry_name              character varying(256)
,srs_id                     integer
,cas_number                 character varying(256)
,itis_number                character varying(256)
,chartype                   character varying(256)
,last_change_date           timestamp
,description                character varying(1000)
,wqx_chr_uid                integer
,fk_chargrptype             integer
,fk_chartype                integer
,itis_parent                character varying(256)
,kingdom_name               character varying(256)
,rank_name                  character varying(256)
,rank_number                integer
);

create table if not exists storetw.fa_regular_result
(pk_isn                         numeric
,organization_id                character varying(256)
,organization_is_number         integer
,station_id                     character varying(256)
,station_name                   character varying(256)
,activity_start_date_time       timestamp
,act_start_time_zone            character varying(256)
,trip_id                        character varying(256)
,trip_name                      character varying(256)
,station_visit_id               character(3)
,characteristic_group_type      character varying(256)
,characteristic_name            character varying(256)
,result_value                   numeric
,result_unit                    character varying(256)
,result_value_text              character varying(256)
,sample_fraction_type           character varying(256)
,result_value_type              character varying(256)
,statistic_type                 character varying(256)
,result_value_status            character varying(12)
,weight_basis_type              character varying(256)
,temperature_basis_level        character varying(256)
,duration_basis                 character varying(256)
,analytical_procedure_source    character varying(256)
,analytical_procedure_id        character varying(256)
,lab_id                         character(8)
,lab_name                       character varying(60)
,lab_certified                  character varying(1)
,lab_batch_id                   character(12)
,analysis_date_time             timestamp
,analysis_time_zone             character varying(256)
,lower_quantitation_limit       character(30)
,upper_quantitation_limit       character(30)
,detection_limit                character varying(25)
,detection_limit_unit           character varying(12)
,detection_limit_description    character varying(256)
,lab_remark                     character varying(256)
,distance_measure_from          character varying(20)
,distance_measure_to            character varying(20)
,particle_size                  character varying(40)
,replicate_analysis_count       numeric
,precision                      character varying(256)
,confidence_level               character varying(256)
,dilution_indicator             character(1)
,recovery_indicator             character(1)
,correction_indicator           character(1)
,stn_latitude                   numeric
,stn_longitude                  numeric
,stn_hdatum                     character(12)
,stn_std_latitude               numeric
,stn_std_longitude              numeric
,stn_std_hdatum                 character(12)
,hydrologic_unit_code           character(8)
,generated_huc                  character(8)
,result_is_number               numeric
,activity_medium                character varying(20)
,fk_station                     integer
,fk_org                         integer
,fk_db_cat                      integer
,fk_gen_db_cat                  integer
,fk_geo_county                  integer
,fk_geo_state                   integer
,fk_date_act_start              integer
,fk_act_medium                  integer
,fk_act_matrix                  integer
,activity_is_number             integer
,fk_char                        integer
,fk_unit_conversion             integer
,activity_id                    character varying(256)
,replicate_number               numeric
,activity_type                  character varying(256)
,activity_category              character varying(256)
,activity_intent                character varying(35)
,location_point_type            character(16)
,point_sequence_number          numeric
,well_number                    character varying(35)
,pipe_number                    character(15)
,activity_stop_date_time        timestamp
,act_stop_time_zone             character varying(256)
,activity_rel_depth             character(15)
,activity_depth                 character varying(256)
,activity_depth_unit            character varying(256)
,activity_upper_depth           character varying(256)
,activity_lower_depth           character varying(256)
,upr_lwr_depth_unit             character varying(256)
,field_procedure_id             character varying(256)
,gear_config_id                 character varying(256)
,activity_latitude              numeric
,activity_longitude             numeric
,act_std_latitude               numeric
,act_std_longitude              numeric
,act_std_hdatum                 character(12)
,std_value                      numeric
,std_unit                       character varying(256)
,fk_act_mad_hdatum              integer
,fk_act_mad_hmethod             integer
,activity_isn                   numeric
,visit_start_date_time          timestamp
,visit_start_time_zone          character(3)
,visit_stop_date_time           timestamp
,visit_stop_time_zone           character(3)
,activity_matrix                character varying(256)
,field_set                      character varying(130)
,point_name                     character varying(30)
,sgo_indicator                  character(1)
,map_scale                      character varying(20)
,field_gear_id                  character varying(256)
,bias                           character varying(256)
,conf_lvl_corr_bias             character(1)
,result_comment                 character varying(4000)
,text_result                    character varying(4000)
,cas_number                     character varying(500)
,epa_reg_number                 character varying(500)
,itis_number                    character varying(500)
,container_desc                 character varying(256)
,temp_preservn_type             character varying(25)
,presrv_strge_prcdr             character varying(256)
,portable_data_logger           character varying(34)
,fk_stn_act_pt                  integer
,fk_statn_types                 integer
,last_userid                    character varying(100)
,last_change_date               timestamp
,blob_id                        character varying(25)
,blob_title                     character varying(256)
,act_blob_id                    character varying(25)
,act_blob_title                 character varying(256)
,activity_comment               character varying(4000)
,activity_depth_ref_point       character varying(256)
,project_id                     character varying(4000)
,tribal_water_quality_measure   character(1)
,result_meas_qual_code          character varying(256)
,activity_cond_org_text         character varying(256)
,result_depth_meas_value        character varying(256)
,result_depth_meas_unit_code    character varying(256)
,result_depth_alt_ref_pt_txt    character varying(256)
,analytical_method_list_agency  character varying(256)
,analytical_method_list_ver     character varying(256)
,smprp_transport_storage_desc   character varying(1999)
,source_system                  character varying(256)
,source_uid                     integer
,etl_id                         character varying(256)
,horizontal_accuracy_measure    character varying(256)
,lab_accred_authority           character varying(256)
,method_speciation              character varying(256)
,lab_samp_prp_method_id         character varying(4000)
,lab_samp_prp_start_date_time   timestamp
,lab_samp_prp_start_tmzone      character varying(256)
,lab_samp_prp_end_date_time     timestamp
,lab_samp_prp_end_tmzone        character varying(256)
,lab_samp_prp_dilution_factor   character varying(1024)
,sampling_point_name            character varying(256)
,last_transaction_id            character varying(256)
,fk_date_lc                     integer
,all_result_detection_limit     character varying(4000)
,confidence_interval            character varying(256)
,fk_gen_geo_state               integer
,fk_gen_geo_county              integer
,fk_method                      integer
,fk_chargrptype                 integer
,fk_chartype                    integer
,analysis_end_date_time         timestamp
,analysis_end_time_zone         character varying(256)
,field_prep_procedure_id        character varying(256)
,fk_date_act_stop               integer
);
create unlogged table if not exists STORETW.result_no_source_test
(data_source_id                 numeric
,data_source                    text
,station_id                     numeric
,site_id                        text
,event_date                     date
,analytical_method              text
,p_code                         text
,activity                       text
,characteristic_name            text
,characteristic_type            text
,sample_media                   text
,organization                   text
,site_type                      text
,huc                            character varying (12)
,governmental_unit_code         character varying (9)
,organization_name              text
,activity_type_code             text
,activity_media_subdiv_name     text
,activity_start_time            text
,act_start_time_zone            text
,activity_stop_date             text
,activity_stop_time             text
,act_stop_time_zone             text
,activity_depth                 text
,activity_depth_unit            text
,activity_depth_ref_point       text
,activity_upper_depth           text
,activity_upper_depth_unit      text
,activity_lower_depth           text
,activity_lower_depth_unit      text
,project_id                     text
,activity_conducting_org        text
,activity_comment               text
,sample_aqfr_name               text
,hydrologic_condition_name      text
,hydrologic_event_name          text
,sample_collect_method_id       text
,sample_collect_method_ctx      text
,sample_collect_method_name     text
,sample_collect_equip_name      text
,result_id                      numeric
,result_detection_condition_tx  text
,sample_fraction_type           text
,result_measure_value           text
,result_unit                    text
,result_meas_qual_code          text
,result_value_status            text
,statistic_type                 text
,result_value_type              text
,weight_basis_type              text
,duration_basis                 text
,temperature_basis_level        text
,particle_size                  text
,precision                      text
,result_comment                 text
,result_depth_meas_value        text
,result_depth_meas_unit_code    text
,result_depth_alt_ref_pt_txt    text
,sample_tissue_taxonomic_name   text
,sample_tissue_anatomy_name     text
,analytical_procedure_id        text
,analytical_procedure_source    text
,analytical_method_name         text
,analytical_method_citation     text
,lab_name                       text
,analysis_date_time             text
,lab_remark                     text
,detection_limit                text
,detection_limit_unit           text
,detection_limit_desc           text
,analysis_prep_date_tx          text
,activity_id                    numeric
)
with (fillfactor = 100)
