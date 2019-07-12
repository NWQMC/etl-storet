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
,last_change_date          date
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
,last_change_date              date
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

