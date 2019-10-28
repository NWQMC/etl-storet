insert
  into station_no_source (data_source_id,
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
select 3 data_source_id,
       'STORET' data_source,
       fa_station."PK_ISN" station_id,
       fa_station."ORGANIZATION_ID" || '-' || fa_station."STATION_ID" site_id,
       fa_station."ORGANIZATION_ID" organization,
       fa_station."STATION_GROUP_TYPE" site_type,
       fa_station."GENERATED_HUC" huc,
       di_geo_state."COUNTRY_CODE" || ':' || rtrim(di_geo_state."FIPS_STATE_CODE") || ':' || di_geo_county."FIPS_COUNTY_CODE" governmental_unit_code,
       case
         when fa_station."LONGITUDE" is not null and fa_station."LATITUDE" is not null
           then st_transform(st_SetSrid(st_MakePoint(fa_station."LONGITUDE", fa_station."LATITUDE"), coalesce(hdatum_to_srid.srid, 4269)),  4269)
       end geom,
       trim(fa_station."STATION_NAME") station_name,
       di_org."ORGANIZATION_NAME",
       trim(fa_station."DESCRIPTION_TEXT") description_text,
       fa_station."LATITUDE",
       fa_station."LONGITUDE",
       substring(fa_station."MAP_SCALE", '[[:digit:]]+$') map_scale,
       coalesce(lu_mad_hmethod."GEOPOSITIONING_METHOD", 'Unknown') geopositioning_method,
       coalesce(rtrim(lu_mad_hdatum."ID_CODE"), 'Unknown') hdatum_id_code,
       substring(fa_station."ELEVATION", '^[[:digit:]]+') elevation_value,
       case
         when fa_station."ELEVATION" is not null
           then coalesce(fa_station."ELEVATION_UNIT", 'ft')
       end elevation_unit,
       case
         when fa_station."ELEVATION" is not null
           then lu_mad_vmethod."ELEVATION_METHOD"
       end elevation_method,
       case
         when fa_station."ELEVATION" is not null
           then coalesce(lu_mad_vdatum."ID_CODE", 'Unknown')
       end vdatum_id_code,
       fa_station."FK_PRIMARY_TYPE" station_type_name
  from storetw_dump."FA_STATION" fa_station
       left join storetw.hdatum_to_srid
         on fa_station."FK_MAD_HDATUM" = hdatum_to_srid.fk_mad_hdatum
       left join storetw_dump."DI_ORG" di_org
         on "FK_ORG" = di_org."PK_ISN"
       left join storetw_dump."DI_GEO_STATE" di_geo_state
         on "FK_GEO_STATE" = di_geo_state."PK_ISN"
       left join storetw_dump."DI_GEO_COUNTY" di_geo_county
         on "FK_GEO_COUNTY" = di_geo_county."PK_ISN"
       left join storetw_dump."LU_MAD_HMETHOD" lu_mad_hmethod
         on "FK_MAD_HMETHOD" = lu_mad_hmethod."PK_ISN"
       left join storetw_dump."LU_MAD_HDATUM" lu_mad_hdatum
         on "FK_MAD_HDATUM" = lu_mad_hdatum."PK_ISN"
       left join storetw_dump."LU_MAD_VMETHOD" lu_mad_vmethod
         on "FK_MAD_VMETHOD" = lu_mad_vmethod."PK_ISN"
       left join storetw_dump."LU_MAD_VDATUM" lu_mad_vdatum
         on "FK_MAD_VDATUM" = lu_mad_vdatum."PK_ISN"
 where fa_station."LOCATION_POINT_TYPE" = '*POINT OF RECORD' and
       fa_station."SOURCE_SYSTEM" is null and
       fa_station."ORGANIZATION_ID" not in (select org_id from storetw.storetw_transition)
