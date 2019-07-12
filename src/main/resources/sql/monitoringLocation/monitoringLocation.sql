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
       storetw.fa_station.pk_isn station_id,
       storetw.fa_station.organization_id || '-' || storetw.fa_station.station_id site_id,
       storetw.fa_station.organization_id organization,
       storetw.fa_station.station_group_type site_type,
       storetw.fa_station.generated_huc huc,
       di_geo_state.country_code || ':' || rtrim(di_geo_state.fips_state_code) || ':' || di_geo_county.fips_county_code governmental_unit_code,
       st_transform(storetw.fa_station.geom, 4269) geom,
       trim(storetw.fa_station.station_name) station_name,
       di_org.organization_name,
       trim(storetw.fa_station.description_text) description_text,
       storetw.fa_station.latitude,
       storetw.fa_station.longitude,
       substring(storetw.fa_station.map_scale, '[[:digit:]]+$') map_scale,
       coalesce(lu_mad_hmethod.geopositioning_method, 'Unknown') geopositioning_method,
       coalesce(rtrim(lu_mad_hdatum.id_code), 'Unknown') hdatum_id_code,
       substring(storetw.fa_station.elevation, '^[[:digit:]]+') elevation_value,
       case
         when storetw.fa_station.elevation is not null
           then coalesce(storetw.fa_station.elevation_unit, 'ft')
       end elevation_unit,
       case
         when storetw.fa_station.elevation is not null
           then lu_mad_vmethod.elevation_method
       end elevation_method,
       case
         when storetw.fa_station.elevation is not null
           then coalesce(lu_mad_vdatum.id_code, 'Unknown')
       end vdatum_id_code,
       storetw.fa_station.fk_primary_type station_type_name
  from storetw.fa_station
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
 where storetw.fa_station.location_point_type = '*POINT OF RECORD' and
       storetw.fa_station.organization_id not in(select org_id from storetw.storetw_transition)
