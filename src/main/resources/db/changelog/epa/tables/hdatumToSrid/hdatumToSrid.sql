create table if not exists ${STORETW_SCHEMA_NAME}.hdatum_to_srid
(fk_mad_hdatum                  numeric
,srid                           integer
)
with (fillfactor = 100)
