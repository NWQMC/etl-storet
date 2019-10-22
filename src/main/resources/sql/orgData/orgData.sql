insert
  into org_data_no_source (data_source_id, data_source, organization_id, organization, organization_name,
                           organization_description, organization_type)
select /*+ parallel(4) */
       3 data_source_id,
       'STORET' data_source,
       "PK_ISN" + 10000000  organization_id,
       "ORGANIZATION_ID" organization,
       "ORGANIZATION_NAME",
       "ORGANIZATION_DESCRIPTION",
       "ORGANIZATION_TYPE"
  from storetw_dump."DI_ORG"
 where "SOURCE_SYSTEM" is null and
       "ORGANIZATION_ID" not in (select org_id from storetw.storetw_transition)