insert
  into project_data_no_source (data_source_id,
                               project_id,
                               data_source,
                               organization,
                               organization_name,
                               project_identifier,
                               project_name,
                               description,
                               sampling_design_type_code,
                               qapp_approved_indicator,
                               qapp_approval_agency_name
                              )
select 3 data_source_id,
       di_project."PK_ISN" + 100000000000 project_id,
       'STORET' data_source,
       di_org."ORGANIZATION_ID" organization,
       di_org."ORGANIZATION_NAME" organization_name,
       di_project."PROJECT_CD" project_identifier,
       di_project."PROJECT_NAME" project_name,
       di_project."PROJECT_DESCRIPTION" project_description,
       di_project."SAMPLING_DESIGN_TYPE_CD" sampling_design_type_cd,
       di_project."QA_APPROVED" qapp_approved_indicator,
       di_project."QA_APPROVAL_AGENCY" qapp_approval_agency_name
  from storetw_dump."DI_PROJECT" di_project
       join storetw_dump."DI_ORG" di_org
         on di_project."FK_ORG" = di_org."PK_ISN"
 where di_project."SOURCE_SYSTEM" is null and
       di_project."TSMPROJ_ORG_ID" not in (select org_id from storetw.storetw_transition)
