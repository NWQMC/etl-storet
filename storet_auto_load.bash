#!/bin/bash

export ORACLE_SID=barry
export ORACLE_HOME=`cat /etc/oratab | grep ^${ORACLE_SID}: | awk -F: '{print $2}'`
export PATH=$PATH:$ORACLE_HOME/bin

export work=/b_test_dir/oradata/storet_20110822
export file_stub=stormodb_shire_storetw
export explog=${file_stub}_expdp.log
export expref=${file_stub}_expdp.ref
export http_base=http://www.epa.gov/storet/download/storetw
export storetpass=`cat $work/.storet`
export date_suffix=`date +%Y%m%d_%H%M`

cd $work

(

curl $http_base/$explog > $explog 2> curlout.log.1

egrep '^Export|successfully completed' $explog
export table_count=`grep "exported " $explog | wc -l`
export complete_count=`grep "successfully completed" $explog | wc -l`

if [ $table_count -ne 14 -o $complete_count -ne 1 ] ; then
   echo "table_count not 14 or complete_count not 1 - "$table_count" "$complete_count". quitting."
   exit 1
fi

if [ -f $expref ] ; then
   diffs=`diff $explog $expref | wc -l`
   echo $diffs " differences found between log file and base ref"
   if [ $diffs -eq 0 ] ; then
      echo "Since no differences, quitting."
      exit 1
   fi
else
   echo "No reference for comparison."
fi


count=0

for i  in `grep orabackup $explog` ; do
   count=`expr $count + 1`
   basename=`basename $i`
   tries=1
   while [ $tries -le 3 ] ; do
      date
      echo "downloading "$basename" ...file "$count"   attempt "$tries
      curl $http_base/$basename > $basename 2> curlout.$count.$tries
      status=$?
      echo "status: "$status
      if [ $status -eq 0 ] ; then
         break
      else
         sleep 300
      fi
      tries=`expr $tries + 1`
   done
   if [ $tries -gt 3 ] ; then
      echo "failed to download "$basename"   quitting."
      exit 1
   fi
done

if [ $count -eq 0 ] ; then
   echo "no files found.  quitting."
   exit 1
fi

echo "downloaded "$count" files"

sqlplus /nolog << EOT
connect storetw/$storetpass@barry.er.usgs.gov
drop table fa_regular_result;
drop table fa_station;
drop table di_activity_matrix;
drop table di_activity_medium;
drop table di_characteristic;
drop table di_geo_county;
drop table di_geo_state;
drop table di_org;
drop table di_statn_types;
drop table lu_mad_hmethod;
drop table lu_mad_hdatum;
drop table lu_mad_vmethod;
drop table lu_mad_vdatum;
drop table mt_wh_config;
exit
EOT

impdp userid=STORETW/$storetpass@barry.er.usgs.gov DIRECTORY=storet dumpfile=${file_stub}_%u.cdmp logfile=${file_stub}_impdp.log "tables=(FA_STATION,FA_REGULAR_RESULT,DI_ACTIVITY_MATRIX,DI_ACTIVITY_MEDIUM,DI_CHARACTERISTIC,DI_GEO_COUNTY,DI_GEO_STATE,DI_ORG,DI_STATN_TYPES, LU_MAD_HMETHOD,LU_MAD_HDATUM,LU_MAD_VMETHOD,LU_MAD_VDATUM,MT_WH_CONFIG)" EXCLUDE=constraint,index,grant

status=$?
echo "import status: "$status

if [ $status -ne 0 ] ; then
   echo "trouble with import.  quitting."
   exit 1
fi

export work=/b_test_dir/oradata/storet_20110822
export storetpass=`cat $work/.storet`
export success_notify=bheck@usgs.gov
export failure_notify=bheck@usgs.gov
sqlplus /nolog << EOT
connect STORETMODERN/$storetpass@widw.er.usgs.gov
whenever sqlerror exit sql.sqlcode
set serveroutput on
set linesize 160
exec dbms_output.enable(100000);
declare
mesg varchar2(100);
begin
   mesg := null;
   create_storet_objects.main(mesg, '$success_notify', '$failure_notify');
   dbms_output.put_line(mesg);
   if mesg like 'FAIL%' then
      raise_application_error(-20000, mesg);
   end if;
end;
/
EOT

status=$?
echo "sqlplus status: "$status

if [ $status -ne 0 ] ; then
   echo "problems creating new tables.  quitting."
   exit 1
fi

mv $explog $expref

date
echo "storet import successful"

) 2>&1 | tee storet_auto_load_$date_suffix.out

exit 0 