#!/bin/bash

export work=/u01/oradata/dbstage/pdc_temp
export file_stub=stormodb_shire_storetw_Weekly
export explog=${file_stub}_expdp.log
export expref=${file_stub}_expdp.ref
export http_base=http://www.epa.gov/storet/download/storetw
export date_suffix=`date +%Y%m%d_%H%M`

cd $work

(

curl $http_base/$explog > $explog 2> curlout.log.1

egrep '^Export|successfully completed' $explog
export table_count=`grep "exported " $explog | wc -l`
export complete_count=`grep "successfully completed" $explog | wc -l`

if [ $table_count -ne $exp_table_count -o $complete_count -ne 1 ] ; then
   echo "table_count("$table_count") not $exp_table_count or complete_count("$complete_count") not 1. quitting."
   exit 1
fi

if [ -f $expref ] ; then
   diffs=`diff $explog $expref | wc -l`
   echo $diffs " differences found between log file and base ref"
   if [ $diffs -eq 0 ] ; then
      echo "Since no differences, we are done."
      exit 1
   fi
else
   echo "No reference for comparison."
fi


files=`grep orabackup test.log | sed -e 's/^.*\//http:\/\/www.epa.gov\/storet\/download\/storetw\//'`
echo $files
echo $files | xargs -n 1 -P 8 wget -q

) 2>&1 | tee storet_dump_$date_suffix.out
