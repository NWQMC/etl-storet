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

if [ $table_count -ne 28 -o $complete_count -ne 1 ] ; then
   echo "table_count("$table_count") not 28 or complete_count("$complete_count") not 1. quitting."
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
      curl --header 'accept encoding=gzip' --retry 3 $http_base/$basename > $basename 2> curlout.$count.$tries
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

) 2>&1 | tee storet_dump_$date_suffix.out

echo "got here"

exit 0 