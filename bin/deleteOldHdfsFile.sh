#!/bin/bash
HDFSPATHtocheck=$1
echo "Scanning : $HDFSPATHtocheck"

today=`date +'%s'`
hdfs dfs -ls $HDFSPATHtocheck | grep "^d" | while read line ; do
dir_date=$(echo ${line} | awk '{print $6}')
difference=$(( ( ${today} - $(date -d ${dir_date} +%s) ) / ( 24*60*60 ) ))
filePath=$(echo ${line} | awk '{print $8}')

if [ ${difference} -gt 10 ]; then
   # hdfs dfs -rm -r $filePath
	echo "will delete $filePath"
fi
done