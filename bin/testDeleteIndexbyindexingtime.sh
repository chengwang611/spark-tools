#!/usr/bin/env bash


i=1
current_date=$(date '+%Y-%m-%d')
today=`date +'%s'`

echo "$current_date"
HTTP_STATUS=$(curl -s -XGET "192.168.0.171:9200/_cat/indices/ga_index*?h=i,creation.date.string&s=creation.date")
echo "$HTTP_STATUS"
terms=$(echo $HTTP_STATUS | tr " " "\n")

for term in $terms
do
 #echo "$i"
 if [ $i -eq "1" ]
 then 
     index_name=$term 
 fi
 if [ $i -eq "2" ]
 then 
    index_date=$(echo $term | cut -d'T' -f 1)
    echo "name= $index_name  date=$index_date"
    difference=$(( ( ${today} - $(date -d "$index_date" +%s) ) / ( 24*60*60 ) ))

    echo "date difference is: $difference"
    if [ ${difference} -gt 7 ]; then
   # delete index 
	echo "will delete index: $index_name"
    fi
    i=0
 fi
 i=$((i+1))
 
done


