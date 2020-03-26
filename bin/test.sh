## run as :     run.sh 192.168.0.171:9200 my_ga_alias ga_day_index-0907 ga_index5
#!/usr/bin/env bash

## retrieve current alias
i=1
current_alias_index=""
HTTP_STATUS=$(curl -s -XGET "192.168.0.171:9200/_cat/indices/ga_index*?h=i,creation.date.string&s=creation.date")
terms=$(echo $HTTP_STATUS | tr " " "\n")
for datee in $terms
do
 echo "$datee"
 i=$((i+1))

done

echo "current index name for alias is:$current_alias_index"
