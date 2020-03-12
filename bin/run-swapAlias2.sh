## run as :     run.sh 192.168.0.171:9200 my_ga_alias ga_day_index-0907 ga_index5
#!/usr/bin/env bash
ADDRESS=$1
USERID=$2
PASSWORD=$3
ALIAS=$4
echo "current alias is : $ALIAS"

## retrieve current alias
i=1
current_alias_index=""
HTTP_STATUS=$(curl --user USERID:PASSWORD -s -XGET "http://$ADDRESS/_cat/aliases/$ALIAS")
terms=$(echo $HTTP_STATUS | tr " " "\n")
for term in $terms
do
 if [ $i -eq "2" ]
 then 
     current_alias_index=$term
 fi
 i=$((i+1))
done
echo "current index name for alias is:$current_alias_index"

OLD_INDEX=$current_alias_index
## estimate the target index name
indexname=$5
current_date=$(date '+%Y%m%d')
current_index_name=$indexname-$current_date
echo "alias will switch to new index: $current_index_name"

NEW_INDEX=$current_index_name

GOOD_STATUS='"acknowledged":true'
if [ -z $ADDRESS ]; then
  ADDRESS="localhost:9200"
fi

if [ -z "$OLD_INDEX" ]
then
      echo "index for alias is no exist"
else
HTTP_STATUS=$(curl   --user USERID:PASSWORD -XPOST "http://$ADDRESS/_aliases" -H'Content-Type: application/json' -d '
{
"actions": [
{ "remove": { "index": "'"$OLD_INDEX"'", "alias": "'"$ALIAS"'" }}
]
}
')  

if grep -v "$GOOD_STATUS" <<< "$HTTP_STATUS"; then
   echo " remove alias job failed! $HTTP_STATUS"
   exit -1

fi
 echo "*****remove alias job successed ! $HTTP_STATUS*****"

fi

HTTP_STATUS=$(curl   --user USERID:PASSWORD -XPOST "http://$ADDRESS/_aliases" -H'Content-Type: application/json' -d '
{
"actions": [
{ "add": { "index": "'"$NEW_INDEX"'", "alias": "'"$ALIAS"'" }}  
]
}
')
if grep -v "$GOOD_STATUS" <<< "$HTTP_STATUS"; then
   echo " add  alias job failed! $HTTP_STATUS"
   exit -1

fi
echo "*****swapping  successed : $HTTP_STATUS*****"
