## run as :     run.sh 192.168.0.171:9200 my_ga_alias ga_day_index-0907 ga_index5
#!/usr/bin/env bash
ADDRESS=$1
USERID=$2
PASSWORD=$3
ALIAS=$4
OLD_INDEX=$5
NEW_INDEX=$6

echo "*****swapp  $ALIAS from  $OLD_INDEX to $NEW_INDEX*****"

if [ -z $ADDRESS ]; then
  ADDRESS="localhost:9200"
fi

# Check that Elasticsearch is running
curl -s "http://$ADDRESS" 2>&1 > /dev/null
if [ $? != 0 ]; then
    echo "Unable to contact Elasticsearch at $ADDRESS"
    echo "Please ensure Elasticsearch is running and can be reached at http://$ADDRESS/"
    exit -1
fi

HTTP_STATUS=$(curl   --user USERID:PASSWORD -XPOST "http://$ADDRESS/_aliases" -H'Content-Type: application/json' -d '
{
"actions": [
{ "remove": { "index": "'"$OLD_INDEX"'", "alias": "'"$ALIAS"'" }}
]
}
')  
GOOD_STATUS='"acknowledged":true'
if grep -v "$GOOD_STATUS" <<< "$HTTP_STATUS"; then
   echo " remove alias job failed! $HTTP_STATUS"
   exit -1

fi
 echo "*****remove alias job successed ! $HTTP_STATUS*****"

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
