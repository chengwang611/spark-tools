## run as :     run.sh 192.168.0.171:9200 my_ga_alias ga_day_index-0907 ga_index5
#!/usr/bin/env bash
ADDRESS=$1
ALIAS=$2
USERID=$3
PASSWORD=$4
OLD_INDEX=$5
NEW_INDEX=$6

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

curl --user USERID:PASSWORD -XPOST 'http://192.168.0.171:9200/_aliases' -H'Content-Type: application/json' -d '
{
"actions": [
{ "remove": { "index": "'"$OLD_INDEX"'", "alias": "'"$ALIAS"'" }},
{ "add": { "index": "'"$NEW_INDEX"'", "alias": "'"$ALIAS"'" }}
]
}
'