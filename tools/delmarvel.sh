#!/bin/bash
SH="sh +x"
. /etc/profile
. ~/.profile

if [ $# == 1 ]; then
    STR_DATE=$1
else
    echo "usage:$0 DATE"
    exit 1
fi


############################################################
if [[ $STR_DATE = "0" ]]; then
    STR_DATE=`date +%Y.%m.%d`
elif [[ $STR_DATE = "-1" ]]; then
    STR_DATE=`date +%Y.%m.%d -d "1 days ago"`
fi

echo "${STR_DATE}"
IP="10.0.0.163"
echo "http://${IP}:9200/.marvel-${STR_DATE}"
curl -XDELETE "http://${IP}:9200/.marvel-${STR_DATE}"
echo ""

