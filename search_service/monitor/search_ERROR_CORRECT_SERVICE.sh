#!/bin/bash
time_now=`date +%H:%M`
time_error=`grep "WARNING ERROR_CORRECT_SERVICE" /var/log/supervisor/search/server.log | awk '{print $2}'|cut -d: -f 1,2|tail -1`
if [ $time_now == $time_error ]
   then
   echo "1"
   else
   echo "0"
fi
 
