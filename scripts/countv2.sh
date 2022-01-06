#!/bin/bash

path=$1
mode=$2

current=-1
count=0

sleep 5
while [ $current -ne $count ]; do
    count=$(find $path -name *.dcm | wc -l)
    echo -ne "$count number of dicoms currently $mode.\r"
    current=$count
    sleep 10
    count=$(find $path -name *.dcm | wc -l)

    if [ $current -eq $count ]
    then 
        echo "Final check, waiting for 180s incase of PACS stall."
        sleep 180
        count=$(find $path -name *.dcm | wc -l)
    fi 
done

count=$(find $path -name *.dcm | wc -l)
echo "$count total dicoms have been $mode."
echo "PROCESS COMPLETE! :]"
