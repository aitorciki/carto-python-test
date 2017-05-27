#!/bin/sh

TMPFILE=$(mktemp)

wget -O $TMPFILE https://s3.amazonaws.com/carto-1000x/data/yellow_tripdata_2016-01.csv
wc -l $TMPFILE
FIELD=$(head -n1 $TMPFILE | tr "," "\n" | grep -nx tip_amount |  cut -d":" -f1)
awk -F , -v field=$FIELD '{ sum += $field; n++ } END { if (n > 0) print sum / n; }' $TMPFILE

rm -f $TMPFILE