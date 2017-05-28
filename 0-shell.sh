#!/bin/sh

TMPFILE=$(mktemp)

wget -q -O $TMPFILE https://s3.amazonaws.com/carto-1000x/data/yellow_tripdata_2016-01.csv
FIELD=$(head -n1 $TMPFILE | tr "," "\n" | grep -nx tip_amount |  cut -d":" -f1)
awk -F , -v field=$FIELD '
    {
        sum += $field;
        n++;
    }
    END {
        print "Total lines: " n - 1;
        print "Average tip_amount: " sum / n;
    }' $TMPFILE

rm -f $TMPFILE