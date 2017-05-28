#!/bin/sh

URL=${1:-https://s3.amazonaws.com/carto-1000x/data/yellow_tripdata_2016-01.csv}
COLUMN_NAME=${2:-tip_amount}

TMPFILE=$(mktemp)

wget -q -O $TMPFILE "$URL"
FIELD=$(head -n1 $TMPFILE | tr "," "\n" | grep -nx "$COLUMN_NAME" |  cut -d":" -f1)
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