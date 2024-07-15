#!/bin/bash

URL="https://glad.umd.edu/dataset/croplands"

MATCH_STRING=".tif"

TEMP_HTML="temp.html"

wget -O $TEMP_HTML $URL

cat $TEMP_HTML | grep -oP 'href="\K[^"]+' | grep "$MATCH_STRING" | while read -r FILE_URL ; do
    wget "$FILE_URL"
done

rm $TEMP_HTML