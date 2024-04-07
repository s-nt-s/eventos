#!/bin/bash
echo "Archive: $1"
curl -sS "https://web.archive.org/save/$1" --compressed -X POST --data "url=$1" > /dev/null 2>&1
exit 0
