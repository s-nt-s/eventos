#!/bin/bash

OUT="out/$1"
mkdir -p "$(dirname "$OUT")"
wget -q --tries=3 --timeout=10 "$PAGE_URL/$1" -O "$OUT" || echo "$2" > "$OUT"