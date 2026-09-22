#!/bin/bash
URL="https://s-nt-s.github.io/imdb-sql/imdb.sqlite.zst"
rm -rf imdb.sqlite
echo "[..] $URL"
curl -sqL "$URL" | zstd -dq -o imdb.sqlite
if [ $? -eq 0 ]; then
    echo "[OK] $URL"
else
    echo "[KO] $URL"
fi
ls -lah imdb.sqlite