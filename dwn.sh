#!/bin/bash
rm -rf imdb.sqlite
curl -L https://s-nt-s.github.io/imdb-sql/imdb.sqlite.zst | zstd -d -o imdb.sqlite
ls -lah imdb.sqlite