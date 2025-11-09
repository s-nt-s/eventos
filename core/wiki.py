from textwrap import dedent
import logging
from typing import Any
import re
from time import sleep
from functools import wraps
from collections import defaultdict
from datetime import datetime, timedelta
from core.util import iter_chunk
from urllib.error import HTTPError
import json
from requests import Session
from core.git import G


logger = logging.getLogger(__name__)
re_sp = re.compile(r"\s+")
LANGS = ('es', 'en', 'ca', 'gl', 'it', 'fr')


def _parse_wiki_val(s: str):
    if not isinstance(s, str):
        return s
    s = s.strip()
    if len(s) == 0:
        return None
    if s.startswith("http://www.wikidata.org/.well-known/genid/"):
        return None
    m = re.match(r"https?://www\.wikidata\.org/entity/(Q\d+)", s)
    if m:
        return f"wd:{m.group(1)}"
    return s


class WikiError(Exception):
    def __init__(self, msg: str, query: str, http_code: int):
        super().__init__(f"{msg}\n{query}")
        self.__query = query
        self.__msg = msg
        self.__http_code = http_code

    @property
    def msg(self):
        return self.__msg

    @property
    def http_code(self):
        return self.__http_code

    @property
    def query(self):
        return self.__query


def retry_fetch(chunk_size=5000):
    def decorator(func):
        internal_cache: dict[tuple[str, str], Any] = {}

        @wraps(func)
        def wrapper(self: "WikiApi", *args, **kwargs):
            undone = set(args).difference({None, ''})
            if len(undone) == 0:
                return {}
            key_cache = json.dumps((func.__name__, kwargs), sort_keys=True)
            result = dict()
            for a in undone:
                val = internal_cache.get((key_cache, a))
                if val is not None:
                    result[a] = val
                    undone.discard(a)

            def _log_line(rgs: tuple, kw: dict, ck: int):
                rgs = sorted(set(rgs))
                line = ", ".join(
                    [f"{len(rgs)} ids [{rgs[0]} - {rgs[-1]}]"] +
                    [f"{k}={v}" for k, v in kw.items()] +
                    [f"chunk_size={ck}"]
                )
                return f"{func.__name__}({line})"

            error_query = {}
            count = 0
            tries = 0
            until = datetime.now() + timedelta(seconds=60*5)
            cur_chunk_size = int(chunk_size)
            while undone and (tries == 0 or (datetime.now() < until and tries < 3)):
                error_query = {}
                tries = tries + 1
                if tries > 1:
                    cur_chunk_size = max(1, min(cur_chunk_size, len(undone)) // 3)
                    sleep(5)
                logger.info(_log_line(undone, kwargs, cur_chunk_size))
                for chunk in iter_chunk(cur_chunk_size, list(undone)):
                    count += 1
                    fetched: dict = None
                    try:
                        fetched = func(self, *chunk, **kwargs) or {}
                        fetched = {k: v for k, v in fetched.items() if v}
                    except WikiError as e:
                        logger.warning(f"└ [KO] {e.msg}")
                        if e.http_code == 429:
                            sleep(60)
                        elif e.http_code is not None:
                            last_error = error_query.get(e.http_code)
                            if last_error is None or len(last_error) > len(e.query):
                                error_query[e.http_code] = str(e.query)
                    if not fetched:
                        continue
                    for k, v in fetched.items():
                        result[k] = v
                        internal_cache[(key_cache, k)] = v
                        undone.remove(k)
                    logger.debug(f"└ [{count}] [{chunk[0]} - {chunk[-1]}] = {len(fetched)} items")

            logger.info(f"{_log_line(args, kwargs, chunk_size)} = {len(result)} items")
            for c, q in error_query.items():
                logger.warning(f"STATUS_CODE {c} for:\n{q}")
            return result

        return wrapper
    return decorator


class WikiApi:
    def __init__(self):
        # https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_User-Agent_Policy
        self.__last_query: str | None = None
        self.__s = Session()
        self.__s.headers = {
            'User-Agent': f'ImdbBoot/0.0 ({G.remote}; {G.mail})',
            "Accept": "application/sparql-results+json",
            'Content-Type': 'application/sparql-query'
        }

    @property
    def last_query(self):
        return self.__last_query

    def query_sparql(self, query: str) -> dict:
        # https://query.wikidata.org/
        query = dedent(query).strip()
        query = re.sub(r"\n(\s*\n)+", "\n", query)
        self.__last_query = query
        try:
            r = self.__s.post(
                "https://query.wikidata.org/sparql",
                data=self.__last_query.encode('utf-8')
            )
            return r.json()
        except Exception as e:
            code = e.code if isinstance(e, HTTPError) else None
            raise WikiError(str(e), self.__last_query, http_code=code) from e

    def query(self, query: str) -> list[dict[str, Any]]:
        data = self.query_sparql(query)
        if not isinstance(data, dict):
            raise WikiError(str(data), self.__last_query)
        result = data.get('results')
        if not isinstance(result, dict):
            raise WikiError(str(data), self.__last_query)
        bindings = result.get('bindings')
        if not isinstance(bindings, list):
            raise WikiError(str(data), self.__last_query)
        for i in bindings:
            if not isinstance(i, dict):
                raise WikiError(str(data), self.__last_query)
            if i.get('subject') and i.get('object'):
                raise WikiError(str(data), self.__last_query)
        return bindings

    def get_filmaffinity(self, *args):
        r: dict[str, int] = dict()
        for k, v in self.get_dict(
            *args,
            key_field='wdt:P345',
            val_field='wdt:P480'
        ).items():
            vals = set(v)
            if len(vals) == 1:
                r[k] = vals.pop()
        return r

    @retry_fetch(chunk_size=300)
    def get_dict(
        self,
        *args,
        key_field: str = None,
        val_field: str = None,
        by_field: str = None
    ) -> dict[str, tuple[str | int, ...]]:
        ids = " ".join(map(lambda x: x if x.startswith("wd:") else f'"{x}"', args))
        if by_field:
            query = dedent('''
                SELECT ?k ?v WHERE {
                    VALUES ?k { %s }
                    ?item %s ?k ;
                          %s ?b .
                       ?b %s ?v .
                }
            ''').strip() % (
                ids,
                key_field,
                by_field,
                val_field,
            )
        elif key_field is None:
            query = dedent('''
                SELECT ?k ?v WHERE {
                    VALUES ?k { %s }
                    ?k %s ?v.
                }
            ''').strip() % (
                ids,
                val_field,
            )
        else:
            query = dedent('''
                SELECT ?k ?v WHERE {
                    VALUES ?k { %s }
                    ?item %s ?k.
                    ?item %s ?v.
                }
            ''').strip() % (
                ids,
                key_field,
                val_field,
            )
        r = defaultdict(list)
        for i in self.query(query):
            k = _parse_wiki_val(i['k']['value'])
            v = _parse_wiki_val(i.get('v', {}).get('value'))
            if v is None:
                continue
            if v.isdigit():
                v = int(v)
            r[k].append(v)
        r = {k: tuple(v) for k, v in r.items()}
        return r

    @retry_fetch(chunk_size=1000)
    def get_wiki_url(self, *args):
        ids = " ".join(map(lambda x: f'"{x}"', args))
        order = []
        for i, lang in enumerate(LANGS, start=1):
            order.append(f'IF(CONTAINS(STR(?site), "://{lang}.wikipedia.org"), {i},')
        len_order = len(order)
        order.append(f"{len_order}" + (')' * len_order))
        order_str = " ".join(order)

        bindings = self.query(
            """
                SELECT ?imdb ?article WHERE {
                VALUES ?imdb { %s }

                ?item wdt:P345 ?imdb .
                ?article schema:about ?item ;
                        schema:isPartOf ?site .

                FILTER(CONTAINS(STR(?site), "wikipedia.org"))

                BIND(
                    %s
                    AS ?priority
                )

                {
                    SELECT ?imdb (MIN(?priority) AS ?minPriority) WHERE {
                    VALUES ?imdb { %s }
                    ?item wdt:P345 ?imdb .
                    ?article schema:about ?item ;
                            schema:isPartOf ?site .
                    FILTER(CONTAINS(STR(?site), "wikipedia.org"))
                    BIND(
                        %s
                        AS ?priority
                    )
                    }
                    GROUP BY ?imdb
                }

                FILTER(?priority = ?minPriority)
                }
                ORDER BY ?imdb
            """ % (ids, order_str, ids, order_str)
        )
        obj: dict[str, set[str]] = defaultdict(set)
        for i in bindings:
            k = i['imdb']['value']
            v = i.get('article', {}).get('value')
            if isinstance(v, str):
                v = v.strip()
            if v is None or (isinstance(v, str) and len(v) == 0):
                continue
            obj[k].add(v)
        obj = {k: v.pop() for k, v in obj.items() if len(v) == 1}
        return obj


WIKI = WikiApi()
