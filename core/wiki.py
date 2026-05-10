from textwrap import dedent
import logging
from typing import Any
import re
from time import sleep
from functools import wraps
from collections import defaultdict
from datetime import datetime, timedelta
from core.util import iter_chunk
import json
from core.git import G
from core.filemanager import FM
from sparql_tsv import SparqlTsv


logger = logging.getLogger(__name__)
re_imdb = re.compile(r"^tt\d{3,}$")
re_film = re.compile(r"^\d{3,}$")


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


def _read(file: str):
    path = FM.resolve_path(file)
    return path.read_text()


class WikiApi:
    def __init__(self):
        # https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_User-Agent_Policy
        self.__st = SparqlTsv(
            endpoint="https://query.wikidata.org/sparql",
            user_agent=f'ImdbBoot/0.0 ({G.remote}; {G.mail})',
        )

    def get_filmaffinity(self, *args):
        r: dict[str, int] = dict()
        template = _read("sparql/imdb_film.sparql")
        for k, v in self.__get_dict(
            *args,
            template=template
        ).items():
            if k and re_imdb.match(k):
                for i in list(v):
                    if not i or not re_film.match(i):
                        v.remove(i)
                v.discard(None)
                if len(v) == 1:
                    r[k] = int(v.pop())
        return r
    
    @retry_fetch(chunk_size=300)
    def __get_dict(
        self,
        *args,
        template: str = None
    ):
        r: dict[str, set[str]] = defaultdict(set)
        ids = " ".join(map(lambda x: f'"{x}"', args))
        query = re.sub(
            r"(\bVALUES\s+\?\w+\s*\{)\s*\}",
            r"\1 " + ids + r" }",
            template
        )
        for k, v in self.__st.query(query):
            r[k].add(v)
        return r

WIKI = WikiApi()


if __name__ == "__main__":
    import sys
    print(WIKI.get_filmaffinity(*sys.argv[1:]))