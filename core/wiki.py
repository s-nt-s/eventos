import logging
import re
from core.git import G
from core.filemanager import FM
from sparql_tsv import SparqlTsv
from typing import Optional
from collections import defaultdict


logger = logging.getLogger(__name__)


class WikiApi:
    def __init__(self):
        # https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_User-Agent_Policy
        self.__st = SparqlTsv(
            endpoint="https://query.wikidata.org/sparql",
            user_agent=f'ImdbBoot/0.0 ({G.remote}; {G.mail})',
            max_retries=3
        )

    def get_filmaffinity(self, *args) -> dict[str, int]:
        logger.debug(f"get_filmaffinity{args}")
        try:
            r = self.__get_filmaffinity(*args)
            logger.debug(f"get_filmaffinity{args} = {len(r)}")
            return r
        except Exception as e:
            logger.debug(f"get_filmaffinity{args} = {e}")
        return {}

    def __get_filmaffinity(self, *args):
        r: dict[str, int] = dict()
        template = FM.load("sparql/imdb_film.sparql")
        for k, v in self.__get_dict(
            *args,
            template=template,
            re_k=r"^tt\d{3,}$",
            re_v=r"^\d{3,}$",
        ).items():
            r[k] = int(v)
        return r

    def __get_dict(
        self,
        *args,
        template: str = None,
        re_k: Optional[str] = None,
        re_v: Optional[str] = None
    ):
        if len(args) == 0:
            return {}
        r_k = re.compile(re_k) if re_k else None
        r_v = re.compile(re_v) if re_v else None
        k_v: dict[str, set[str]] = defaultdict(set)
        v_k: dict[str, set[str]] = defaultdict(set)
        ids = " ".join(map(lambda x: f'"{x}"', sorted(set(args))))
        query = re.sub(
            r"(\bVALUES\s+\?\w+\s*\{)\s*\}",
            r"\1 " + ids + r" }",
            template
        )
        for k, v in self.__st.query(query):
            if not k or not v:
                continue
            if r_k and not r_k.match(k):
                continue
            if r_v and not r_v.match(v):
                continue
            k_v[k].add(v)
            v_k[v].add(k)
        r: dict[str, str] = {}
        for k, vals in k_v.items():
            if len(vals) != 1:
                continue
            v = vals.pop()
            if len(v_k[v]) == 1:
                r[k] = v
        return r


WIKI = WikiApi()


if __name__ == "__main__":
    from core.log import config_log
    config_log("log/wiki.log", log_level=logging.DEBUG)
    import sys
    print(WIKI.get_filmaffinity(*sys.argv[1:]))