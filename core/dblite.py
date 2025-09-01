
from sqlite3 import OperationalError, connect, Cursor
from atexit import register
import logging
from functools import cache
from collections import defaultdict

logger = logging.getLogger(__name__)


def dict_factory(cursor: Cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d


def gW(tp: tuple):
    if len(tp) == 0:
        return None
    if len(tp) == 1:
        return "= ?"
    prm = ", ".join(['?'] * len(tp))
    return f"in ({prm})"


def escape_fts5(text: str) -> str:
    text = text.replace('"', '""')
    return f'"{text}"'


class DBlite:
    def __init__(self, file: str):
        self.__file = file
        self.__con = None
        register(self.close)

    @property
    def file(self):
        return self.__file

    @property
    def con(self):
        if self.__con is None:
            logger.info(f"Connecting to {self.__file}")
            self.__con = connect(
                f"file:{self.__file}?mode=ro&immutable=1",
                uri=True
            )
        return self.__con

    def select(self, sql: str, *args, row_factory=None, **kwargs):
        self.con.row_factory = row_factory
        cursor = self.con.cursor()
        try:
            if len(args):
                cursor.execute(sql, args)
            else:
                cursor.execute(sql)
        except OperationalError:
            logger.critical(sql)
            raise
        for r in cursor:
            yield r
        cursor.close()
        self.con.row_factory = None

    def one(self, sql: str, *args, **kwargs):
        for r in self.select(sql, *args, **kwargs):
            return r[0] if len(r) == 1 else r

    def to_tuple(self, *args, **kwargs):
        arr = []
        for i in self.select(*args, **kwargs):
            if isinstance(i, (tuple, list)) and len(i) == 1:
                i = i[0]
            arr.append(i)
        return tuple(arr)

    def get_dict(self, *args, **kwargs):
        obj = dict()
        for k, v in self.select(*args, **kwargs):
            obj[k] = v
        return obj

    def get_dict_set(self, *args, **kwargs):
        obj = defaultdict(set)
        for k, v in self.select(*args, **kwargs):
            obj[k].add(v)
        return dict(obj)

    def close(self):
        if self.__con is None:
            return
        logger.info(f"Closing {self.__file}")
        self.__con.close()
        self.__con = None

    @cache
    def __search_person(self, name: str) -> tuple[str, ...]:
        if not isinstance(name, str):
            return tuple()
        name = name.strip().lower()
        if len(name) == 0:
            return tuple()
        sql = "select id from {t} where {w}"
        for t, w in (
            ('PERSON', "lower(name) = ? COLLATE NOCASE"),
            ('PERSON_FTS', "name MATCH ?"),
            #('', "lower(name) like ('%' || ? || '%')")
        ):
            n = escape_fts5(name) if t.endswith("_FTS") else name
            ids = self.to_tuple(sql.format(t=t, w=w), n)
            if len(ids):
                return ids
        return tuple()

    def search_person(self, *names: str):
        p: set[str] = set()
        for n in names:
            p = p.union(self.__search_person(n))
        return tuple(sorted(p))

    def search_imdb_id(
            self,
            *titles: str,
            year: int,
            director: tuple[str, ...] = None,
            duration: int = None,
            year_gap: int = 1,
            full_match: bool = False
    ) -> str | None:
        if director is None:
            director = None
        id_titles = self.__search_movie_by_title(
            *titles,
            min_year=year-year_gap if year else None,
            max_year=year+year_gap if year else None,
            duration=duration
        )
        if not full_match and len(id_titles) == 1:
            return id_titles[0]
        id_director = self.__search_movie_by_director(
            *director,
            min_year=year-year_gap if year else None,
            max_year=year+year_gap if year else None,
            duration=duration
        )
        if not full_match and len(id_director) == 1:
            return id_director[0]
        ok = set(id_director).intersection(id_titles)
        if len(ok) == 1:
            return ok.pop()
        return None

    @cache
    def __search_movie_by_title(self, *titles: str, min_year=None, max_year=None, duration: int = None) -> tuple[tuple[str, ...], ...]:
        arr_titles = []
        for t in map(str.strip, titles):
            if t and t not in arr_titles:
                arr_titles.append(t)
        if len(arr_titles) == 0:
            return tuple()
        sql = []
        arg = []
        for title in arr_titles:
            for t, w in (
                ('TITLE', "title = ? COLLATE NOCASE"),
                ('TITLE_FTS', "title MATCH ?"),
                #"lower(title) like ('%' || ? || '%')"
            ):
                tt = escape_fts5(title) if t.endswith("_FTS") else title
                sql.append(f"select movie from {t} where {w}")
                arg.append(tt)
        main_sql = "select distinct movie from (" + (" union ".join(sql)) + ")"
        where = []
        if duration:
            where.append(f"duration < {duration+10}")
            where.append(f"duration > {duration-10}")
        if min_year:
            where.append(f"year > {min_year}")
        if max_year:
            where.append(f"year < {max_year}")
        if where:
            main_sql = main_sql+" where movie in (select id from movie where " 
            main_sql = main_sql+(" and ".join(where)) + ")"
        ids = self.to_tuple(
            main_sql,
            *arg
        )
        return ids

    @cache
    def __search_movie_by_director(self, *directors: str, min_year=None, max_year=None, duration: int = None) -> tuple[tuple[str, ...], ...]:
        arr_directors = []
        for d in directors:
            d = d.strip()
            if d and d not in arr_directors:
                arr_directors.append(d)
        if len(arr_directors) == 0:
            return tuple()
        sql = []
        arg = []
        for director in arr_directors:
            for t, w in (
                ('PERSON', "lower(name) = ? COLLATE NOCASE"),
                ('PERSON_FTS', "name MATCH ?"),
                #('', "lower(name) like ('%' || ? || '%')")
            ):
                n = escape_fts5(director) if t.endswith("_FTS") else director
                sql.append(f"select id from {t} where {w}")
                arg.append(n)
        main_sql = "select distinct movie from director where person in (" + (" union ".join(sql)) + ")"
        where = []
        if duration:
            where.append(f"duration < {duration+10}")
            where.append(f"duration > {duration-10}")
        if min_year:
            where.append(f"year > {min_year}")
        if max_year:
            where.append(f"year < {max_year}")
        if where:
            main_sql = main_sql+" and movie in (select id from movie where "
            main_sql = main_sql+(" and ".join(where)) + ")"
        ids = self.to_tuple(
            main_sql,
            *arg
        )
        return ids


DB = DBlite("imdb.sqlite")


if __name__ == "__main__":
    pass
