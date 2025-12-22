import requests
from typing import Dict, Set, List, Union
from functools import cached_property
import logging
import json
from core.web import Web, WebException
from core.cache import Cache, TupleCache
from core.event import Event, Session, Place, Category, FieldNotFound
from core.filemanager import FM
from core.util import re_or, re_and

logger = logging.getLogger(__name__)


class CineEntradasException(Exception):
    pass


class CinemaCache(Cache):
    def __init__(self, file: str, *args, reload: bool = False, skip: bool = False, maxOld=1, loglevel=None, **kwargs):
        super().__init__(file, *args, kwself="slf", reload=reload,
                         skip=skip, maxOld=maxOld, loglevel=loglevel, **kwargs)

    def parse_file_name(self, *args, slf: "CineEntradas" = None, **kargv):
        movies = ",".join(args)
        if len(movies) == 0:
            movies = "all"
        return self.file.format(cinema=slf.cinema, movies=movies)


class CinemaEventCache(TupleCache):
    def __init__(self, file: str, *args, reload: bool = False, skip: bool = False, maxOld=1, loglevel=None, **kwargs):
        super().__init__(file, *args, kwself="slf", reload=reload,
                         skip=skip, maxOld=maxOld, loglevel=loglevel, builder=Event.build, **kwargs)

    def parse_file_name(self, *args, slf: "CineEntradas" = None, **kargv):
        movies = ",".join(args)
        if len(movies) == 0:
            movies = "all"
        return self.file.format(cinema=slf.cinema, movies=movies)


def hasMorePages(js: Union[Dict, List]):
    if isinstance(js, dict):
        if js.get("hasMorePages"):
            return True
        js = list(js.values())
    if isinstance(js, list):
        for i in js:
            if hasMorePages(i):
                return True
    return False


class CineEntradas:
    SALA_BERLANGA = 2369

    def __init__(self, cinema: int, price: float):
        self.cinema = cinema
        self.price = price

    def iter_graphql(self, data: Dict):
        logger.debug("graphql operationName="+data.get("operationName"))
        while True:
            r = requests.post(
                'https://entradas-next-live.kinoheld.de/graphql',
                headers={'content-type': 'application/json'},
                json=data
            )
            js = r.json()
            if not isinstance(js, dict) or js.get('errors'):
                raise CineEntradasException(js)
            yield js['data']
            if not hasMorePages(js):
                break
            data['variables']['page'] = data['variables'].get('page', 1) + 1

    def graphql(self, data: Dict):
        gen = self.iter_graphql(data)
        val = next(gen)
        nxt = next(gen, None)
        if nxt is not None:
            raise NotImplementedError("Pagination is not supported")
        return val

    @property
    @CinemaCache("rec/cineentradas/{cinema}.json")
    def info(self):
        data = {
            "operationName": "FetchCinemas",
            "variables": {
                "ids": [str(self.cinema)],
                "buildingType": {}
            },
            "query": FM.load("graphql/cineentradas/cinema.gql")
        }
        js = self.graphql(data)
        dt = js['cinemas']['data'][0]
        cinema = dt['urlSlug']
        city = dt['city']['urlSlug']
        root = f"https://cine.entradas.com/cine/{city}/{cinema}"
        logger.debug(root)

        def __get(slc: str, *urls):
            w = Web()
            w.s.headers.update({'Accept-Encoding': 'gzip, deflate'})
            for i, url in enumerate(urls):
                w.get(url)
                try:
                    return w.select_one(slc)
                except WebException:
                    if i == len(urls)-1:
                        raise
        n = __get('script[type="application/ld+json"]', root, root+"/sesiones")
        js = n.get_text()
        while isinstance(js, str):
            js = json.loads(js)
        ad = js['address']
        dt['address'] = ", ".join((
            ad['streetAddress'].title(),
            ad['postalCode'],
            ad['addressLocality']
        ))
        return dt

    @cached_property
    def movies(self):
        js = self.graphql({
            "operationName": "FetchShowGroupsFilters",
            "variables": {
                "cinemaId": str(self.cinema),
                "playing": {},
                "filters": ["showGroups"]
            },
            "query": FM.load("graphql/cineentradas/movies.gql")
        })
        for f in js['showGroups']['filterOptions']:
            if f['label'] == 'Movie':
                return f['values']
        raise FieldNotFound("showGroups/filterOptions[label='Movie']/values", js)

    @CinemaCache("rec/cineentradas/{cinema}/{movies}.json")
    def get_sessions(self, *movies: str):
        data = {
            "operationName": "FetchShowGroupsForCinema",
            "variables": {
                "cinemaId": str(self.cinema),
                "playing": {}
            },
            "query": FM.load("graphql/cineentradas/sessions.gql")
        }
        if len(movies) > 0:
            data['variables']['showGroups'] = list(movies)
        arr = []
        for js in self.iter_graphql(data):
            arr.extend(js['showGroups']['data'])
        return arr

    @property
    @CinemaEventCache("rec/cineentradas{cinema}.json")
    def events(self):
        logger.info("Cine Entradas: Buscando eventos")
        events: Set[Event] = set()
        for i in self.get_sessions():
            category = Category.CINEMA
            city = i['cinema']['city']['urlSlug']
            movie = i['movie']['urlSlug']
            cinema = self.info['urlSlug']
            name: str = i['movie']['title']
            id = f"ce{self.info['id']}_{i['movie']['id']}"
            if re_or(name.lower(), "enclavedanza", to_log=id):
                category = Category.DANCE
            elif re_and(name.lower(), "conciertos", ("territorios", "jazz", "duo", "trio", "charla"), to_log=id):
                category = Category.MUSIC
            root = f"https://cine.entradas.com/cine/{city}/{cinema}"
            e = Event(
                id=id,
                url=f"{root}/sesiones?showGroups={movie}",
                name=name,
                img=(i['movie'].get('thumbnailImage') or {}).get('url'),
                duration=i['movie']['duration'],
                price=self.price,
                category=category,
                place=Place(
                    name=self.info['name'],
                    address=f"{self.info['address']}"
                ),
                sessions=self.__find_sessions(root, i['shows']['data'])
            )
            events.add(e)
        return tuple(sorted(events))

    def __find_sessions(self, root: str, shows: List[Dict]):
        sessions: Set[Session] = set()
        for s in shows:
            sessions.add(Session(
                url=root+"/evento/"+str(s['urlSlug']),
                date=s['beginning'][:16].replace("T", " ")
            ))
        return tuple(sorted(sessions))


if __name__ == "__main__":
    from core.log import config_log

    config_log("log/cineentradas.log", log_level=(logging.DEBUG))
    print(CineEntradas(CineEntradas.SALA_BERLANGA, price=4.40).events)
