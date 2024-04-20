import requests
from typing import Dict, Set, List, Union
from functools import cached_property
import logging
import json
from .web import Web
from .cache import Cache, TupleCache
from .event import Event, Session, Place, Category, FieldNotFound
from .filemanager import FM

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

    def graphql(self, data: Dict):
        logger.debug("graphql operationName="+data.get("operationName"))
        r = requests.post(
            'https://entradas-next-live.kinoheld.de/graphql',
            headers={'content-type': 'application/json'},
            json=data
        )
        js = r.json()
        if not isinstance(js, dict) or js.get('errors'):
            raise CineEntradasException(js)
        if hasMorePages(js):
            raise NotImplementedError("Pagination is not supported")
        return js['data']

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
        slc = 'script[type="application/ld+json"]'
        w = Web()
        w.get(root)
        n = w.select_one(slc)
        js = json.loads(n.get_text())
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
        js = self.graphql(data)
        return js['showGroups']['data']

    @property
    @CinemaEventCache("rec/cinenetradas{cinema}.json")
    def events(self):
        events: Set[Event] = set()
        for i in self.get_sessions():
            city = i['cinema']['city']['urlSlug']
            movie = i['movie']['urlSlug']
            cinema = self.info['urlSlug']
            root = f"https://cine.entradas.com/cine/{city}/{cinema}"
            events.add(Event(
                id=f"ce{self.info['id']}_{i['movie']['id']}",
                url=f"{root}/pelicula/{movie}",
                name=i['movie']['title'],
                img=i['movie']['thumbnailImage']['url'],
                duration=i['movie']['duration'],
                price=self.price,
                category=Category.CINEMA,
                place=Place(
                    name=self.info['name'],
                    address=f"{self.info['address']}"
                ),
                sessions=self.__find_sessions(root, i['shows']['data'])
            ))
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
    from .log import config_log
    import json
    config_log("log/dore.log", log_level=(logging.DEBUG))
    print(CineEntradas(CineEntradas.SALA_BERLANGA, price=4.40).events)
