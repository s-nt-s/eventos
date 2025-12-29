from core.event import Event, Category, Cinema, Session
from portal.casaencendida import CasaEncendida
from portal.dore import Dore
from portal.madriddestino import MadridDestino
from portal.salaberlanga import SalaBerlanga
from portal.salaequis import SalaEquis
from portal.casaamerica import CasaAmerica
from portal.academiacine import AcademiaCine
from portal.caixaforum import CaixaForum
from portal.madrides import MadridEs
from portal.telefonica import Telefonica
from portal.teatromonumental import TeatroMonumental
from portal.mad_convoca import MadConvoca
from datetime import datetime
from core.util import get_domain, to_uuid, find_duplicates, get_main_value
import logging
from typing import Tuple
from core.cache import TupleCache
import re
import pytz
from collections import defaultdict
from core.wiki import WIKI
from core.filmaffinity import FilmAffinityApi
from core.dblite import DB
from core.web import WEB
from functools import cache

logger = logging.getLogger(__name__)


def gNow():
    return datetime.now(tz=pytz.timezone('Europe/Madrid'))


def find_filmaffinity_if_needed(imdb_film: dict[str, int], e: Cinema):
    if not isinstance(e, Cinema):
        return None
    if isinstance(e.filmaffinity, int):
        return None
    _id_ = imdb_film.get(e.imdb)
    if isinstance(_id_, int):
        return _id_
    if isinstance(e.cycle, str):
        return None
    if isinstance(e.imdb, str):
        db_year = e.year or DB.one("select year from MOVIE where id = ?", e.imdb)
        db_title = DB.to_tuple("select title from TITLE where movie = ?", e.imdb)
        _id_ = FilmAffinityApi.search(
            db_year,
            *db_title
        )
        if isinstance(_id_, int):
            return _id_
    _id_ = FilmAffinityApi.search(e.year, *e.get_full_aka())
    if isinstance(_id_, int):
        return _id_


def round_to_even(x):
    up = int((x + 2) // 2) * 2
    down = int(x // 2) * 2
    if x == int(x):
        return down
    if abs(x - down) < abs(x - up):
        return down
    return up


class EventCollector:
    def __init__(
        self,
        max_price: dict[Category, float],
        max_sessions: int,
        avoid_working_sessions: bool,
        publish: dict[str, str],
        ko_places: Tuple[str],
        categories: Tuple[Category, ...],
    ):
        self.__max_price = max_price
        self.__max_sessions = max_sessions
        self.__categories = categories
        self.__ko_places = ko_places
        self.__avoid_working_sessions = avoid_working_sessions
        self.__publish = publish
        self.__madrid_destino = MadridDestino()

    @TupleCache("rec/events.json", builder=Event.build)
    def __get_events(self,):
        logger.info("Recuperar eventos")
        md_events = self.__madrid_destino.events
        md_places = tuple(sorted(set(e.place for e in md_events if e.place)))
        eventos = \
            MadConvoca().events + \
            MadridEs(
                remove_working_sessions=self.__avoid_working_sessions,
                places_with_store=md_places,
                max_price=max(self.__max_price.values())
            ).events + \
            Dore().events + \
            md_events + \
            CasaEncendida().events + \
            SalaBerlanga().events + \
            SalaEquis().events + \
            CasaAmerica().events + \
            AcademiaCine().events + \
            CaixaForum().events + \
            Telefonica().events + \
            TeatroMonumental().events
        logger.info(f"{len(eventos)} recuperados")
        eventos = tuple(filter(self.__filter, eventos))
        logger.info(f"{len(eventos)} pasan 1º filtro")

        arr: list[Event | Cinema] = list()
        done: set[Event] = set()
        for e in eventos:
            e = e.fix_type()
            e = e.fix(publish=self.__publish.get(e.id, e.publish))
            if e not in done:
                done.add(e)
                if self.__filter(e):
                    self.__publish[e.id] = e.publish
                    arr.append(e)
        logger.info(f"{len(arr)} pasan 2º filtrados")
        return tuple(arr)

    @cache
    def get_max_price(self, category: Category) -> float:
        if category in self.__max_price:
            return self.__max_price[category]
        return max(self.__max_price.values())

    def __filter(self, e: Event, to_log=True):
        if e.place.name in self.__ko_places:
            if to_log:
                logger.debug(f"Descartada por place={e.place.name} {e.url}")
            return False
        max_price = self.get_max_price(e.category)
        if e.price > max_price:
            if to_log:
                logger.debug(f"Descartada por price={e.price} {e.url or e.id}")
            return False
        if e.category not in self.__categories:
            if to_log:
                logger.debug(f"Descartada por category={e.category.name} {e.url or e.id}")
            return False

        #if "madrid.es" in map(get_domain, e.iter_urls()):
        #    if e.place.name in (
        #        "Faro de Moncloa"
        #    ):
        #        # Ya registrado en madrid-destino
        #        return False

        e.remove_old_sessions(gNow())
        e.remove_working_sessions(to_log=to_log)

        count_session = len(e.sessions)
        if count_session == 0:
            if to_log:
                logger.debug(f"Descartada por 0 sesiones {e.url or e.id}")
            return False
        if count_session > self.__max_sessions:
            if to_log:
                logger.warning(f"Tiene {count_session} sesiones {e.url or e.id}")
            return False
        return True

    def get_events(self):
        aux = self.__get_events()
        aux = self.__dedup(aux)
        aux = self.__check_sessions(aux)
        aux = self.__complete_filmaffinity(aux)
        aux = self.__complete_url(aux)

        events: list[Event | Cinema] = []
        for e in filter(self.__filter, aux):
            events.append(e.merge(publish=self.__publish.get(e.id, e.publish)))
            if e.id not in self.__publish and e.publish:
                self.__publish[e.id] = e.publish
        events = sorted(
            events,
            key=lambda e: (min(s.date for s in e.sessions), e.name, e.url or '')
        )
        return tuple(events)

    def __dedup(self, events: Tuple[Event, ...]):
        url_cat: dict[str, set[Category]] = defaultdict(set)
        mad_more_cat: dict[str, set[Category]] = defaultdict(set)
        ok_events = set(events)
        for e in ok_events:
            if e.category not in (None, Category.UNKNOWN) and e.url and get_domain(e.url) != "madrid.es":
                url_cat[e.url].add(e.category)
        for e in list(ok_events):
            if "madrid.es" in (get_domain(e.url), get_domain(e.more)):
                cat = get_main_value(url_cat.get(e.more, set()).union(url_cat.get(e.url, set())))
                if cat not in (None, Category.UNKNOWN, e.category):
                    logger.debug(f"[{e.id}] FIX: category={cat} <- {e.category}")
                    ok_events.remove(e)
                    ok_events.add(e.merge(category=cat).fix_type())
                elif e.category:
                    if e.more:
                        mad_more_cat[e.more].add(e.category)
                    if e.url:
                        mad_more_cat[e.url].add(e.category)
        ids = set(e.id for e in ok_events)
        for e in set(self.__madrid_destino.events):
            if not self.__filter(e, to_log=False) and e.id not in ids:
                cat = get_main_value(mad_more_cat.get(e.url))
                if cat not in (None, e.category):
                    logger.debug(f"[{e.id}] FIX: category={cat} <- {e.category}")
                    e = e.merge(category=cat).fix_type().fix()
                    if self.__filter(e, to_log=False):
                        ok_events.add(e)

        def _mk_key_madrid_music(e: Event):
            if e.category != Category.MUSIC:
                return None
            doms = set(map(get_domain, (e.url, e.more)))
            doms.discard(None)
            if len(doms) != 1 or doms.pop() != "madrid.es":
                return None

            return (e.more or e.url, e.place, e.price)

        for evs in find_duplicates(
            ok_events,
            _mk_key_madrid_music
        ):
            for e in evs:
                ok_events.remove(e)
            more = evs[0].more
            _id_ = MadridEs.get_id(more)
            e = Event.fusion(*evs, firstEventUrl=False).merge(
                name=None,
                id=_id_,
                url=more,
            ).fix()
            ok_events.add(e)

        def _mk_key_cycle(e: Event | Cinema):
            if not e.cycle:
                return None
            urls: set[str] = set()
            for s in e.sessions:
                if s.url:
                    urls.add(s.url)
            if len(e.sessions) == 1 or len(urls) == 0:
                return (e.cycle, e.category, e.place, round_to_even(e.price))

        for evs in find_duplicates(
            ok_events,
            _mk_key_cycle
        ):
            for e in evs:
                ok_events.remove(e)
            cycle = evs[0].cycle
            _id_ = to_uuid("".join(e.id for e in evs))
            e = Event.fusion(*evs, firstEventUrl=True).merge(
                name=cycle,
                cycle=cycle,
                id=_id_,
            ).fix()
            st_more = set(x.more for x in evs if x.more)
            st_url = set(x.url for x in evs if x.url)
            if all(s.url for s in e.sessions):
                e = e.merge(url=None, more=None)
            if len(st_url) == 1 and e.url is None:
                e = e.merge(url=st_url.pop())
            if len(st_more) == 1 and e.url is None:
                e = e.merge(url=st_more.pop())
            if len(st_more) == 1 and e.more is None:
                e = e.merge(more=st_more.pop())
            ok_events.add(e)

        def _mk_place_name(e: Event | Cinema):
            name = re.sub(r"[:'',\.«»]", "", e.name).lower()
            k = (e.place, e.category, name, e.price) #, tuple((s.date for s in e.sessions)))
            return k

        for evs in find_duplicates(
            ok_events,
            _mk_place_name
        ):
            for e in evs:
                ok_events.remove(e)
            _id_ = to_uuid("".join(e.id for e in evs))
            e = Event.fusion(*evs).merge(
                id=_id_,
            ).fix()
            ok_events.add(e)

        for re_url in (
            re.compile(r"^https://www\.condeduquemadrid\.es/actividades/\S+$"),
            re.compile(r"^https://www\.teatroespanol.es/\S+$"),
            re.compile(r"^https://21distritos\.es/evento/\S+$"),
            re.compile(r"^https://tienda\.madrid-destino\.com/es/\S+$"),
            re.compile(r"^https://www\.teatrocircoprice\.es/programacion/\S+$"),
            re.compile(r"^https://www\.centrocentro\.org/\S+$"),
        ):
            def _mk_url(e: Event | Cinema):
                for u in e.iter_urls():
                    if re_url.match(u):
                        return (u, e.place, e.price)

            for evs in find_duplicates(
                ok_events,
                _mk_url
            ):
                for e in evs:
                    ok_events.remove(e)
                _id_ = to_uuid("".join(e.id for e in evs))
                e = Event.fusion(*evs).merge(
                    id=_id_,
                ).fix()
                ok_events.add(e)

        return tuple(e.fix_type().fix() for e in ok_events)

    def __complete_filmaffinity(self, events: Tuple[Event | Cinema, ...]):
        arr1 = list(events)
        imdb: set[str] = set()
        for e in arr1:
            if isinstance(e, Cinema) and e.imdb and e.filmaffinity is None:
                imdb.add(e.imdb)
        imdb_film = WIKI.get_filmaffinity(*imdb)
        for i, e in enumerate(arr1):
            filmaffinity = find_filmaffinity_if_needed(imdb_film, e)
            if filmaffinity:
                logger.debug(f"FIND FilmAffinity: {filmaffinity}")
                arr1[i] = e.merge(filmaffinity=filmaffinity).fix()

        return tuple(arr1)

    def __complete_url(self, events: Tuple[Event | Cinema, ...]):
        arr1 = list(events)
        for i, e in enumerate(arr1):
            while e.also_in and None in (e.url, e.more):
                new_also = e.also_in[1:]
                if e.url is None:
                    e = e.merge(url=e.also_in[0], also_in=new_also)
                elif e.more is None:
                    e = e.merge(more=e.also_in[0], also_in=new_also)
            arr1[i] = e
        return tuple(arr1)

    def __check_sessions(self, events: Tuple[Event | Cinema, ...]):
        aux = map(self.__check_sessionse_of_event, events)
        return tuple(filter(self.__filter, aux))

    def __check_sessionse_of_event(self, e: Event | Cinema):
        sessions = list(e.sessions)
        s_doms: set[str] = set(map(get_domain, (s.url for s in e.sessions)))
        ok_doms = tuple(sorted(s_doms.difference((
            None,
            "madrid.es"
        ))))
        need_check_domain = ok_doms in (
            ("tienda.madrid-destino.com", ),
        )

        sessions: list[Session] = []
        for s in e.sessions:
            if need_check_domain and get_domain(s.url) not in ok_doms:
                continue
            if s.url in self.__madrid_destino.full_sessions:
                continue
            if s.url and re.match(r"https://tienda\.madrid-destino\.com/.*/\d+/?$", s.url):
                soup = WEB.get_cached_soup(s.url)
                mapa_url = s.url.rstrip("/")+"/mapa"
                if soup.find("a", href=mapa_url):
                    s = s._replace(url=mapa_url)
            sessions.append(s)
        return e.merge(sessions=tuple(sessions))

    @property
    def publish(self):
        return self.__publish
