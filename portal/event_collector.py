from core.event import Event, Category, Cinema, Session
from core.zone import Zones
from portal.casaencendida import CasaEncendida
from portal.casamexico import CasaMexico
from portal.dore import Dore
from portal.madriddestino import MadridDestino
from portal.salaberlanga import SalaBerlanga
from portal.salaequis import SalaEquis
from portal.casaamerica import CasaAmerica
from portal.academiacine import AcademiaCine
from portal.caixaforum import CaixaForum
from portal.madrid_es import MadridEs
from portal.telefonica import Telefonica
from portal.teatromonumental import TeatroMonumental
from portal.mad_convoca import MadConvoca
from portal.universidad import Universidades
from portal.ateneomadrid import AteneoMadrid
from portal.circulobellasartes import CirculoBellasArtes
from portal.teatrobarrio import TeatroBarrio
from portal.alcala import Alcala
from portal.goethe import Goethe
from portal.ifrances import InstitutoFrances
from datetime import datetime, date
from core.util import round_to_even, get_domain, find_duplicates, get_main_value, re_or, isWorkingHours, get_festivos, re_and
from core.publish import PublishDB
import logging
from typing import Tuple
from core.cache import TupleCache
import re
import pytz
from collections import defaultdict
from core.wiki import WIKI
from core.filmaffinity import FilmAffinityApi
from core.dblite import DB
from functools import cache
from core.zone import Circles
from core.place import Place, Places
from portal.fundacionmarch import FundacionMarch
from concurrent.futures import ThreadPoolExecutor
from portal.reinasofia import ReinaSofia
from portal.ucm import Ucm
from core.eventbrite import Api as EventBriteApi


logger = logging.getLogger(__name__)


def get_events(source):
    if isinstance(
        source, (
            Alcala,
            MadConvoca,
            AteneoMadrid,
            Universidades,
            MadridEs,
            Goethe,
            MadridDestino,
            TeatroBarrio,
        )
    ):
        return source.events
    return source().events


def run_parallel(*sources):
    with ThreadPoolExecutor() as executor:
        results = executor.map(get_events, sources)
    arr: list[Event] = []
    for r in results:
        arr.extend(r)
    return tuple(arr)


def gNow():
    return datetime.now(tz=pytz.timezone('Europe/Madrid'))


def getMin(dt: date | datetime) -> int:
    if isinstance(dt, datetime):
        dt = dt.date()
    if dt in (
        date(2026,  3, 31),
    ):
        return 15.5
    if dt in (
        date(2026,  3, 19),
        date(2026,  4, 23),
        date(2026,  5, 21),
        date(2026,  9, 17),
        date(2026, 10, 22),
        date(2026, 11, 19),
    ):
        return 18
    weekday = dt.weekday()
    return [
        18.5,
        17,
        15.5,
        15.5,
        15.5,
        0,
        0
    ][weekday]


def isAlcalaOkDate(dt: datetime):
    wd = dt.weekday()
    min_hour = max(
        getMin(dt) + 1,
        18.50 if wd in (1, 2) else 18
    )
    return not isWorkingHours(
        dt,
        min_hour=min_hour
    )


def isOkDate(dt: datetime, delta: int = 0.5):
    if dt.date() in get_festivos(dt.year):
        return True
    min_hour = getMin(dt)
    if min_hour > 0:
        min_hour = min_hour + delta
    return not isWorkingHours(dt, min_hour=min_hour)


def isOkDateVillaverde(dt: datetime):
    if dt.date() in get_festivos(dt.year):
        return True
    if not isOkDate(dt):
        return False
    min_hour = 18
    if dt.weekday() == 4:
        min_hour = 16.5
    return not isWorkingHours(dt, min_hour=min_hour)


@cache
def isOkPlace(p: Place | tuple[float, float] | str, address: str = None):
    latlon = None
    name = None
    if isinstance(p, Place):
        name = p.name
        address = p.address
        if p.latlon:
            latlon = map(float, p.latlon.split(","))
    elif isinstance(p, str):
        name = p
    elif isinstance(p, tuple) and len(p) == 2:
        latlon = p
    if re_or(
        address,
        r"Milano$",
        r"Italy$",
        r"Hortaleza$",
        r"avenida de Betanzos",
        r"Aranjuez,? Madrid",
        r"San Lorenzo (de El|del) Escorial",
        # Vicálvaro
        r"Vic[aá]lvaro",
        flags=re.I
    ):
        return False
    if all(x is None for x in (latlon, name)):
        return True
    if name:
        if re_or(
            name,
            "San Lorenzo de Escorial",
            "Fuenlabrada",
            "Museo L[aá]zaro Galdiano",
            # Aranjuez
            "Campus( de)? Aranjuez",
            # Mostoles
            "Campus( de)? M[oó]stoles",
            "COAJ",
            "Centro cultural Maestro Alonso",
            "centro juvenil",
            "Centro cultural Lope de Vega",
            "Espacio Abierto Quinta de los Molinos",
            "Parroquia Nuestra Señora de Guadalupe",
            ("La Pedriza", "Manzanares"),
            "AV La Vecinal del Barrio Bilbao y Pueblo Nuevo",
            'Quinta de la Fuente del Berro',
            'Espacio de igualdad María Telo',
            # Collado Villaba
            'CSO La Tejedora',
            # Colón
            'Centro cultural Emilia Pardo Bazán',
            # Carabanchel
            'Espacio de igualdad María de Maeztu',
            'Espacio de igualdad Lourdes Hernández',
            # Vallecas
            'Mercado Numancia',
            '^El espacio$',
            'Centro cultural Las Californias',
            'Centro cultural Alberto Sánchez',
            'Biblioteca Miguel Delibes',
            'Biblioteca Pública Miguel Hernández',
            # Villaverde
            'Espacio de igualdad Clara Campoamor',
            # Usera
            ("centro", 'Maris Stella'),
            # Manuel Becerra
            ('Centro', 'Rafael Altamira'),
            ('Centro', 'Buenavista'),
            # Urgel
            ('Centro', 'Fernando Lázaro Carreter'),
            # Getafe
            ('Edificio Concepción Arenal', 'Getafe'),
            # El pozo
            ("palomeras bajas", "felipe( de)? diego"),
            # Pacifico
            ("Espacio de igualdad", "Elena Arnedo Soriano"),
            # Colmenar Viejo
            'Colmenar Viejo',
            # Lucero
            'CCM Lucero',
            # Laguna
            ("Asociacion Vecinal", "Fraternidad de los Carmenes"),
            # Ciudad Lineal
            "Parque (de )?Arriaga",
            flags=re.I
        ):
            logger.debug(f"Lugar descartado por name={name}")
            return False
    if latlon is None:
        return True
    lat, lon = latlon
    kms: list[float] = []
    for c in Circles:
        kms.append(c.value.get_km(lat, lon))
        if kms[-1] <= c.value.kms:
            return True
    k = round(min(kms))
    logger.debug(f"Lugar descartado {k}km {p.name} {p.url}")
    return False


def isKoEvent(e: Event):
    if e.place == Places.TEATRO_PRICE.value:
        if re_or(e.name, r'hop!?', flags=re.I):
            return True
    if e.place == Places.CAIXA_FORUM.value:
        if re_or(e.name, "Conoce CaixaForum", "Descubre el jardín vertical", flags=re.I):
            return True
    if re_or(e.place.zone, "alcal[aá]( de)? henares", flags=re.I):
        if re_and(e.name, r"cu[ée]ntame", r"experiencia", flags=re.I):
            return True
    if re_or(
        e.place.name,
        "Centro cultural Oporto",
        "Centro cultural Galileo",
        "Centro cultural Clara del Rey",
        "Centro cultural Casa de Vacas",
        "Biblioteca Mario Vargas Llosa",
        "Biblioteca La Chata",
        'Biblioteca Francisco Umbral',
        'Biblioteca Eugenio Trías',
        'Biblioteca Benito Pérez Galdós',
        'Biblioteca Ana María Matute',
        flags=re.I
    ):
        if e.price == 0 and e.category in (
            Category.THEATER,
            Category.VISIT,
            Category.LITERATURE
        ):
            return True
    if re_or(
        e.name,
        "Aprende Chotis",
        "tributo a Carmen Sevilla",
        r"Lectura en español y en ingl[eé]s",
        r"aniversario de (los )?(EE\.?UU|USA|estados unidos)",
        flags=re.I
    ):
        return True
    if e.place.zone == Zones.ALCALA_DE_HENARES.value.name:
        if e.category == Category.WORKSHOP and len(e.sessions)>1:
            return True
        if re_or(e.name, r"Repair\s*Caf[eé]", flags=re.I):
            return True
    if e.category == Category.CONFERENCE and e.place == Places.ATENEO_MADRID.value:
        if re_or(
            e.name,
            r"farmacia",
            r"perspectiva iberoamericana",
            r"Contar Madrid",
            r"psico-?an[aá]lisis",
            r"Camino de Santiago",
            r"Encuentro de Coros",
            r"arte contempor[aá]neo",
            r"homenaje",
            r"aniversario",
            "don quijote",
            flags=re.I
        ):
            return True
    return False


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


class EventCollector:
    def __init__(
        self,
        max_price: dict[Category, float],
        max_sessions: int,
        publish: PublishDB,
        categories: Tuple[Category, ...],
    ):
        self.__eventbrite = EventBriteApi()
        self.__max_price = max_price
        self.__max_max_price = max(self.__max_price.values())
        self.__max_sessions = max_sessions
        self.__categories = categories
        self.__publish = publish
        self.__madrid_destino = MadridDestino()
        self.__avoid_categories = tuple(set({
            Category.CHILDISH,
            Category.SENIORS,
            Category.ORGANIZATIONS,
            Category.NON_GENERAL_PUBLIC,
            Category.MARGINALIZED,
            Category.ONLINE,
            Category.SPAM,
            Category.PUPPETRY,
            Category.EXPO,
            Category.YOUTH,
            Category.CONTEST,
            Category.SPORT,
            Category.POETRY,
            Category.HIKING,
            Category.VIEW_POINT,
            Category.NO_EVENT,
            Category.MATERNITY,
            Category.INSTITUTIONAL_POLICY,
        }).difference(self.__categories))

    @TupleCache("rec/events.json", builder=Event.build)
    def __get_events(self,):
        logger.info("Recuperar eventos")
        store_events = run_parallel(
            self.__madrid_destino,
            TeatroMonumental,
            CirculoBellasArtes,
            ReinaSofia,
        )
        places_with_store = set(e.place for e in store_events if e.place)
        places_with_store.update((
            Places.TEATRO_MONUMENTAL.value,
        ))
        eventos = \
            store_events + \
            run_parallel(
                MadridEs(
                    isOkDate={
                        "villaverde": isOkDateVillaverde,
                        None: isOkDate
                    },
                    places_with_store=tuple(sorted(places_with_store)),
                    max_price=self.__max_max_price,
                    avoid_categories=self.__avoid_categories,
                    isOkPlace=isOkPlace,
                    districts=(
                        "arganzuela",
                        "centro",
                        "moncloa",
                        "chamber[ií]",
                        "retiro",
                        "salamanca",
                        "villaverde",
                        "carabanchel",
                    )
                ),
                AteneoMadrid(
                    isOkDate=isOkDate,
                ),
                FundacionMarch,
                Ucm,
                Universidades(
                    "https://eventos.uc3m.es/ics/location/espana/lo-1.ics",
                    "https://eventos.uam.es/ics/location/espana/lo-1.ics",
                    "https://eventos.urjc.es/ics/location/espana/lo-1.ics",
                    "https://eventos.uah.es/ics/location/espana/lo-1.ics",
                    verify_ssl=False,
                    isOkPlace=isOkPlace,
                    isOkDate=isOkDate,
                ),
                Goethe(max_price=self.__max_max_price),
                InstitutoFrances,
                AcademiaCine,
            ) + \
            run_parallel(
                Alcala(
                    isOkDate=isAlcalaOkDate
                ),
                MadConvoca(
                    isOkDate=isOkDate,
                ),
                TeatroBarrio(
                    max_price=self.__max_max_price
                ),
                CasaAmerica,
                Telefonica,
                Dore,
                CasaEncendida,
                SalaBerlanga,
                SalaEquis,
                CaixaForum,
                CasaMexico,
            )
        logger.info(f"{len(eventos)} recuperados")
        eventos = tuple(filter(self.__filter, eventos))
        eventos = self.__madrid_destino.fix_sessions(eventos)
        eventos = self.__eventbrite.fix_events(eventos)
        eventos = tuple(filter(self.__filter, eventos))
        logger.info(f"{len(eventos)} pasan 1º filtro")

        arr: list[Event | Cinema] = list()
        done: set[Event] = set()
        for e in eventos:
            e = e.fix_type()
            if e not in done:
                done.add(e)
                if self.__filter(e):
                    arr.append(e)
        logger.info(f"{len(arr)} pasan 2º filtrados")
        return tuple(arr)

    @cache
    def get_max_price(self, category: Category) -> float:
        if category in self.__max_price:
            return self.__max_price[category]
        return max(self.__max_price.values())

    def __filter(self, e: Event, to_log=True):
        if isKoEvent(e):
            return False
        if not isOkPlace(e.place.name, e.place.address):
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
        e.remove_ko_sessions(isOkDate=isOkDate, to_log=to_log)

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
            events.append(e.merge(publish=self.__publish.get(e)))

        events = sorted(
            events,
            key=lambda e: (
                min(s.date for s in e.sessions),
                len(e.sessions),
                e.duration or 0,
                e.name or '',
                e.url or ''
            )
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

        ok_events = self.__dedup_fusion(ok_events)

        return tuple(e.fix_type().fix() for e in ok_events)

    def __dedup_fusion(self, ok_events: set[Event]):
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
            _id_ = None
            name = None
            if more and get_domain(more) == "madrid.es":
                _id_ = MadridEs.get_id(more)
                name = MadridEs.get_name(more)

            e = Event.fusion(
                *evs,
                id=_id_,
                url=more,
                name=name
            )
            ok_events.add(e)

        def _mk_key_cycle(e: Event | Cinema):
            if not e.cycle:
                return None
            urls: set[str] = set()
            for s in e.sessions:
                if s.url and get_domain(s.url) != "madrid.es":
                    urls.add(s.url)
            if len(e.sessions) == 1 or len(urls) == 0:
                return (e.cycle, e.category, e.place, round_to_even(e.price))

        for evs in find_duplicates(
            ok_events,
            _mk_key_cycle
        ):
            for e in evs:
                ok_events.remove(e)
            e = Event.fusion(
                *evs,
                name=evs[0].cycle,
            )
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
            e = Event.fusion(*evs)
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
                e = Event.fusion(*evs)
                ok_events.add(e)

        return ok_events

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
        aux = map(self.__check_sessions_of_event, events)
        return tuple(filter(self.__filter, aux))

    def __check_sessions_of_event(self, e: Event | Cinema):
        sessions = list(e.sessions)
        s_doms: set[str] = set(map(get_domain, (s.url for s in e.sessions)))
        ok_doms = tuple(sorted(s_doms.difference((
            None,
            "madrid.es"
        ))))
        main_doms = ok_doms in (
            ("tienda.madrid-destino.com", ),
        )

        sessions: list[Session] = []
        for s in e.sessions:
            if main_doms and get_domain(s.url) not in ok_doms:
                continue
            sessions.append(s)
        return e.merge(sessions=tuple(sessions))

