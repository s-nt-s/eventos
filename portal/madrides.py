from core.web import Web, WebException, WEB, Driver
from bs4 import Tag, BeautifulSoup
import re
from typing import Set, Dict, Tuple, Union, Optional
from urllib.parse import urlencode
from core.event import Event, Session, Place, Category, CategoryUnknown, isWorkingHours, FIX_EVENT
from core.util import plain_text, re_or, re_and, get_domain
from ics import Calendar
from arrow import Arrow
import logging
from core.cache import TupleCache
from urllib.parse import urlparse, parse_qs
from functools import cached_property, cache
from collections import defaultdict
from portal.util.madrides import find_more_url
from html import unescape
from tatsu.exceptions import FailedParse
from core.zone import Circles
from core.apimadrides import ApiMadridEs, MadridEsEvent
from types import MappingProxyType
from datetime import datetime
import pytz
from zoneinfo import ZoneInfo
from core.bulkrequests import BulkRequestsFileJob, BulkRequests
from core.filemanager import FM
from os.path import isfile
from typing import NamedTuple


logger = logging.getLogger(__name__)
re_sp = re.compile(r"\s+")

TZ_ZONE = 'Europe/Madrid'
NOW = datetime.now(tz=pytz.timezone(TZ_ZONE))


def get_vgnextoid(url: str | Tag):
    if isinstance(url, Tag):
        url = url.attrs.get("href")
    if url is None:
        return None
    if not isinstance(url, str):
        raise ValueError(url)
    url = url.strip()
    if len(url) == 0 or get_domain(url) != "madrid.es":
        return None
    qr = get_query(url)
    id = qr.get("vgnextoid")
    if not isinstance(id, str):
        return None
    id = id.strip()
    if len(id) == 0:
        return None
    return id


class ICSDownloader(BulkRequestsFileJob):
    def __init__(self, target: str, id: str):
        self.__target = target
        self.__id = id

    @property
    def file(self) -> str:
        return self.__target.format(self.__id)

    @property
    def url(self) -> str:
        return f"https://www.madrid.es/ContentPublisher/jsp/cont/microformatos/obtenerVCal.jsp?vgnextoid={self.__id}"

    @staticmethod
    def dwn(*vgnextoid: str):
        store = FM.resolve_path("rec/madrides/ics/")
        store.mkdir(parents=True, exist_ok=True)
        to_file = f"{store}/{{}}.ics"
        BulkRequests().run(
            *(ICSDownloader(
                to_file,
                i
            ) for i in vgnextoid if i),
            label="ics"
        )
        data: dict[str, str] = {}
        for i in vgnextoid:
            f = to_file.format(i)
            if isfile(f):
                data[i] = f
        return MappingProxyType(data)


def str_to_datetime(s: str):
    dt = datetime.strptime(s, "%Y-%m-%d %H:%M")
    dt = dt.replace(tzinfo=ZoneInfo(TZ_ZONE))
    return dt


def get_query(url: str):
    purl = urlparse(url)
    qr = parse_qs(purl.query)
    return {k: v[0] for k, v in qr.items()}


def safe_get_text(n: Tag):
    if isinstance(n, Tag):
        return get_text(n)


def get_text(n: Tag):
    if n is None:
        return None
    t = n.get_text()
    t = re_sp.sub(" ", t)
    t = re.sub(r'[“”]', '"', t)
    t = t.strip()
    if len(t):
        return t


def clean_lugar(s: str):
    if re.search(r"Centro cultural Clara del Rey", s, flags=re.I):
        return "Centro cultural Clara del Rey"
    if re.search(r".*Nave.*\bTerneras\b.*\bCasa del Reloj.*", s, flags=re.I):
        return "Nave Terneras"
    if re.search(r".*La Lonja\b.*\bCasa del Reloj.*", s, flags=re.I):
        return "La Lonja"
    if re.search(r"Casa del Reloj", s, flags=re.I):
        return "Casa del Reloj"
    s = re.sub(r"\bCentro de Información y Educación Ambiental\b", "CIEA", s, flags=re.I)
    s = re.sub(r"^Biblioteca Pública( Municipal)?", "Biblioteca", s)
    s = re.sub(r"\s+\(.*?\)\s*$", "", s)
    s = re.sub(r"^Mercado municipal de ", "Mercado ", s, flags=re.I)
    s = re.sub(
        r"^Espacio de igualdad ([^\.]+)\..*$", r"Espacio de igualdad \1",
        s,
        flags=re.I
    )
    s = re.sub(
        r"^Centro de Información y Educación Ambiental de (.*)$",
        r"Centro de información y educación ambiental de \1",
        s,
        flags=re.I
    )
    s = re.sub(r"^(Matadero) (Medialab|Madrid)$", r"\1", s, flags=re.I)
    s = re.sub(r"^(Cineteca) Madrid$", r"\1", s, flags=re.I)
    s = re.sub(r"^(Imprenta Municipal)\s.*$", r"\1", s, flags=re.I)
    s = re.sub(r"^Centro (cultural|sociocultural)\b", "Centro cultural", s, flags=re.I)
    s = re.sub(r"\s+de\s+Madrid$", "", s, flags=re.I)
    s = re.sub(r"^Centro dotacional integrado", "Centro dotacional integrado", s, flags=re.I)
    s = re.sub(r"\bFaro de la Moncloa\b", "Faro de Moncloa", s, flags=re.I)
    lw = plain_text(s).lower()
    if lw.startswith("museo de san isidro"):
        return "Museo San Isidro"
    for txt in (
        "Biblioteca Eugenio Trías",
        "Centro dotacional integrado Arganzuela",
        "Centro danza Matadero",
        "Auditorio de la Plaza de Chamberí",
    ):
        if lw.startswith(plain_text(txt)):
            return txt
    for txt in (
        "Conde Duque",
        "Matadero"
    ):
        if lw.endswith(" "+plain_text(txt)):
            return txt
    return s


def get_href(n: Tag):
    if n is None:
        return None
    if n.name == "a":
        return n.attrs.get("href")
    return get_href(n.find("a"))


def str_to_arrow_hour(h: str):
    if h is None:
        return None
    if h.isdigit() and int(h) < 25:
        h = f"{int(h):02d}:00"
    while len(h) < 5:
        h = "0"+h
    if re.match(r"^([0-1][0-9]|2[0-4]):([0-5][0-9]|60)$", h):
        return Arrow.strptime(h, "%H:%M")


@cache
def isOkPlace(p: Place):
    if re.search(r"\bcentro juvenil\b", p.name, flags=re.I):
        return False
    if p.latlon is None:
        return True
    kms: list[float] = []
    lat, lon = map(float, p.latlon.split(","))
    for c in Circles:
        kms.append(c.value.get_km(lat, lon))
        if kms[-1] <= c.value.kms:
            return True
    k = round(min(kms))
    logger.debug(f"Lugar descartado {k}km {p.name} {p.url}")
    return False


class FormSearchResult(NamedTuple):
    vgnextoid: str
    a: Tag
    div: Tag


class FormSearch:
    AGENDA = "https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/?vgnextfmt=default&vgnextchannel=ca9671ee4a9eb410VgnVCM100000171f5a0aRCRD"
    TAXONOMIA = "https://www.madrid.es/ContentPublisher/jsp/apl/includes/XMLAutocompletarTaxonomias.jsp?taxonomy=/contenido/actividades&idioma=es&onlyFirstLevel=true"

    def __init__(self):
        self.__w = Web()
        self.__w.s = Driver.to_session(
            "firefox",
            "https://www.madrid.es",
            session=self.__w.s,
        )
        self.distritos = MappingProxyType(self.__get_options("#distrito"))
        self.usuarios = MappingProxyType(self.__get_options("#usuario"))
        self.distritos = MappingProxyType(self.__get_options("#distrito"))
        self.tipos = MappingProxyType(self.__get_tipos())

    def get(self, url, *args, **kwargs) -> BeautifulSoup:
        if self.__w.url != url:
            logger.debug(url)
            self.__w.get(url, *args, **kwargs)
        title = get_text(self.__w.soup.select_one("title"))
        if title == "Access Denied":
            body = get_text(self.__w.soup.select_one("body"))
            body = re.sub(r"^Access Denied\s+", "", body or "")
            raise ValueError(f"{url} {title} {body}".strip())
        return self.__w.soup

    def __prepare_search(self):
        self.get(FormSearch.AGENDA)
        action, data = self.__w.prepare_submit("#generico1", enviar="buscar")
        if action is None:
            raise WebException(f"#generico1 NOT FOUND in {self.__w.url}")
        for k in ("gratuita", "movilidad"):
            if k in data:
                del data[k]
        data["tipo"] = "-1"
        data["distrito"] = "-1"
        data["usuario"] = "-1"
        return action, data

    @cache
    def get_vgnextoid(self, **kwargs):
        results = self.get_results(**kwargs)
        return tuple(sorted(set(r.vgnextoid for r in results)))

    @cache
    def get_results(self, **kwargs):
        action, action_data = self.__prepare_search()

        def _get(url: str):
            soup = self.get(url)
            arr = soup.select("#listSearchResults ul.events-results li div.event-info")
            a_next = soup.select_one("li.next a.pagination-text")
            logger.debug(f"{len(arr)} en {url}")
            if a_next is None:
                return None, arr
            return a_next.attrs["href"], arr

        for k, v in action_data.items():
            if k not in kwargs:
                kwargs[k] = v

        start_url = action + '?' + urlencode(kwargs)
        rt_arr: Dict[str, FormSearchResult] = {}
        url = str(start_url)
        while url:
            url, arr = _get(url)
            for div in arr:
                a = div.select_one("a.event-link")
                vgnextoid = get_vgnextoid(a)
                if vgnextoid is None:
                    continue
                rt_arr[vgnextoid] = FormSearchResult(
                    vgnextoid=vgnextoid,
                    a=a,
                    div=div
                )
        logger.debug(f"{len(rt_arr)} TOTAL en {start_url}")
        return tuple(rt_arr.values())

    @cached_property
    def zona(self):
        data: Dict[str, str] = {}
        for k, v in self.distritos.items():
            if re.search(r"arganzuela|centro|moncloa|chamberi|retiro|salamaca|villaverde|carabanchel", plain_text(v)):
                data[k] = v
        return data

    def __get_options(self, slc: str):
        data: Dict[str, str] = {}
        soup = self.get(FormSearch.AGENDA)
        for o in soup.select(slc+" option"):
            k = o.attrs["value"]
            v = re_sp.sub(" ", o.get_text()).strip()
            if k != "-1":
                data[k] = v
        return data

    def __get_tipos(self):
        data: Dict[str, str] = {}
        soup = self.get(FormSearch.TAXONOMIA, parser="xml")
        for n in soup.find_all('item'):
            value = n.find('value').string.strip()
            text = re_sp.sub(" ", n.find('text').string).strip()
            data[value] = text
        return data


class MadridEs:
    def __init__(
        self,
        remove_working_sessions: bool = False,
        places_with_store: tuple[Place, ...] = None,
        max_price: Optional[float] = None,
        avoid_categories: tuple[Category, ...] = tuple(),
    ):
        self.__remove_working_sessions = remove_working_sessions
        self.__places_with_store = places_with_store or tuple()
        self.__max_price = max_price
        self.__avoid_categories = avoid_categories
        self.__form = FormSearch()
        self.__info: MappingProxyType[str, MadridEsEvent] = MappingProxyType({
             get_vgnextoid(i.url): i for i in ApiMadridEs().get_events() if get_vgnextoid(i.url)
        })

    @cached_property
    def _free(self):
        vals = set(self.__form.get_vgnextoid(gratuita="1"))
        for k, v in self.__info.items():
            if v.price == 0:
                vals.add(k)
        ids = tuple(sorted(vals))
        logger.debug(f"{len(ids)} ids en gratuita=1")
        return vals

    @cached_property
    def _category(self):
        category: Dict[Category, Set[str]] = defaultdict(set)
        tipos = {plain_text(unescape(v)): k for k, v in self.__form.tipos.items()}
        usuarios = {plain_text(unescape(v)): k for k, v in self.__form.usuarios.items()}

        def _set_cats(key: str, data_key: Dict[str, str], data_cat: Dict[Category, Tuple[str, ...]]):
            data_val: Set[str] = set()
            for k, v in data_key.items():
                if re_or(k, *data_cat):
                    data_val.add(v)
            for cat, key_vals in data_cat.items():
                if key == 'usuario':
                    ok_aud: set[str] = set()
                    for kk, vv in self.__info.items():
                        for x in map(plain_text, vv.audience):
                            if re_or(x, *key_vals):
                                ok_aud.add(x)
                                category[cat].add(kk)
                    if len(category[cat]):
                        logger.debug(f"{len(category[cat])} ids en audience in {tuple(sorted(ok_aud))}")
                data_val: Set[str] = set()
                data_txt: Set[str] = set()
                for k, v in data_key.items():
                    if re_or(k, *key_vals):
                        data_val.add(v)
                        data_txt.add(k)
                if len(data_val) == 0:
                    logger.warning(f"No encontrado {key} que cumpla {key_vals}, disponible = {tuple(data_key.keys())}")
                    continue
                logger.debug(f"{cat} = {key} in {tuple(sorted(data_txt))}")
                for v in sorted(data_val):
                    ids = self.__form.get_vgnextoid(**{key: v})
                    logger.debug(f"{len(ids)} ids en {key}={v}")
                    category[cat] = category[cat].union(ids)

        for k, v in self.__info.items():
            if re_or(
                v.title,
                r"concierto infantil",
                r"en familia",
                r"elaboraci[óo]n de comederos de aves",
                r"los [\d\. ]+ primeros d[íi]as no se repiten",
                r"photocall hinchable",
                r"^re vuelta al patio",
                r"taller familiar",
                r"huerto familiar",
                r"Pedag[óo]gico Infantil",
                (r"dia", r"internacional", r"familias?"),
                (r"taller", r"pequeños"),
                flags=re.I
            ):
                category[Category.CHILDISH].add(k)
            if re_or(
                v.description,
                r"musical? infantil",
                r"teatro infantil",
                r"relatos en familia",
                r"concierto familiar",
                r"bienestar de niños y niñas",
                (r"cuentacuentos", r"en familia"),
                flags=re.I
            ):
                category[Category.CHILDISH].add(k)
            if re_or(v.title, "para mayores$", flags=re.I):
                category[Category.SENIORS].add(k)
            if re_or(
                v.title,
                r"d[íi]a mundial de la poes[íi]a",
                r"encuentro po[ée]tico",
                r"Recital de poes[íi]a",
                r"Versos entrevistados",
                r"Presentaci[óo]n del poemario",
                flags=re.I
            ):
                category[Category.POETRY].add(k)
            if re_or(v.title, r"Muestra de proyectos \d+", flags=re.I):
                category[Category.EXPO].add(k)
            if re_or(v.title, "Grupo de hombres por la Igualdad", flags=re.I):
                category[Category.ACTIVISM].add(k)
        _set_cats('usuario', usuarios, {
            Category.CHILDISH: (
                'familias',
                r'j[óo]venes',
                'niñas',
                'niños',
            ),
            Category.SENIORS: ('mayores', ),
            Category.MARGINALIZED: (
                'colectivos necesitados',
                'discapacidad',
                r'necesidad socioecon[oó]mica',
                'emergencia social',
                r'situaci[óo]n de dependencia',
                'sin hogar',
                r'v[íi]ctimas',
                'violencia genero',
                r'(in|e)?migrantes',
                'drogodependientes',
            ),
            Category.NON_GENERAL_PUBLIC: (
                'conductores',
                'vehiculos',
                'empresarios',
                'comerciantes',
                r'ongs?',
                'animales',
            ),
        })
        _set_cats('tipo', tipos, {
            Category.CHILDISH: (
                'escolares',
                'campamentos',
            ),
            Category.ONLINE: (
                'en linea',
                'online',
            ),
            Category.READING_CLUB: (
                r'club(es)? de lectura',
            ),
            Category.WORKSHOP: (
                r'cursos?',
                r'taller(es)?',
            ),
            Category.CONTEST: (
                r'concursos?',
                r'certamen(es)?',
            ),
            Category.THEATER: (
                r"(clasico|drama)\b.*teatro",
                r"(zarzuela).*\bm[úu]sica",
                r"teatro perfomance",
            ),
            Category.MUSIC: (
                r"(opera)\b.*teatro",
                r"flamenco\b.*danza",
                r"(rap|jazz|soul|funky|swing|reagge|flamenco|clasica|batucada|latina|española|electronica|rock|pop|folk|country).*\bm[úu]sica",
                r"^m[úu]sica$",
            ),
            Category.DANCE: (
                r"danza y baile",
                r"(cl[áa]sica|tango|breakdance|contempor[áa]ne(a|o))\b.*danza",
            ),
            Category.SPORT: (
                r'deportivas',
            ),
            Category.EXPO: (
                r"^exposici[óo]n(es)?$",
            ),
            Category.LITERATURE: (
                r'recital(es)?',
                r'presentaci[óo]n(es)?',
                r'actos? literarios?',
            ),
            Category.CINEMA: (
                r'\b(documental|cine experimental)\b',
                r"\b(ficci[óo]n)\b.*cine",
                r"cine\b.*(ficci[óo]n)\b",
                r"^cine$"
            ),
            Category.CONFERENCE: (
                r'congresos?',
                r'jornadas?',
                r'conferencias?',
                r'coloquios?s'
            )
        })

        done: Set[str] = set()
        rt_dict: Dict[Tuple[str, ...], Category] = {}
        for k, v in category.items():
            rt_dict[tuple(sorted(v))] = k
            done = done.union(v)
        return rt_dict

    def __get_description(self, url: str):
        inf = self.__info.get(get_vgnextoid(url))
        if inf and inf.description:
            return inf.description
        soup = WEB.get_cached_soup(url)
        txt = get_text(soup.select_one("div.tramites-content div.tiny-text"))
        return txt

    def __get_price(self, url_event: str):
        prc = FIX_EVENT.get(MadridEs.get_id(url_event), {}).get("price")
        if isinstance(prc, (int, float)):
            return prc
        vgnextoid = get_vgnextoid(url_event)
        if vgnextoid in self._free:
            return 0
        inf = self.__info.get(vgnextoid)
        if inf and inf.price is not None:
            return inf.price
        prc = self.__get_price_from_url(url_event)
        if prc is not None:
            return prc
        urls: list[str] = []
        soup_event = WEB.get_cached_soup(url_event)
        for a in soup_event.select("div.tramites-content a[href]"):
            href = a.attrs["href"]
            if isinstance(href, str) and href not in urls:
                urls.append(href)
        second_try: list[str] = []
        for href in urls:
            new_id = get_vgnextoid(href)
            if new_id is None:
                continue
            if new_id in self._free:
                return 0
            inf = self.__info.get(new_id)
            if inf and inf.price is not None:
                return inf.price
            second_try.append(href)
        for href in second_try:
            if href.startswith("https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/"):
                new_price = self.__get_price_from_url(href)
                if new_price is not None:
                    return new_price

    @cache
    def __get_price_from_url(self, url_event: str):
        soup_event = WEB.get_cached_soup(url_event)
        prices: set[str] = set()
        for n in soup_event.select("div.tramites-content, #importeVenta p"):
            txt = get_text(n)
            if txt is None:
                continue
            if re_or(
                txt,
                "Entrada libre hasta completar aforo",
                "Entrada gratuita",
                "Gratuito para grupos",
                "Acceso gratuito hasta completar aforo",
                flags=re.I
            ):
                return 0
            for p in re.findall(r"(\d[\d\.,]*)\s*(?:€|euros?)", txt):
                p = p.replace(",", '.').strip()
                try:
                    prices.add(float(p))
                except Exception:
                    pass
        if len(prices):
            return max(prices)
        if soup_event.select_one("ul li p.gratuita"):
            return 0

    @property
    @TupleCache("rec/madrides.json", builder=Event.build)
    def events(self) -> Tuple[Event, ...]:
        logger.info("Madrid Es: Buscando eventos")
        results: dict[str, FormSearchResult] = {}
        for data in self.iter_submit():
            for r in self.__form.get_results(**data):
                if not self.__is_ko_info(r.vgnextoid):
                    results[r.vgnextoid] = r
        all_events: Set[Event] = set()
        ics_files = ICSDownloader.dwn(*results.keys())
        for r in results.values():
            e = self.__get_event(
                r,
                ics_files[r.vgnextoid]
            )
            if e is not None:
                all_events.add(e)
        places_with_price: set[Place] = set()
        for e in all_events:
            if isinstance(e.price, (int, float)) and e.price > 0:
                places_with_price.add(e.place)
        for e in list(all_events):
            if e.price is None and e.place not in places_with_price:
                if re_or(e.place.name, "Espacio de igualdad", "Centro dotacional", "Centro cultural", "Biblioteca", flags=re.I):
                    all_events.remove(e)
                    all_events.add(e.merge(price=0))
            if e.price is None:
                logger.debug(f"Precio no encontrado en {e.url}")
                all_events.remove(e)

        return Event.fusionIfSimilar(
            all_events,
            ('name', 'place')
        )

    def __is_ko_info(self, vgnextoid: str):
        inf = self.__info.get(vgnextoid)
        if inf is None:
            return False
        dtend = str_to_datetime(inf.dtend)
        if dtend < NOW:
            return True
        dtstart = str_to_datetime(inf.dtstart)
        if dtend < dtstart:
            logger.critical(f"[{vgnextoid}] dtend < dtstart {inf.url}")
            return True
        if self.__max_price is not None and inf.price is not None and inf.price > self.__max_price:
            logger.debug(f"[{vgnextoid}] descartado por price={inf.price} > {self.__max_price} {inf.url}")
            return True
        cats = set(v for k, v in self._category.items() if vgnextoid in k)
        ko_cat = cats.intersection(self.__avoid_categories)
        if ko_cat:
            logger.debug(f"[{vgnextoid}] descartado por audience={inf.audience} categories={tuple(sorted(ko_cat))} {inf.url}")
            return True
        if inf.price and inf.price > 0 and inf.place:
            price_ko = self.__is_ko_price(inf.price, Place(
                name=clean_lugar(inf.place.location),
                address=inf.place.address,
                latlon=f"{inf.place.latitude},{inf.place.longitude}"
            ).normalize())
            if price_ko:
                logger.debug(f"[{vgnextoid}] descartado por {price_ko} {inf.url}")
                return True

    def __is_ko_price(self, price: float, place: Place):
        if price is None:
            return None
        if self.__max_price is not None and price > self.__max_price:
            return f"price={price} > {self.__max_price}"
        if price > 0 and place in self.__places_with_store:
            return f"evento de pago en lugar [{place.name}] con store"

    def __get_event(self, r: FormSearchResult, ics_file: str):
        if self.__is_ko_info(r.vgnextoid):
            return None
        place = self.__get_place(r.vgnextoid, r.div)
        if place is None or not isOkPlace(place):
            return None
        url_event = r.a.attrs["href"]
        duration, sessions = self.__get_sessions(url_event, ics_file)
        if len(sessions) == 0:
            return None
        cat = self.__find_category(r.div, url_event)
        if cat is None or cat in self.__avoid_categories:
            return None
        price = self.__get_price(url_event)
        price_ko = self.__is_ko_price(999 if price is None else price, place)
        if price_ko:
            logger.debug(f"[{r.vgnextoid}] descartado por {price_ko} {url_event}")
            return None
        ev = Event(
            id=MadridEs.get_id(url_event),
            url=url_event,
            name=get_text(r.a),
            img=None,
            price=price,
            category=cat,
            place=place,
            duration=duration,
            sessions=sessions
        )
        return ev

    def __get_place(self, vgnextoid: str, div: Tag):
        lg = div.select_one("a.event-location")
        if lg:
            return Place(
                name=clean_lugar(lg.attrs["data-name"]),
                address=lg.attrs["data-direction"],
                latlon=lg.attrs["data-latitude"]+","+lg.attrs["data-longitude"]
            ).normalize()
        inf = self.__info.get(vgnextoid)
        if inf and inf.place:
            return Place(
                name=clean_lugar(inf.place.location),
                address=inf.place.address,
                latlon=f"{inf.place.latitude},{inf.place.longitude}"
            ).normalize()

    def __get_sessions(self, url_event: str, ics_file: str) -> Tuple[Union[int, None], Tuple[Session, ...]]:
        cal = self.__get_cal(ics_file)
        if cal is None:
            return 0, tuple()

        durations: Set[int] = set()
        sessions: Set[Session] = set()
        for event in cal.events:
            if event.begin.strftime("%Y-%m-%d") != event.end.strftime("%Y-%m-%d"):
                continue
            s_date = self.__get_start(event.begin, url_event)
            s = Session(
                date=s_date
            )
            if not self.__remove_working_sessions or not isWorkingHours(s.get_date()):
                sessions.add(s)
            if event.end.strftime("%H:%M") != "23:59":
                durations.add(int((event.end - event.begin).seconds / 60))
        if len(sessions) == 0:
            return 0, tuple()
        duration = self.__get_duration(durations, url_event)
        return duration, tuple(sorted(sessions))

    def __get_duration(self, durations: Set[int], url_event: str):
        limit = (24*60)-1
        ok = set((d for d in durations if d < limit))
        duration = max(ok) if ok else limit
        if duration < limit:
            return duration
        duration = self.__get_duration_from_madrides(url_event)
        if duration is not None:
            return duration
        more_url = find_more_url(url_event)
        dom = get_domain(more_url)
        if dom == "madrid.es":
            duration = self.__get_duration_from_madrides(more_url)
            if duration is not None:
                return duration
        if dom == "centrodanzamatadero.es":
            soup = WEB.get_cached_soup(more_url)
            for txt in map(get_text, soup.select(".inner-wrapper.card .field__item")):
                if txt is None:
                    continue
                m = re.match(r"^(\d+) hora\D+(\d+) minutos*", txt)
                if m:
                    return (int(m.group(1))*60)+int(m.group(2))
                m = re.match(r"^(\d+) hora\b.*", txt)
                if m:
                    return (int(m.group(1))*60)

    def __get_duration_from_madrides(self, url: str):
        desc = self.__get_description(url)
        if not desc:
            return None
        for r in (
            r"\bDuraci[óo]n[:\s]+(\d+) min",
        ):
            m = re.search(r, desc, flags=re.I)
            if m is None:
                continue
            duration = int(m.group(1))
            logger.debug(f"FIX duration={duration} <- {url}")
            return duration
        for r in (
            r"\bcelebraci[oó]n[:\s]+de (\d+(?::\d+)?) a (\d+(?::\d+)?) h",
            r"\bhorario[:\s]+de (\d+(?::\d+)?) a (\d+(?::\d+)?) h"
        ):
            m = re.search(r, desc, flags=re.I)
            if m is None:
                continue
            h1 = str_to_arrow_hour(m.group(1))
            h2 = str_to_arrow_hour(m.group(2))
            if None in (h1, h2):
                continue
            if h1 > h2:
                h2 = h2.shift(days=1)
            duration = int((h2 - h1).seconds / 60)
            logger.debug(f"FIX duration={duration} <- {url}")
            return duration

    def __get_start(self, start: Arrow, url_event: str):
        ko_hour = ("00:00", None)
        s_date = start.strftime("%Y-%m-%d %H:%M")
        s_day, s_hour = s_date.split()
        if s_hour not in ko_hour:
            return s_date
        s_hour = self.__get_start_from_madrides(url_event)
        if s_hour not in ko_hour:
            return f"{s_day} {s_hour}"
        more_url = find_more_url(url_event)
        dom = get_domain(more_url)
        if dom == "madrid.es":
            s_hour = self.__get_start_from_madrides(more_url)
            if s_hour not in ko_hour:
                return f"{s_day} {s_hour}"
        if dom == "centrodanzamatadero.es":
            soup = WEB.get_cached_soup(more_url)
            for txt in map(get_text, soup.select(".inner-wrapper.card .field__item")):
                if txt is None:
                    continue
                m = re.match(r"^(\d\d:\d\d)(\s*h)?$", txt)
                if m:
                    return f"{s_day} {m.group(1)}"
        return s_date

    def __get_start_from_madrides(self, url: str):
        desc = self.__get_description(url)
        if not desc:
            return None
        for r in (
            r"\bcelebraci[óo]n[:\s]+de (\d+(?::\d+)?) a (\d+(?::\d+)?) h",
            r"\bhorario[:\s]+de (\d+(?::\d+)?) a (\d+(?::\d+)?) h",
            r"\bdar[áa]n? comienzo a las (\d+(?::\d+)?) h",
        ):
            m = re.search(r, desc, flags=re.I)
            if m is None:
                continue
            h = str_to_arrow_hour(m.group(1))
            if h:
                hm = h.strftime("%H:%M")
                logger.debug(f"FIX hour={hm} <- {url}")
                return hm
        return None

    @cache
    def __get_cal(self, ics_file: str):
        valid_lines: list[str] = []
        with open(ics_file, "r") as f:
            for line in map(str.rstrip, f.readlines()):
                if len(line) == 0:
                    continue
                if line.startswith(("BEGIN", "END", " ")):
                    valid_lines.append(line)
                    continue
                if ":" not in line:
                    continue
                field = re.split(r"[;:]", line)[0].strip()
                if len(field) == 0 or field != field.upper():
                    continue
                m = re.match(r"^\s*(DTSTAMP|DTSTART|DTEND)\s*:\s*(\d+T[\d:Z]+)\s*$", line)
                if m:
                    k, v = m.groups()
                    if re.match(r"^\d{8}T\d\d:\d\d:\d\dZ$", v):
                        v = v.replace(":", "")[:-1]
                        line = f"{k}:{v}"
                valid_lines.append(line)
        try:
            return Calendar("\n".join(valid_lines))
        except (NotImplementedError, FailedParse, KeyError) as e:
            logger.error(str(e)+" "+ics_file)
            return None

    def __find_category(self, div: Tag, url_event: str):
        fix_cat = FIX_EVENT.get(MadridEs.get_id(url_event), {}).get("category")
        if isinstance(fix_cat, str):
            return Category[fix_cat]
        vgnextoid = get_vgnextoid(url_event)
        if vgnextoid is None:
            raise ValueError(url_event)
        plain_type = plain_text(safe_get_text(div.select_one("p.event-type")))
        name = (get_text(div.select_one("a.event-link")) or "").lower()
        plain_name = plain_text(name)
        if re_or(plain_name, r"d[íi]a mundial de la poes[íi]a", r"encuentro po[ée]tico", r"Recital de poes[íi]a", r"Versos entrevistados", "Presentaci[óo]n del poemario", to_log=id, flags=re.I):
            return Category.POETRY
        if re_or(plain_name, r"Muestra de proyectos \d+", to_log=vgnextoid, flags=re.I):
            return Category.EXPO
        if re_or(plain_name, r"taller familiar", r"huerto familiar", to_log=vgnextoid, flags=re.I):
            return Category.CHILDISH
        if re_or(plain_name, "Grupo de hombres por la Igualdad", to_log=vgnextoid, flags=re.I):
            return Category.ACTIVISM

        note_place = div.select_one("a.event-location")
        plain_place = plain_text(note_place.attrs["data-name"]) if note_place else None
        if re_or(plain_place, "titeres", to_log=vgnextoid):
            return Category.PUPPETRY

        name_tp = re.split(r"\s*[:'\"\-]", name)[0].lower()
        tp_name = plain_text(((plain_type or "")+" "+plain_name).strip())
        maybeSPAM = any([
            re_or(plain_name, "el mundo de los toros", "el mundo del toro", "federacion taurina", "tertulia de toros", to_log=vgnextoid),
            re_and(plain_name, "actos? religios(os)?", ("santo rosario", "eucaristia", "procesion"), to_log=vgnextoid),
        ])

        for ids, cat in self._category.items():
            if vgnextoid in ids:
                if maybeSPAM and cat == Category.CONFERENCE:
                    return Category.SPAM
                logger.debug(f"{vgnextoid} en {cat}")
                return cat

        if re_and(tp_name, "taller", ("animales", "pequeños"), to_log=vgnextoid):
            return Category.CHILDISH
        if re_and(tp_name, "dia", "internacional", "familias?", to_log=vgnextoid):
            return Category.CHILDISH
        if re_or(tp_name, "concierto infantil", "en familia", r"[Ee]laboraci[óo]n de comederos de aves", r"[Ll]os [\d\. ]+ primeros d[íi]as no se repiten", "[pP]hotocall hinchable", to_log=vgnextoid):
            return Category.CHILDISH
        if re_or(plain_name, "^re vuelta al patio", to_log=vgnextoid):
            return Category.CHILDISH
        if re_or(plain_name, "para mayores$", to_log=vgnextoid):
            return Category.SENIORS
        if maybeSPAM:
            return Category.SPAM
        if re_or(plain_name, "Mejora tu ingl[eé]s con charlas", "POM Condeduque", to_log=vgnextoid, flags=re.I):
            return Category.WORKSHOP
        if re_or(plain_name, "Salida medioambiental", to_log=vgnextoid, flags=re.I):
            return Category.HIKING
        if re_or(
            plain_name,
            "recital de piano",
            r"Cuartero de C[áa]mara",
            r"Arias de [Óo]pera",
            "No cesar[áa]n mis cantos",
            to_log=vgnextoid,
            flags=re.I
        ):
            return Category.MUSIC
        if re_and(plain_name, "ballet", ("repertorio", "clasico"), to_log=vgnextoid):
            return Category.DANCE
        if re_or(plain_name, r"certamen( de)? (pintura|decoraci[oó]n|ilustraci[oó]n)", "festival by olavide", to_log=vgnextoid):
            return Category.EXPO
        if re_or(plain_name, "belen viviente", r"Representaci[óo]n(es)? teatral(es)?", to_log=vgnextoid, flags=re.I):
            return Category.THEATER
        if re_or(
            plain_name,
            r"belen (popular )?(angelicum|tradicional|monumental|napolitano)",
            r"belen (de )?navidad en",
            "belenes del mundo",
            r"apertura al publico (de el|del) belen",
            r"dioramas? de navidad",
            to_log=vgnextoid,
            flags=re.I
        ):
            return Category.EXPO
        if re_or(name_tp, r"^exposici[oó]n(es)$", to_log=vgnextoid):
            return Category.EXPO
        if re_or(
            name_tp,
            r"^conferencias?$",
            r"^pregon$",
            r'[Mm]ocrofestival, tableros y pantallas',
            to_log=vgnextoid
        ):
            return Category.CONFERENCE
        if re_or(name_tp, r"^conciertos?$", to_log=vgnextoid):
            return Category.MUSIC
        if re_or(plain_name, "cañon del rio", "ruta a caballo", "cerro de", r"actividad(es)? acuaticas? pantano", to_log=vgnextoid):
            return Category.SPORT
        if re_or(name_tp, r"^teatros?$", to_log=vgnextoid):
            return Category.THEATER
        if re_or(name_tp, r"^danzas?$", "Voguing", to_log=vgnextoid, flags=re.I):
            return Category.DANCE
        if re_or(name_tp, r"^cine$", to_log=vgnextoid):
            return Category.CINEMA
        if re_or(name_tp, r"^visitas? guiadas?$", to_log=vgnextoid):
            return Category.VISIT
        if re_or(plain_name, r"^exposicion y (charla|coloquio)", r"europa ilustra", to_log=vgnextoid):
            return Category.EXPO
        if re_or(plain_name, r"^conferencia y (charla|coloquio)", to_log=vgnextoid):
            return Category.CONFERENCE
        if re_or(
            plain_name,
            r"^taller",
            "tertulias en latin",
            r"taller(es)? de calidad del aire",
            "compostagram",
            "esquejodromo",
            to_log=vgnextoid
        ):
            return Category.WORKSHOP
        if re_or(plain_name, "visitas guiadas para", "Recorrido por la Iluminaci[óo]n", to_log=vgnextoid, flags=re.I):
            return Category.VISIT
        if re_or(plain_name, "^concierto de", to_log=vgnextoid):
            return Category.MUSIC
        if re_or(tp_name, ("espectaculo", "magia"), r"\b[Ll]a magia de", to_log=vgnextoid):
            return Category.MAGIC
        if re_or(tp_name, "cine", "proyeccion(es)?", "cortometrajes?", to_log=vgnextoid):
            return Category.CINEMA
        if re_or(tp_name, "musica", "musicales", "conciertos?", "hip-hob", "jazz", "reagge", "flamenco", "batucada", "rock", to_log=vgnextoid):
            return Category.MUSIC
        if re_or(tp_name, "teatro", "zarzuela", "lectura dramatizada", to_log=vgnextoid):
            return Category.THEATER
        if re_or(tp_name, "exposicion(es)?", "noche de los museos", to_log=vgnextoid):
            return Category.EXPO
        if re_or(plain_type, "danza", "baile", to_log=vgnextoid):
            return Category.DANCE
        if re_or(tp_name, "conferencias?", "coloquios?", "presentacion(es)?", to_log=vgnextoid):
            return Category.CONFERENCE
        if re_or(tp_name, "charlemos sobre", to_log=vgnextoid):
            return Category.CONFERENCE
        if re_or(tp_name, "club(es)? de lectura", to_log=vgnextoid):
            return Category.READING_CLUB
        if re_or(tp_name, ("elaboracion", "artesanal"), to_log=vgnextoid):
            return Category.WORKSHOP
        if re_or(plain_type, "cursos?", "taler(es)?", "capacitacion", to_log=vgnextoid):
            return Category.WORKSHOP
        if re_or(plain_type, "concursos?", "certamen(es)?", to_log=vgnextoid):
            return Category.CONTEST
        if re_or(plain_type, "actividades deportivas", to_log=vgnextoid):
            return Category.SPORT
        if re_or(
            plain_name,
            "^senderismo",
            r"^senda",
            "senda botanica",
            "excursion medioambiental",
            r"^del? .* a casa de campo$",
            "^salida multiaventura",
            r"(paseo|itinerario) ornitologico",
            r"^entreparques",
            ("deportes?", "torneo"),
            to_log=vgnextoid
        ):
            return Category.SPORT
        if re_or(plain_place, "educacion ambiental") and re_or(plain_name, "^arroyo", to_log=vgnextoid):
            return Category.SPORT
        if re_or(plain_place, "imprenta") and re_or(tp_name, "demostracion(es)?", "museos?", to_log=vgnextoid):
            return Category.EXPO
        if re_or(plain_name, "^(danza|chotis)", to_log=vgnextoid):
            return Category.DANCE
        if re_or(plain_name, "^(charlas?|ensayos?)", to_log=vgnextoid):
            return Category.CONFERENCE
        if re_or(plain_name, "^(acompañamiento digital)", to_log=vgnextoid):
            return Category.WORKSHOP
        if re_or(plain_name, "^(webinario)", to_log=vgnextoid):
            return Category.ONLINE
        if re_or(plain_name, "^(paseo|esculturas)", "de el retiro$", to_log=vgnextoid):
            return Category.VISIT
        if re_or(plain_name, "^mercadea en el mercado", "^mercadea en los mercadillos", to_log=vgnextoid):
            return Category.CONFERENCE
        if re_or(plain_name, "poemario", "^poesia rapidita", r"^\d+ poemas", "poesia o barbarie", to_log=vgnextoid):
            return Category.POETRY
        if re_or(plain_name, "^hacer actuar", to_log=vgnextoid):
            return Category.WORKSHOP
        if re_or(plain_name, "^concentracion", "Grupo de hombres por la Igualdad", to_log=vgnextoid, flags=re.I):
            return Category.ACTIVISM
        if re_or(plain_type, r"visitas?", to_log=vgnextoid):
            return Category.VISIT
        if re_or(plain_name, r"visita a", to_log=vgnextoid):
            return Category.VISIT
        if re_or(plain_type, "jornadas?", "congresos?", to_log=vgnextoid):
            return Category.CONFERENCE
        if re_or(plain_name, "actuacion coral", "recital coral", "taller de sevillanas", to_log=vgnextoid):
            return Category.MUSIC
        if re_or(plain_name, "encuentro artistico", to_log=vgnextoid):
            return Category.EXPO
        if re_or(plain_name, "^(cantando|banda municipal)", to_log=vgnextoid):
            return Category.MUSIC
        if re_and(plain_name, "dialogos?", "mac", to_log=vgnextoid):
            return Category.CONFERENCE
        if re_or(plain_name, "lengua de signos", r"^[Tt]alleres", to_log=vgnextoid):
            return Category.WORKSHOP
        if re_or(plain_name, "^El mago", flags=re.I, to_log=vgnextoid):
            return Category.MAGIC
        if re_and(plain_name, "fiesta", "aniversario", flags=re.I, to_log=vgnextoid):
            return Category.PARTY

        desc = self.__get_description(url_event)
        if re_or(desc, "[mM]usical? infantil", r"[Tt]eatro infantil", "relatos en familia", "concierto familiar", "bienestar de niños y niñas", ("cuentacuentos", "en familia"), to_log=vgnextoid, flags=re.I):
            return Category.CHILDISH
        if re_or(desc, "zarzuela", "teatro", "espect[áa]culo (circense y )?teatral", to_log=vgnextoid, flags=re.I):
            return Category.THEATER
        if re_or(desc, "itinerario .* kil[ó]metros", to_log=vgnextoid, flags=re.I):
            return Category.SPORT
        if re_or(plain_name, "actuacion", "verbena") and re_or(desc, "música", "concierto", "canciones", "pop", "rock", "baila", "bailable", "cantante", " d[ée]cada prodigiosa", to_log=vgnextoid, flags=re.I):
            return Category.MUSIC
        if re_or(desc, "Concierto", r"\b[Uu]n concierto de", r"\b[Gg][oó]spel", to_log=vgnextoid):
            return Category.MUSIC
        if re_or(desc, r"intervienen l[oa]s", "una mesa redonda con", " encuentro del ciclo Escritores", to_log=vgnextoid, flags=re.I):
            return Category.CONFERENCE
        if (desc or '').count("poesía") > 2 or re_or(
            desc,
            "presentación del poemario",
            r"recital de poes[íi]a",
            "presenta su poemario",
            r"presentan? este poemario de",
            r"poemas in[eé]ditos",
            flags=re.I
        ):
            return Category.POETRY
        if re_or(
            desc,
            "propuesta creativa y participativa que combina lectura, escritura y expresión",
            r"Se organizará un '?escape room'?",
            "taller creativo",
            "pensado para ejercitar la memoria",
            "m[óo]dulo pr[aá]ctico",
            to_log=vgnextoid,
            flags=re.I
        ):
            return Category.WORKSHOP
        if re_and(desc, r"presentaci[oó]n", (r"libros?", r"novelas?"), (r"autore(es)?", r"autoras?"), to_log=vgnextoid):
            return Category.CONFERENCE
        if re_and(desc, "ilusionista", "mentalismo"):
            return Category.MAGIC

        if re_and(plain_place, "ambiental", ("casa de campo", "retiro"), to_log=vgnextoid):
            return Category.VISIT

        logger.critical(str(CategoryUnknown(url_event, f"{vgnextoid}: type={plain_type}, name={plain_name}")))
        return Category.UNKNOWN

    @staticmethod
    def get_id(lk: str):
        vgnextoid = get_vgnextoid(lk)
        if vgnextoid is None:
            return None
        return "ms"+vgnextoid

    def iter_submit(self):
        for dis in self.zona.keys():
            data = {}
            data["distrito"] = dis
            yield data

    @cached_property
    def zona(self):
        data: Dict[str, str] = {}
        for k, v in self.__form.distritos.items():
            if re.search(r"arganzuela|centro|moncloa|chamberi|retiro|salamaca|villaverde|carabanchel", plain_text(v)):
                data[k] = v
        return data


if __name__ == "__main__":
    from core.log import config_log
    config_log("log/madrides.log", log_level=(logging.INFO))
    print(MadridEs(
        remove_working_sessions=True
    ).events)
    #m.get_events()
