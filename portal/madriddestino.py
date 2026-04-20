from core.web import Driver, WEB, get_text, buildSoup
from core.util import re_or, plain_text, get_obj, re_and, get_domain
from typing import Set, Dict
from functools import cached_property, cache
import logging
from core.cache import TupleCache, HashCache
import json
from core.event import Event, Cinema, Session, Place, Category, FieldNotFound, FieldUnknown, CategoryUnknown, find_book_category
from datetime import datetime
import re
import requests
from pytz import timezone
from typing import NamedTuple
from collections import defaultdict
from core.fetcher import Getter
from aiohttp import ClientResponse
from core.md import MD

logger = logging.getLogger(__name__)
S = requests.Session()
S.headers.update({
    'Host': 'api-tienda.madrid-destino.com',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
    'Accept-Encoding': 'gzip, deflate, br',
    'X-SaleChannel': '3c4b1c81-e854-4324-830f-d59bec8cf9a2',
    'X-Locale': 'es',
    'Origin': 'https://tienda.madrid-destino.com',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Referer': 'https://tienda.madrid-destino.com/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
    'TE': 'trailers'
})


KO_MORE = (
    None,
    '',
    'imccwem.munimadrid.es'
)


async def rq_to_data(r: ClientResponse):
    js = await r.json()
    if not isinstance(js, dict):
        raise ValueError(f'{r.url} is not a dict')
    data = js.get('data')
    if not isinstance(data, dict):
        raise ValueError(f'{r.url} is not a {{"data": dict}}')
    return data


class Seat(NamedTuple):
    id: int
    free: int
    zone: str


async def rq_to_info_seats(r: ClientResponse):
    js = await r.json()
    if not isinstance(js, dict):
        raise ValueError(f'{r.url} is not a dict')
    data = js.get('data')
    metadata = js.get('metadata')
    if not isinstance(data, dict) and not isinstance(metadata, dict):
        raise ValueError(f'{r.url} is not a {{"data": dict, "metadata": dict}}')
    seats = data.get("seats")
    roomZones = metadata.get("roomZones")
    if not isinstance(seats, list) or not isinstance(roomZones, list):
        raise ValueError(f'{r.url} is not a {{"data": {{seats: [...]}}, "metadata": {{roomZones: [...]}}}}')
    dct_roomZones: dict[int, str] = {}
    for r in roomZones:
        dct_roomZones[int(r['id'])] = r['name']
    info_seats: set[Seat] = set()
    for s in seats:
        st = Seat(
            id=s['id'],
            free=s['free'],
            zone=dct_roomZones[int(s['roomZoneId'])]
        )
        info_seats.add(st)
    count: dict[str, int] = dict()
    for st in info_seats:
        if st.free > 0:
            count[st.zone] = count.get(st.zone, 0) + st.free
    return count


async def rq_to_info_soup(r: ClientResponse):
    info: set[SoupInfo] = set()
    soup = buildSoup(str(r.url), await r.text())
    for script in map(get_text, soup.select("script")):
        if not script:
            continue
        for m in re.findall(r'{id:(\d+),[^{}]+,sessionStart:"([\d\-: ]+)"', script):
            info.add(SoupInfo(
                id=int(m[0]),
                sessionStart=m[1]
            ))
    return tuple(sorted(info))


async def rq_to_mapa(r: ClientResponse):
    soup = buildSoup(str(r.url), await r.text())
    mapa_url = str(r.url).rstrip("/")+"/mapa"
    if soup.find("a", href=mapa_url):
        return mapa_url


def timestamp_to_date(timestamp: int):
    tz = timezone('Europe/Madrid')
    d = datetime.fromtimestamp(timestamp, tz)
    return d.strftime("%Y-%m-%d %H:%M")


class SoupInfo(NamedTuple):
    id: int
    sessionStart: str

    @staticmethod
    def build(*args, **kwargs):
        obj = get_obj(*args, **kwargs)
        if obj is None:
            return None
        return SoupInfo(**obj)


class Data(NamedTuple):
    state: dict
    info: dict[int, dict]
    soup: dict[int, tuple[SoupInfo, ...]]

    @staticmethod
    def build(*args, **kwargs):
        obj = get_obj(*args, **kwargs)
        if obj is None:
            return None
        for kk, cst in {
            'info': None,
            'soup': SoupInfo
        }.items():
            o = {}
            for k, v in obj.get(kk, {}).items():
                if cst:
                    if isinstance(v, dict):
                        v = cst(**v)
                    elif isinstance(v, list):
                        v = tuple(map(lambda x: cst(**x), v))
                o[int(k)] = v
            obj[kk] = o
        return Data(**obj)


HEADERS = {
    'Host': 'api-tienda.madrid-destino.com',
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:124.0) Gecko/20100101 Firefox/124.0',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'es-ES,es;q=0.8,en-US;q=0.5,en;q=0.3',
    'Accept-Encoding': 'gzip, deflate, br',
    'X-SaleChannel': '3c4b1c81-e854-4324-830f-d59bec8cf9a2',
    'X-Locale': 'es',
    'Origin': 'https://tienda.madrid-destino.com',
    'DNT': '1',
    'Connection': 'keep-alive',
    'Referer': 'https://tienda.madrid-destino.com/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-site',
    'Pragma': 'no-cache',
    'Cache-Control': 'no-cache',
    'TE': 'trailers'
}


class MadridDestino:
    URL = "https://tienda.madrid-destino.com/es"

    def __init__(self):
        self.__data_getter = Getter(
            onread=rq_to_data,
            headers=HEADERS
        )
        self.__info_session_getter = Getter(
            onread=rq_to_info_seats,
            headers=HEADERS
        )
        self.__soup_getter = Getter(
            onread=rq_to_info_soup,
            headers=HEADERS,
            skip=({}, None, tuple(), [])
        )
        self.__map_getter = Getter(
            onread=rq_to_mapa,
            headers=HEADERS,
            raise_for_status=False,
            skip=({}, None, tuple(), []),
            max_concurrency=10,
            timeout=60,
        )

    @TupleCache("rec/madriddestino/data.json", builder=Data.build)
    def __get_data(self):
        state = self.get_state_from_url(MadridDestino.URL)
        info_url: dict[str, int] = {}
        soup_url: dict[str, int] = {}
        orgs: dict[int] = {}

        for org in state['organizations']:
            if isinstance(org, dict):
                org_id = org.get('id')
                if org_id is not None:
                    orgs[org_id] = org

        for e in state['events']:
            e_id = e.get('id')
            if e_id is None:
                continue
            info_url[f"https://api-tienda.madrid-destino.com/public_api/events/{e_id}/info"] = e_id
            org = orgs.get(e['organization_id'])
            if org:
                soup_url[MadridDestino.URL+'/'+org['slug']+'/'+e['slug']] = e_id

        info: dict[int, dict] = self.__data_getter.get_from_url_id(
            info_url
        )

        soup: dict[int, dict] = self.__soup_getter.get_from_url_id(
            soup_url
        )

        return Data(
            state=state,
            info=info,
            soup=soup
        )

    def fix_sessions(self, events: tuple[Event]):
        ban_url: set[str] = set()

        def _iter_session(evs: tuple[Event]):
            for e in evs:
                for s in e.sessions:
                    if s.url in ban_url:
                        continue
                    if get_domain(s.url) == "tienda.madrid-destino.com":
                        id_session = s.url.rsplit("/", 1)[-1]
                        if id_session.isdecimal():
                            _id_ = int(id_session)
                            yield _id_, s

        session_url: dict[str, int] = {}
        for _id_, s in _iter_session(events):
            state_event = self.__get_event_info_from_session(_id_) or {}
            freeCapacity = state_event.get('freeCapacity')
            if freeCapacity == 0:
                ban_url.add(s.url)
                logger.debug(f"FULL session freeCapacity={freeCapacity} {s.url}")
                continue
            session_url[f"https://api-tienda.madrid-destino.com/public_api/sessions/{_id_}"] = _id_

        url_session: set[str] = set()
        sess_seats: dict[int, dict[str, int]] = self.__info_session_getter.get_from_url_id(
            session_url
        )
        for _id_, s in _iter_session(events):
            all_ok_seats = self.__get_all_ok_seats(sess_seats, _id_)
            if all_ok_seats is not None and len(all_ok_seats[1]) == 0:
                logger.debug(f"FULL session seats={all_ok_seats[0]} {s.url}")
                ban_url.add(s.url)
                continue
            url_session.add(s.url)

        url_to_map: dict[str, str] = self.__map_getter.get(*url_session)
        evs: set[Event] = set()
        for e in events:
            ss: list[Session] = []
            for s in e.sessions:
                if s.url in ban_url:
                    continue
                new_url = url_to_map.get(s.url)
                if new_url:
                    s = s._replace(url=new_url)
                ss.append(s)
            if tuple(ss) != e.sessions:
                e = e.merge(sessions=tuple(ss))
            evs.add(e)

        return tuple(sorted(evs))

    def __get_event_info_from_session(self, id_session: int):
        for e in self.data.state['events']:
            soup = self.data.soup.get(e['id'])
            if id_session in self.__get_session_from_soup(soup).values():
                return e

    @HashCache("rec/madriddestino/state/{}.json")
    def get_state_from_url(self, url: str) -> Dict:
        with Driver(browser="firefox") as f:
            f.get(url)
            f.wait_ready()
            js = f.execute_script(
                "return JSON.stringify(window.__NUXT__.state)")
            obj = json.loads(js)
            if not isinstance(obj, dict) or obj.get("errorApi") is True:
                raise ValueError(obj)
            for k, v in list(obj.items()):
                if v in (None, [], '', {}):
                    del obj[k]
            return obj

    @cached_property
    def data(self):
        return self.__get_data()

    #@Cache("rec/madriddestino/session/{}.json")
    #def get_info_session(self, id):
    #    url = f"https://api-tienda.madrid-destino.com/public_api/sessions/{id}"
    #    logger.debug(url)
    #    data = S.get(url).json()['data']
    #    return data

    @staticmethod
    def mk_id(id: int) -> int:
        return f"md{id}"

    @property
    @TupleCache("rec/madriddestino.json", builder=Event.build)
    def events(self):
        logger.info("Madrid Destino: Buscando eventos")
        events: Set[Event] = set()
        for e in self.data.state['events']:
            org = self.__find("organizations", e['organization_id'])
            if org is None:
                continue
            logger.debug("event.id="+str(e['id']))
            info = self.data.info.get(e['id']) or {}
            soup = self.data.soup.get(e['id'])
            url = MadridDestino.URL+'/'+org['slug']+'/'+e['slug']
            id = MadridDestino.mk_id(e['id'])
            more = info.get('webSource')
            durt = info.get('duration')
            ev = Event(
                id=id,
                url=url,
                name=e['title'],
                img=e['featuredImage']['url'],
                price=e['highestPrice'],
                duration=durt if durt is not None else 60,
                category=self.__find_category(url, id, e, info),
                place=self.__find_place(e),
                sessions=self.__find_sessions(url, e, soup),
                more=None if more in KO_MORE else more
            )
            ev = self.__complete(ev, info)
            events.add(ev)
        logger.info(f"Madrid Destino: Buscando eventos {len(events)}")
        return tuple(sorted(events))

    def __complete(self, ev: Event, info: dict):
        ori_more = ev.more or ''
        if all(s.url for s in ev.sessions) and get_domain(ev.more) in (
            None,
            'cinetecamadrid.com',
            'teatroespanol.es',
            'condeduquemadrid.es',
            '21distritos.es',
            'teatrocircoprice.es',
            'nave10matadero.es',
            'centrodanzamatadero.es',
            'mataderomadrid.org',
            'intermediae.es',
            'medialab-matadero.es',
            'centrocentro.org',
            'serreria-belga.es',
        ):
            ev = ev.merge(url=ev.more, more=None)
        ev = ev.fix_type()
        if not isinstance(ev, Cinema):
            return ev
        if ori_more and ori_more.startswith("https://www.cinetecamadrid.com/programacion/"):
            soup = WEB.get_cached_soup(ori_more)
            director: list[str] = []
            isVarios = False
            dir_txt = get_text(soup.select_one("div.field--name-field-director")) or ''
            for d in map(str.strip, re.split(r", ", dir_txt)):
                if not d or d in director:
                    continue
                if re_or(
                    d,
                    'Vari[oa].*director[eaox@]s?', 
                    'Vari[oa].*autor[eaox@]s?', 
                    flags=re.I
                ):
                    isVarios = True
                    continue
                director.append(d)
            desc = MD.convert(soup.select_one('div.wrap-desc'))
            year = get_text(soup.select_one("div.field--name-field-ano-filmacion"))
            ev = ev.merge(
                director=tuple(director),
                year=int(year) if year and year.isdecimal() else None
            )
            if not director and isVarios and len(
                [i for i in map(int, re.findall(r"(\d+)['’]", desc or '')) if i < 30]
            ) > 1:
                ev = ev.merge(cycle="Cortometrajes")
        if not ev.director:
            director: list[str] = []
            for d in map(
                str.strip,
                re.findall(
                    r"\b[Dd]irigida por( [A-Z][a-z]+(?: [A-Z][a-z]+))",
                    info.get('description'
                ) or '')
            ):
                if d and d not in director:
                    director.append(d)
            ev = ev.merge(
                director=tuple(director)
            )
        return ev

    def __find_place(self, e: Dict):
        space_id = set()
        for s in e['rooms']:
            space_id.add(s.get('space_id'))
        for s in e.get('spaces', []):
            space_id.add(s.get('id'))
        if None in space_id:
            space_id.remove(None)
        if len(space_id) == 0:
            raise FieldNotFound("place", e['id'])
        if len(space_id) > 1:
            address: Set[str] = set()
            for i in space_id:
                a = plain_text(self.__find("spaces", i)['address'])
                if a:
                    address.add(a)
            if len(address) != 1:
                logger.critical(FieldUnknown(MadridDestino.URL, "place", f"{e['id']}: " + ", ".join(
                    map(str, sorted(space_id))
                )))
                return Place(
                    name="¿?",
                    address="¿?"
                )
        space = self.__find("spaces", sorted(space_id).pop())
        return Place(
            name=re.sub(r"\s+Madrid$", "", space['name']),
            address=space['address']
        )

    def __find_sessions(self, source: str, e: Dict, soup: tuple[SoupInfo, ...]):
        id_session = self.__get_session_from_soup(soup)
        sessions: Set[Session] = set()
        for s in e['uAvailableDates']:
            dt = timestamp_to_date(s)
            _id_ = id_session.get(dt)
            url = f"{source}/{_id_}" if _id_ else None
            sessions.add(Session(
                date=dt,
                url=url
            ))
        return tuple(sorted(sessions, key=lambda s: s.date))

    def __get_all_ok_seats(self, data_seats: dict[str, dict[str, int]], id_session: int):
        if id_session not in data_seats:
            return None
        zones = set(
            k for k, v in data_seats[id_session].items() if v > 0
        )
        ok_zones = zones.difference({
            "PMR",
            "Discapacidad",
            "Acompañante PMR",
        })
        for z in list(ok_zones):
            if re_or(
                z,
                "visibilidad reducida",
                "visibilidad limitada",
                flags=re.I
            ): 
                ok_zones.remove(z)
        return tuple(sorted(zones)), tuple(sorted(ok_zones))

    def __get_session_from_soup(self, soup: tuple[SoupInfo, ...]):
        id_data: dict[str, int] = dict()
        if soup is None:
            return id_data
        data: dict[str, set[int]] = defaultdict(set)
        for s in soup:
            dt = s.sessionStart[:16]
            data[dt].add(s.id)
        for dt, ids in data.items():
            if len(ids) == 1:
                id_data[dt] = ids.pop()
        return id_data

    @cache
    def __find(self, k: str, id: int):
        for i in self.data.state[k]:
            if isinstance(i, dict) and i.get('id') == id:
                return i
        logger.warning(str(FieldNotFound(f"{k}.id={id}", self.data.state[k])))

    def __find_category(self, url: str, id: str, e: Dict, info: Dict):
        cats: Set[str] = set()
        eventCategories = e.get('eventCategories') or []
        for c in self.data.state['categories']:
            if c['id'] in eventCategories:
                cats.add(c['label'])
            for ch in c.get('children', []):
                if ch['id'] in eventCategories:
                    cats.add(ch['label'])
                    cats.add(c['label'])
        for c in list(cats):
            if " / " in c:
                cats = cats.union(c.split(" / "))
        cats = set(plain_text(c.lower()) for c in cats)

        def is_cat(*args):
            ok = cats.intersection((plain_text(a).lower() for a in args))
            if ok:
                logger.debug(f"{id} cumple {', '.join(sorted(ok))}")
                return True

        is_cine = is_cat('cine')
        psub = plain_text(e.get('subtitle'))
        pt = plain_text(e['title'])
        desc = info.get('description') or ''
        for k, v in {
            'ó': '&oacute;',
            'é': '&eacute;'
        }.items():
            desc = desc.replace(v, k)

        if re_or(
            psub,
            r"Bailas,? baby",
            flags=re.I
        ):
            return Category.CHILDISH

        audience = plain_text(info.get('audience'))
        if not is_cine and re_or(
            audience,
            r"solo niñ[oax@]s",
            "especialmente recomendada para la infancia",
            "peques menores de",
            "dirigido a peques",
            r"de [0-9][\-a\s]+([0-9]|1[0-2]) años",
            r"Desde \d+ años,? sin acompañante",
            r"familiar\.? Desde \d+ años,? (con|sin) acompañante",
            r"De \d+ meses a \d años",
            r"A partir de \d+ meses",
            r"familiar desde \d años",
            r"^Niñ[oa]s y niñ[oa]s",
            flags=re.I,
            to_log=id
        ):
            return Category.CHILDISH
        if not is_cine and re_or(
            audience,
            r"de [0-9][\-a\s]+1[0-8] años",
            r"solo si tienes entre 1[3-8] y 18 años",
            to_log=id
        ):
            return Category.YOUTH

        is_para_todos = audience is None or re_or(
            audience,
            "todos los publicos",
            "de 6 a 99 años",
            "no recomendada para menores de",
            to_log=id
        )

        if re_or(
            pt,
            "[eE]nsayos gr[aá]ficos"
        ):
            return Category.READING_CLUB
        if re_or(
            e['title'],
            "^RUTA\s*/",
            flags=re.I
        ):
            return Category.VISIT
        if re_or(
            e['title'],
            r"Presentaci[oó]n del libro",
            flags=re.I
        ):
            return find_book_category(e['title'], desc, Category.LITERATURE)
        
        if re_or(pt, "Visitas Faro de Moncloa", r"Mirador Madrid[\s\-]+As[oó]mate a Madrid", to_log=id, flags=re.I):
            return Category.VIEW_POINT
        if re_or(pt, "taller infantil", "concierto matinal familiar", "canciones de cuna", to_log=id, flags=re.I):
            return Category.CHILDISH
        if re_and(pt, "Fanzine sonoro", ("familiar", "adolescente"), to_log=id, flags=re.I):
            return Category.CHILDISH
        if not is_cine and is_cat("en familia", "infantil"):
            return Category.CHILDISH
        if re_or(pt, "sesion adolescente", to_log=id):
            return Category.YOUTH
        if not is_para_todos and is_cat("mayores"):
            return Category.SENIORS

        if is_cat("online"):
            return Category.ONLINE
        if is_cat("visitas"):
            return Category.VISIT
        if is_cat("títeres"):
            return Category.PUPPETRY
        if is_cat("circo"):
            return Category.CIRCUS
        if is_cat("taller", "curso", "formacion"):
            return Category.WORKSHOP
        if is_cine:
            return Category.CINEMA
        if is_cat("danza"):
            return Category.DANCE
        if is_cat("concierto"):
            return Category.MUSIC

        if re_or(pt, "visitas dialogadas", "guided conversations", to_log=id):
            return Category.VISIT

        if re_or(pt, "^taller", to_log=id):
            return Category.WORKSHOP

        if is_cat("teatro", "teatro de objetos", "performance"):
            return Category.THEATER
        if is_cat("conferencia"):
            return Category.CONFERENCE
        if is_cat("música", "jazz", "arte sonoro"):
            return Category.MUSIC
        if re_or(pt, 'musica', to_log=id):
            return Category.MUSIC
        if re_or(pt, "visitas", to_log=id):
            return Category.VISIT
        if is_cat("letras"):
            return Category.CONFERENCE
        if is_cat("juvenil"):
            return Category.YOUTH

        if re_or(pt, "parking", to_log=id, flags=re.I):
            return Category.NO_EVENT
        if re_or(pt, r"Charlas con altura", to_log=id, flags=re.I):
            return Category.CONFERENCE
        if re_or(psub, r"^Taller de", to_log=id, flags=re.I) or re_or(audience, "Taller", to_log=id, flags=re.I):
            return Category.WORKSHOP
        if re_or(psub, "Baychimo Teatro", flags=re.I):
            return Category.THEATER
        if re_or(psub, r"di[aá]logo con creadores foto-libros", flags=re.I):
            return Category.CONFERENCE
        if re_or(pt, "belen del ayuntamiento", flags=re.I):
            return Category.EXPO
        if re_or(
            desc,
            "para beb[eé]s y primera infancia",
            flags=re.I
        ):
            return Category.CHILDISH
        if re_or(desc, "Un taller de creatividad", flags=re.I):
            return Category.WORKSHOP
        if re_or(desc, "Los Absurdos Teatro", "teatro de sombras", "Un taller de experimentaci[oó]n", "Un taller de reflexi[oó]n", ("[eE]n esta actividad exploraremos", "con diversos materiales"), flags=re.I):
            return Category.THEATER

        if is_cat("pintura", "ilustración", "fotografía", "exposición"):
            for r in e.get('rooms', []):
                if re_or(r.get('name'), 'Sal[oó]n de actos', flags=re.I, to_log=id):
                    return Category.CONFERENCE
            return Category.EXPO

        if is_cat("audiovisual"):
            return Category.CINEMA

        logger.critical(str(CategoryUnknown(url, f"{pt} - {psub} - {audience}: " + ", ".join(sorted(cats)))))
        return Category.UNKNOWN


if __name__ == "__main__":
    from core.log import config_log
    config_log("log/madriddestino.log", log_level=logging.INFO)
    evs = MadridDestino(expand_max_price=10).events
    #print(evs)
