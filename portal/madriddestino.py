from core.web import Driver, WEB, get_text, buildSoup
from core.util import re_or, plain_text, get_obj, re_and, get_domain
from typing import Set, Dict
from functools import cached_property, cache
import logging
from core.cache import TupleCache, HashCache
import json
from core.event import Event, Cinema, Session, Place, Category, FieldNotFound, FieldUnknown, CategoryUnknown
from datetime import datetime
import re
import requests
from pytz import timezone
from typing import NamedTuple
from collections import defaultdict
from core.fetcher import Getter
from aiohttp import ClientResponse

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
        info = {}
        for k, v in obj.get('info', {}).items():
            info[int(k)] = v
        obj['info'] = info
        soup = {}
        for k, v in obj.get('soup', {}).items():
            soup[int(k)] = tuple(map(lambda x: SoupInfo(*x), v))
        obj['soup'] = soup
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
        self.__full_session: set[str] = set()
        self.__info_getter = Getter(
            onread=rq_to_data,
            headers=HEADERS
        )
        self.__soup_getter = Getter(
            onread=rq_to_info_soup,
            headers=HEADERS
        )
        self.__map_getter = Getter(
            onread=rq_to_mapa,
            headers=HEADERS
        )

    @property
    def full_sessions(self):
        return tuple(sorted(self.__full_session))

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
            if e_id is not None:
                info_url[f"https://api-tienda.madrid-destino.com/public_api/events/{e_id}/info"] = e_id

            org = orgs.get(e['organization_id'])
            if org:
                soup_url[MadridDestino.URL+'/'+org['slug']+'/'+e['slug']] = e_id

        info: dict[int, dict] = {}
        for k, v in self.__info_getter.get(*info_url.keys()).items():
            info[info_url[k]] = v

        soup: dict[int, dict] = {}
        for k, v in self.__soup_getter.get(*soup_url.keys()).items():
            soup[soup_url[k]] = v

        return Data(
            state=state,
            info=info,
            soup=soup
        )

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
            #if len(e['eventCategories']) == 0:
            #    continue
            #if e['freeCapacity'] == 0:
            #    continue
            org = self.__find("organizations", e['organization_id'])
            if org is None:
                continue
            logger.debug("event.id="+str(e['id']))
            info = self.data.info[e['id']]
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
                category=self.__find_category(id, e, info),
                place=self.__find_place(e),
                sessions=self.__find_sessions(url, e, soup),
                more=None if more in KO_MORE else more
            )
            ev = self.__complete(ev, info)
            events.add(ev)
        url_session: set[str] = set()
        for e in events:
            for s in e.sessions:
                url_session.add(s.url)
        url_to_map = self.__map_getter.get(*url_session)
        evs: list[Event] = []
        for e in sorted(events):
            sessions = list(e.sessions)
            for i, s in enumerate(sessions):
                mp = url_to_map.get(s.url)
                if mp is not None:
                    if s.url in self.__full_session:
                        self.__full_session.add(mp)
                    sessions[i] = s._replace(url=mp)
            e = e.merge(sessions=tuple(sessions))
            evs.append(e)
        logger.info(f"Madrid Destino: Buscando eventos {len(evs)}")
        return tuple(evs)

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
            'centrocentro.org'
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
                if d in ('Varios/as directores/as', 'Varios/as autores/as', 'Varias autoras'):
                    isVarios = True
                    continue
                director.append(d)
            desc = get_text(soup.select_one('div.wrap-desc'))
            year = get_text(soup.select_one("div.field--name-field-ano-filmacion"))
            ev = ev.merge(
                director=tuple(director),
                year=int(year) if year and year.isdecimal() else None
            )
            if not director and isVarios and len(re.findall(r"\d+'", desc)) > 2:
                ev = ev.merge(cycle="Cortometrajes")
        if not ev.director:
            director: list[str] = []
            for d in map(str.strip, re.findall(r"\b[Dd]irigida por( [A-Z][a-z]+(?: [A-Z][a-z]+))", info['description'])):
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
            #if _id_:
            #    self.get_info_session(_id_)
            if url and e['freeCapacity'] == 0:
                self.__full_session.add(url)
            sessions.add(Session(
                date=dt,
                url=url
            ))
        return tuple(sorted(sessions, key=lambda s: s.date))

    def __get_session_from_soup(self, soup: tuple[SoupInfo, ...]):
        data: dict[str, set[int]] = defaultdict(set)
        for s in soup:
            dt = s.sessionStart[:16]
            data[dt].add(s.id)
        id_data: dict[str, int] = dict()
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

    def __find_category(self, id: str, e: Dict, info: Dict):
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

        audience = plain_text(info.get('audience'))
        if not is_cine and re_or(
            audience,
            "solo niñas",
            "solo niños",
            r"de [0-9][\-a\s]+([0-9]|1[0-2]) años",
            "especialmente recomendada para la infancia",
            "peques menores de",
            "dirigido a peques",
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

        pt = plain_text(e['title'])
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
        if is_cat("taller", "curso"):
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
        psub = plain_text(e.get('subtitle'))
        if re_or(psub, r"^Taller de", to_log=id, flags=re.I) or re_or(audience, "Taller", to_log=id, flags=re.I):
            return Category.WORKSHOP
        if re_or(psub, "Baychimo Teatro", flags=re.I):
            return Category.THEATER
        if re_or(pt, "belen del ayuntamiento", flags=re.I):
            return Category.EXPO
        desc = info.get('description') or ''
        for k, v in {
            'ó': '&oacute;'
        }.items():
            desc = desc.replace(v, k)
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

        logger.critical(str(CategoryUnknown(MadridDestino.URL, f"{e['id']} {pt}: " + ", ".join(sorted(cats)))))
        return Category.UNKNOWN


if __name__ == "__main__":
    from core.log import config_log
    config_log("log/madriddestino.log", log_level=logging.INFO)
    evs = MadridDestino().events
    #print(evs)
