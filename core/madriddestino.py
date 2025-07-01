from .web import Driver, WEB, get_text
from .util import re_or, plain_text, get_obj
from typing import Set, Dict
from functools import cached_property, cache
import logging
from .cache import Cache
import json
from .event import Event, Session, Place, Category, FieldNotFound, FieldUnknown, CategoryUnknown
from .cache import TupleCache
from datetime import datetime
import re
import requests
from pytz import timezone
from typing import NamedTuple
from collections import defaultdict
from core.util.madriddestino import find_more_url as find_more_url_madriddestino


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


class MadridDestino:
    URL = "https://tienda.madrid-destino.com/es"

    @Cache("rec/madriddestino/state.json")
    def __get_state(self) -> Dict:
        with Driver(browser="firefox") as f:
            f.get(MadridDestino.URL)
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
    def state(self):
        return self.__get_state()

    @Cache("rec/madriddestino/{}.json")
    def get_info(self, id) -> Dict:
        url = f"https://api-tienda.madrid-destino.com/public_api/events/{id}/info"
        logger.debug(url)
        data = S.get(url).json()['data']
        return data

    @property
    @TupleCache("rec/madriddestino.json", builder=Event.build)
    def events(self):
        events: Set[Event] = set()
        for e in self.state['events']:
            if len(e['eventCategories']) == 0:
                continue
            if e['freeCapacity'] == 0:
                continue
            org = self.__find("organizations", e['organization_id'])
            if org is None:
                continue
            logger.debug("event.id="+str(e['id']))
            info = self.get_info(e['id'])
            url = MadridDestino.URL+'/'+org['slug']+'/'+e['slug']
            id = "md"+str(e['id'])
            ev = Event(
                id=id,
                url=url,
                name=e['title'],
                img=e['featuredImage']['url'],
                price=e['highestPrice'],
                duration=info['duration'] or 60,
                category=self.__find_category(id, e, info),
                place=self.__find_place(e),
                sessions=self.__find_sessions(url, e)
            )
            if all(s.url for s in ev.sessions):
                new_url = find_more_url_madriddestino(ev.url)
                if new_url:
                    ev = ev.merge(url=new_url)
            events.add(ev)
        return tuple(sorted(events))

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
                raise FieldUnknown(MadridDestino.URL, "place", f"{e['id']}: " + ", ".join(
                    map(str, sorted(space_id))
                ))
        space = self.__find("spaces", sorted(space_id).pop())
        return Place(
            name=re.sub(r"\s+Madrid$", "", space['name']),
            address=space['address']
        )

    def __find_sessions(self, url: str, e: Dict):
        id_session = self.__get_session_from_soup(e['id'], url)
        sessions: Set[Session] = set()
        for s in e['uAvailableDates']:
            dt = timestamp_to_date(s)
            _id_ = id_session.get(dt)
            sessions.add(Session(
                date=dt,
                url=f"{url}/{_id_}" if _id_ else None
            ))
        return tuple(sorted(sessions, key=lambda s: s.date))

    @TupleCache("rec/madriddestino/{}_soup.json", builder=SoupInfo.build)
    def get_info_from_soup(self, id: int, url: str):
        info: set[SoupInfo] = set()
        soup = WEB.get_cached_soup(url)
        for script in map(get_text, soup.select("script")):
            if not script:
                continue
            for m in re.findall(r'{id:(\d+),[^{}]+,sessionStart:"([\d\-: ]+)"', script):
                info.add(SoupInfo(
                    id=int(m[0]),
                    sessionStart=m[1]
                ))
        return tuple(sorted(info))

    def __get_session_from_soup(self, id: int, url: str):
        data: dict[str, set[int]] = defaultdict(set)
        for s in self.get_info_from_soup(id, url):
            dt = s.sessionStart[:16]
            data[dt].add(s.id)
        id_data: dict[str, int] = dict()
        for dt, ids in data.items():
            if len(ids) == 1:
                id_data[dt] = ids.pop()
        return id_data

    @cache
    def __find(self, k: str, id: int):
        for i in self.state[k]:
            if isinstance(i, dict) and i.get('id') == id:
                return i
        logger.warning(str(FieldNotFound(f"{k}.id={id}", self.state[k])))

    def __find_category(self, id: str, e: Dict, info: Dict):
        audience = plain_text(info['audience'])
        if re_or(
            audience,
            "solo niñas",
            "solo niños",
            r"de [0-9][\-a\s]+([0-9]|1[0-2]) años",
            "especialmente recomendada para la infancia",
            "peques menores de",
            to_log=id
        ):
            return Category.CHILDISH
        if re_or(
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
        cats: Set[str] = set()
        for c in self.state['categories']:
            if c['id'] in e['eventCategories']:
                cats.add(c['label'])
            for ch in c.get('children', []):
                if ch['id'] in e['eventCategories']:
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

        if re_or(pt, "taller infantil", "concierto matinal familiar", to_log=id):
            return Category.CHILDISH
        if not is_cat("cine") and is_cat("en familia", "infantil"):
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
        if is_cat("cine"):
            return Category.CINEMA
        if is_cat("danza"):
            return Category.DANCE
        if is_cat("concierto"):
            return Category.MUSIC

        if re_or(pt, "visitas dialogadas", to_log=id):
            return Category.VISIT

        if re_or(pt, "^taller", to_log=id):
            return Category.WORKSHOP

        if is_cat("teatro", "teatro de objetos", "performance"):
            return Category.THEATER
        if is_cat("conferencia"):
            return Category.CONFERENCE
        if is_cat("música", "jazz", "arte sonoro"):
            return Category.MUSIC
        if is_cat("pintura", "ilustración", "fotografía", "exposición"):
            return Category.EXPO

        if re_or(pt, 'musica', to_log=id):
            return Category.MUSIC
        if re_or(pt, "visitas", to_log=id):
            return Category.VISIT
        if is_cat("audiovisual"):
            return Category.CINEMA
        if is_cat("letras"):
            return Category.CONFERENCE

        if is_cat("juvenil"):
            return Category.YOUTH
        logger.critical(str(CategoryUnknown(MadridDestino.URL, f"{e['id']} {pt}: " + ", ".join(sorted(cats)))))
        return Category.UNKNOWN


if __name__ == "__main__":
    from .log import config_log
    config_log("log/madriddestino.log", log_level=(logging.DEBUG))
    print(MadridDestino().events)
