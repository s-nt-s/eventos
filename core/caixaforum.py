from .web import Web, get_text
from .cache import TupleCache
from typing import Set, Dict
from functools import cache
import logging
from .event import Event, Place, Session, Category, FieldNotFound, FieldUnknown
import re
from bs4 import Tag
from datetime import datetime
from .util import plain_text

logger = logging.getLogger(__name__)
NOW = datetime.now()


class CaixaForum(Web):
    URLS = (
        "https://caixaforum.org/es/madrid/actividades?p=999&c=874798",
        "https://caixaforum.org/es/madrid/actividades?p=999&c=874804",
        "https://caixaforum.org/es/madrid/actividades?p=999&c=314579",
        "https://caixaforum.org/es/madrid/actividades?p=999&c=161124943"
    )

    def __init__(self, refer=None, verify=True):
        super().__init__(refer, verify)
        self.s.headers.update({'Accept-Encoding': 'identity'})

    def get(self, url, auth=None, parser="lxml", **kvargs):
        if self.url == url:
            return self.soup
        logger.debug(url)
        return super().get(url, auth, parser, **kvargs)

    def json(self, url) -> Dict:
        logger.debug(url)
        r = self.s.get(url)
        return r.json()

    @property
    @TupleCache("rec/caixaforum.json", builder=Event.build)
    def events(self):
        events: Set[Event] = set()
        for url in CaixaForum.URLS:
            self.get(url)
            divs = list(self.soup.select("div.card-item"))
            logger.debug(f"{len(divs)} div.card-item")
            for div in divs:
                events.add(self.__div_to_event(div))
        if None in events:
            events.remove(None)
        return tuple(sorted(events))

    def __div_to_event(self, div: Tag):
        a = div.select_one("h3 a")
        if a is None:
            return None
        url = a.attrs["href"]
        eid = int(url.split("_a")[-1])
        category=self.__find_category(div)
        self.get(url)
        sessions = self.__find_session(eid)
        if len(sessions) == 0:
            logger.warning(str(FieldNotFound("session", self.url)))
            return None
        nmg = div.select_one('figure img')
        img = nmg.attrs.get("data-src") or nmg.attrs.get("src")
        img = re.sub(r"(\.jpe?g)/.*$", r"\1", img)
        return Event(
            id=f"cf{eid}",
            url=url,
            name=get_text(a),
            img=img,
            price=self.__find_price(div),
            category=category,
            duration=self.__find_duration(category),
            sessions=sessions,
            place=Place(
                name="Caixa Forum",
                address="Paseo del Prado, 36, Centro, 28014 Madrid"
            )
        )

    @cache
    def __find_session(self, eid: int):
        sessions: Set[Session] = set()
        url = "https://caixaforum.org/es/web/madrid/actividades?p_p_id=headersearch_INSTANCE_HeaderSearch&p_p_lifecycle=2&p_p_resource_id=%2Fsearch%2FoneBox&_headersearch_INSTANCE_HeaderSearch_cpDefinitionId="+str(eid)
        js = self.json(url)
        ss = js.get('sessions', [])
        for s in ss:
            h, mm = map(int, s["date"].split(":"))
            d, m, y = map(int, s["time"].split("/"))
            sessions.add(Session(
                url=s['url'],
                date=f"{y}-{m:02d}-{d:02d} {h:02d}:{mm:02d}"
            ))
        return tuple(sorted(sessions))

    def __find_price(self, div: Tag):
        price = get_text(div.select_one("div.card-block-btn span"))
        if price is None:
            raise FieldNotFound("price", div)
        price = price.lower()
        if "gratuita" in price:
            return 0
        prcs = tuple(map(int, re.findall(r"(\d+)\s*€", price)))
        if len(prcs) == 0:
            n = self.select_one("#description-read")
            prcs = tuple(map(int, re.findall("(\d+)\s*€", get_text(n))))
        if len(prcs) == 0:
            raise FieldNotFound("price", div)
        return max(prcs)

    def __find_duration(self, category: Category):
        duration = []
        for p in map(get_text, self.soup.findAll("p", string=re.compile(r".*\d+\s+minutos.*"))):
            duration.extend(map(int, re.findall(r"\d+", p)))
        if len(duration) == 0:
            if category in (Category.EXPO, Category.WORKSHOP, Category.MUSIC, Category.CONFERENCE, Category.OTHERS):
                return 60
            div = self.soup.select_one("div.secondary-text")
            if div is not None and div.find("p", string=re.compile(r".*duración de una hora.*")):
                return 60
            raise FieldNotFound("duration", self.url)
        return sum(duration)

    def __find_category(self, div: Tag):
        txt = get_text(div.select_one("div.on-title"))
        if txt is None:
            raise FieldNotFound("category", div)
        cat = plain_text(txt.lower())
        if re.search(r"concierto", cat):
            return Category.MUSIC
        if re.search(r"exposicion", cat):
            return Category.EXPO
        if re.search(r"proyecciones", cat):
            return Category.CINEMA
        if re.search(r"taller", cat):
            return Category.WORKSHOP
        if re.search(r"encuentro|conferencia", cat):
            return Category.CONFERENCE
        if re.search(r"otros formatos", cat):
            return Category.OTHERS
        raise FieldUnknown("category", txt)

if __name__ == "__main__":
    from .log import config_log
    config_log("log/caisaforum.log", log_level=(logging.DEBUG))
    (CaixaForum().events)
