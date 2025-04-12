from .web import Web, get_text
from .cache import TupleCache
from typing import Set, Dict, List
from functools import cached_property, cache
import logging
from .event import Event, Place, Session, Category, FieldNotFound, CategoryUnknown
import re
from bs4 import Tag
from datetime import datetime
from .util import plain_text, re_or
import json


logger = logging.getLogger(__name__)
NOW = datetime.now()


class CasaAmerica(Web):
    URL = "https://www.casamerica.es/agenda"

    def __init__(self, refer=None, verify=True):
        super().__init__(refer, verify)
        self.s.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
            ),
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1"
        })

    def get(self, url, auth=None, parser="lxml", **kvargs):
        if url == self.url:
            return self.soup
        logger.debug(url)
        return super().get(url, auth, parser, **kvargs)

    @cached_property
    def calendar(self):
        urls: List[str] = []
        url = CasaAmerica.URL + "/" + NOW.strftime("%Y%m")
        while url:
            self.get(url)
            if self.soup.select_one("div.view-grouping h2.dia") is None:
                return tuple(urls)
            if url not in urls:
                urls.append(url)
            url = None
            cal = self.soup.select_one('nav.paginador-agenda a.page-link[rel="next"]')
            if cal is not None:
                url = cal.attrs["href"]
        return tuple(urls)

    @property
    @TupleCache("rec/casaamerica.json", builder=Event.build)
    def events(self):
        events: Set[Event] = set()
        for url in self.calendar:
            events = events.union(self.__url_to_events(url))
        return self.__clean_events(events)

    def __clean_events(self, all_events: Set[Event]):
        if None in all_events:
            all_events.remove(None)
        data: Dict[str, Set[Event]] = {}
        for e in all_events:
            if e.title not in data:
                data[e.title] = set()
            data[e.title].add(e)
        vnts: Set[Event] = set()
        for arr in map(sorted, data.values()):
            if len(arr) == 1:
                vnts.add(arr[0])
                continue
            sessions: Set[Session] = set()
            for e in arr:
                for s in e.sessions:
                    sessions.add(s.merge(url=(s.url or e.url)))
            vnts.add(e.merge(sessions=tuple(sorted(sessions))))
        events: Set[Event] = set()
        for e in vnts:
            surl = set(s.url for s in e.sessions if s.url)
            if len(surl) > 0:
                if len(surl) > 1:
                    e = e.merge(url='')
                else:
                    e = e.merge(
                        url=surl.pop(),
                        sessions=tuple(sorted(s.merge(url=None) for s in e.sessions))
                    )
            events.add(e)
        return tuple(sorted(events))

    def __url_to_events(self, url: str):
        self.get(url)
        date = None
        ym = url.rstrip("/").split("/")[-1]
        y = int(ym[:4])
        m = int(ym[4:])
        now = NOW.strftime("%Y-%m-%d")
        for n in self.soup.select("div.view-content h2.dia, div.view-content li.row"):
            txt = get_text(n)
            if txt is None:
                continue
            if n.name == "h2":
                d = int(txt.split()[0])
                date = f"{y}-{m:02d}-{d:02d}"
                continue
            if date < now:
                continue
            ev = self.__div_to_event(date, n, url)
            if ev:
                yield ev

    def __div_to_event(self, date: str, info: Tag, source: str):
        h = info.find("p", string=re.compile(r"^\s*Horario\s*:\s+\d\d:\d\d\s*$"))
        if h is None:
            #logger.warning(str(FieldNotFound("p[text=Horario: HH:MM]", source)))
            return None
        a = info.select_one("h3.titulo a")
        if a is None:
            raise FieldNotFound("h3.titulo", self.url)
        hm = get_text(h).split()[-1]
        url = a.attrs["href"]

        ev = self.__url_to_event(url)
        if ev is None:
            return None

        return ev.merge(
            name=get_text(a),
            sessions=(Session(
                url=url,
                date=date+" "+hm
            ),)
        )

    @cache
    def __url_to_event(self, url):
        self.get(url)
        if self.__is_block():
            return None
        js = self.__find_json()
        content = "\n".join(filter(lambda x: x is not None, map(get_text, self.soup.select("div.contenido p")))).lower()
        category = self.__find_category(content)
        return Event(
            id="am"+js['path']['currentPath'].split("/")[-1],
            url=url,
            name=None,
            img=self.__find_img(),
            price=self.__find_price(),
            category=category,
            duration=self.__find_duration(category, content),
            sessions=None,
            place=Place(
                name="La casa America",
                address="Plaza Cibeles, s/n, Salamanca, 28014 Madrid"
            )
        )

    def __is_block(self):
        txt = plain_text(self.soup.find("title"))
        if txt is None:
            raise FieldNotFound("title", self.url)
        if txt.lower().startswith("acceso denegado"):
            logger.warning("ACCESS DENIED "+self.url)
            return True
        return False

    def __find_img(self):
        return self.select_one("figure.imagen img").attrs["src"]

    def __find_json(self):
        js = get_text(self.select_one('script[type="application/json"]'))
        return json.loads(js)

    def __find_price(self):
        prices = set()
        for p in map(get_text, self.soup.select("article p")):
            if p is None or "General:" not in p:
                continue
            prices = prices.union(map(float, re.findall(r"([\d,.]+)€", p)))
        if len(prices) == 0:
            return 0
        return max(prices)

    def __find_duration(self, category: Category, content: str):
        durations = set()
        for p in map(get_text, self.soup.select("article p")):
            if p is not None:
                durations = durations.union(map(int, re.findall(r"(\d+)['’]", p)))
        if len(durations) > 0:
            return sum(durations)
        if re.search(r"1 hora y 20 minutos", content):
            return 80
        if re.search(r"lunes a viernes de 11.00 a 19.30. sábados de 11.00 a 15.00", content):
            return (60*8)+30
        if re.search(r"19.00 a 21.00", content):
            return 2*60
        if re.search(r"9.30 a 18.30", content):
            return 9*60
        if category in (Category.CONFERENCE, ):
            return 60
        logger.warning(str(FieldNotFound("duration", self.url)))
        return 0

    def __find_category(self, content: str):
        c = self.select_one("h1.tematica span.field")
        plan_content = plain_text(content)
        txt = plain_text(c).lower()
        if txt == "infantil":
            return Category.CHILDISH
        if re_or(plan_content, "presentacion (del )?libro"):
            return Category.CONFERENCE
        if txt == "cine":
            return Category.CINEMA
        if txt == "exposiciones":
            return Category.EXPO
        if txt == "teatro":
            return Category.THEATER
        if txt == "musica":
            return Category.MUSIC
        w1 = content.split()[0]
        if w1 == "concierto":
            return Category.MUSIC
        if re.search(r"proyección del documental", content):
            return Category.CINEMA
        if re.search(r"\b(conferencia|mesa redonda|debate|esta charla propone|seminario)\b", content) or w1 in ("presentación", "diálogo", "jornada"):
            return Category.CONFERENCE
        logger.critical(str(CategoryUnknown(self.url, txt)))
        return Category.UNKNOWN


if __name__ == "__main__":
    from .log import config_log
    config_log("log/casaamerica.log", log_level=(logging.DEBUG))
    print(CasaAmerica().events)
