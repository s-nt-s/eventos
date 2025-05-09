from .web import get_text, Driver, WebException, MyTag
from .cache import TupleCache
from typing import Set, Dict, Union, List, Tuple
import logging
from .event import Event, Place, Session, Category, FieldNotFound, CategoryUnknown
import re
from bs4 import Tag
from datetime import datetime
from selenium.webdriver.common.by import By
from .util import plain_text, re_or
from functools import cached_property

logger = logging.getLogger(__name__)
NOW = datetime.now()


class MyIdTag(MyTag):
    def __init__(self, id: int, url: str, node: Tag):
        super().__init__(url, node)
        self.id = id


class CaixaForum:
    URLS = (
        "https://caixaforum.org/es/madrid/actividades?p=999",
        #"https://caixaforum.org/es/madrid/actividades?p=999&c=874798",
        #"https://caixaforum.org/es/madrid/actividades?p=999&c=874804",
        #"https://caixaforum.org/es/madrid/actividades?p=999&c=314579",
        #"https://caixaforum.org/es/madrid/actividades?p=999&c=161124943"
    )

    def __init__(self):
        self.__driver: Union[Driver, None] = None

    @cached_property
    def __category(self):
        done: Set[int] = set()
        category: Dict[Tuple[int, ...], Category] = {}
        with Driver(browser="firefox") as f:
            for cat, url in {
                Category.CHILDISH: "https://caixaforum.org/es/madrid/familia?p=999",
            }.items():
                ids = set(d.id for d in self.__get_div_events(f, url))
                category[tuple(sorted(ids.difference(done)))] = cat
                done = done.union(ids)
        return category

    def __visit(self, url: str):
        self.__driver.get(url)
        self.__driver.wait_ready()
        if re.search(r"_a\d+$", url):
            for slc in ("div.card-detail div.card-block-btn span", "#description-read"):
                self.__driver.safe_wait(slc, by=By.CSS_SELECTOR)

    def __get_soup(self, url: str):
        self.__visit(url)
        return MyTag(url, self.__driver.get_soup())

    def __get_json(self, url: str) -> Union[Dict, List]:
        node = self.__get_soup(url)
        return node.select_one_json("body")

    def __get_ld_json(self, url: str) -> Dict:
        js = self.__get_soup(url).select_one_json('script[type="application/ld+json"]')
        for k in ("startDate", "endDate"):
            v = js.get(k)
            if v:
                js[k] = datetime.fromisoformat(v)
        return js

    @property
    @TupleCache("rec/caixaforum.json", builder=Event.build)
    def events(self):
        events: Set[Event] = set()
        with Driver(browser="firefox") as f:
            self.__driver = f
            for url in CaixaForum.URLS:
                divs = self.__get_div_events(f, url)
                for div in divs:
                    events.add(self.__div_to_event(div))
        if None in events:
            events.remove(None)
        return tuple(sorted(events))

    def __get_div_events(self, f: Driver, url: str) -> Tuple[MyIdTag, ...]:
        events: List[MyIdTag] = []
        f.get(url)
        f.wait_ready()
        soup = f.get_soup()
        warn = get_text(soup.select_one("div.portlet-body div.title-warrings h2"))
        if warn is not None:
            logger.warning(warn+" "+url)
            return tuple()
        slc = "div.card-item:has(a > h2)"
        for div in soup.select(slc):
            h2 = div.select_one("a > h2")
            url = h2.find_parent("a").attrs["href"]
            eid = int(url.split("_a")[-1])
            events.append(MyIdTag(id=eid, url=url, node=div))
        if len(events) == 0:
            raise WebException(f"{slc} NOT FOUND in {url}")
        logger.debug(f"{len(events)} {slc}")
        return tuple(events)

    def __div_to_event(self, div: MyIdTag):
        h2 = div.select_one("a > h2")
        url = h2.find_parent("a").attrs["href"]
        info = self.__get_ld_json(url)
        ficha = self.__get_ficha(div.id)
        sessions = self.__find_session(url, ficha, info)
        if len(sessions) == 0:
            return None
        event_soup = self.__get_soup(url)
        category = self.__find_category(div.id, div)
        return Event(
            id=f"cf{div.id}",
            url=url,
            name=get_text(h2),
            img=self.__find_img(event_soup),
            price=self.__find_price(div.id, event_soup),
            category=category,
            duration=self.__find_duration(
                url,
                event_soup.select_one("#description-read"),
                category,
                info
            ),
            sessions=sessions,
            place=Place(
                name="Caixa Forum",
                address="Paseo del Prado, 36, Centro, 28014 Madrid"
            )
        )

    def __find_img(self, div: MyTag):
        try:
            nmg = div.select_one('figure img')
        except WebException as e:
            logger.warning("Imagen no encontrada: " + str(e))
            return None
        img = nmg.attrs.get("data-src") or nmg.attrs.get("src")
        img = re.sub(r"(\.jpe?g)/.*$", r"\1", img)
        return img

    def __get_ficha(self, eid: int) -> Dict:
        url = "https://caixaforum.org/es/web/madrid/actividades?p_p_id=headersearch_INSTANCE_HeaderSearch&p_p_lifecycle=2&p_p_resource_id=%2Fsearch%2FoneBox&_headersearch_INSTANCE_HeaderSearch_cpDefinitionId="+str(eid)
        js = self.__get_json(url)
        return js

    def __find_session(self, event_url: str, ficha: Dict, info: Dict):
        sessions: Set[Session] = set()
        ss: List[Dict] = ficha.get('sessions', [])
        for s in ss:
            if s.get('availableCapacity') == 0:
                continue
            h, mm = map(int, s["date"].split(":"))
            d, m, y = map(int, s["time"].split("/"))
            sessions.add(Session(
                url=s['url'],
                date=f"{y}-{m:02d}-{d:02d} {h:02d}:{mm:02d}"
            ))
        if sessions:
            return tuple(sorted(sessions))
        st: datetime = info.get("startDate")
        nd: datetime = info.get("endDate")
        if None in (st, nd) or st.date() != nd.date():
            logger.warning(str(FieldNotFound("session", event_url)))
            return tuple()
        return (Session(
                url=event_url,
                date=st.strftime("%Y-%m-%d %H:%M")
                ),)

    def __find_price(self, id: int, div: MyTag):
        for txt in div.select_txt("script"):
            m = re.search(r"loadOneBoxTicketsDetailActivity\(['\"]" + str(id) + r"['\"], ['\"](\d+)", txt)
            if m:
                return int(m.group(1))
        span = div.select_one_txt(
            "div.card-detail div.card-block-btn span",
            warning=True
        ) or ""
        price = span.lower()
        if "gratuita" in price:
            return 0
        prcs = tuple(map(int, re.findall(r"(\d+)\s*€", price)))
        if len(prcs) == 0:
            n = div.select_one("#description-read")
            prcs = tuple(map(int, re.findall(r"(\d+)\s*€", get_text(n))))
        if len(prcs) == 0:
            logger.warning(str(FieldNotFound("price", div.url)))
            return 0
        return max(prcs)

    def __find_duration(self, url_event: str, soup: Tag, category: Category, info: Dict):
        duration = []
        for p in map(get_text, soup.findAll("p", string=re.compile(r".*\d+\s+minutos.*"))):
            duration.extend(map(int, re.findall(r"\d+", p)))
        if duration:
            return sum(duration)
        if category in (Category.EXPO, Category.WORKSHOP, Category.MUSIC, Category.CONFERENCE, Category.UNKNOWN):
            return 60
        div = soup.select_one("div.secondary-text")
        if div is not None and div.find("p", string=re.compile(r".* (una|1) hora.*")):
            return 60
        st: datetime = info.get("startDate")
        nd: datetime = info.get("endDate")
        if None not in (st, nd) and st.date() == nd.date():
            return int((nd-st).total_seconds() / 60)
        logger.warning(f"duración no encontrada en {url_event}")
        return 60

    def __find_category(self, eid: int, div: MyTag):
        for ids, cat in self.__category.items():
            if eid in ids:
                return cat
        try:
            txt = div.select_one_txt("p.on-title")
        except WebException as e:
            logger.critical(str(CategoryUnknown(div.url, str(e))))
            return Category.UNKNOWN
        cat = plain_text(txt.lower())
        if re_or(cat, "concierto"):
            return Category.MUSIC
        if re_or(cat, "exposicion"):
            return Category.EXPO
        if re_or(cat, "proyecciones", "proyeccion"):
            return Category.CINEMA
        if re_or(cat, "taller", "curso", "espacio educativo"):
            return Category.WORKSHOP
        if re_or(cat, "encuentro", "conferencia", "debate", "tertulia", "jornada"):
            return Category.CONFERENCE
        #if re_or(cat, "otros formatos"):  # , "espacio de mediacion", "espectaculo"):
        #    return Category.OTHERS
        if re_or(cat, "visita"):
            return Category.VISIT
        if re_or(cat, "performance"):
            return Category.THEATER
        logger.critical(str(CategoryUnknown(div.url, txt)))
        return Category.UNKNOWN


if __name__ == "__main__":
    from .log import config_log
    config_log("log/caixaforum.log", log_level=(logging.DEBUG))
    (CaixaForum().events)
