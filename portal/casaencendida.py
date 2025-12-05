from core.web import get_text, Driver, MyTag
from typing import Set, Dict, List, Union, Optional
from functools import cache
from core.cache import TupleCache
import logging
from core.event import Event, Session, Places, Category, CategoryUnknown
import re
from datetime import datetime
from core.filemanager import FM
from selenium.webdriver.common.by import By

logger = logging.getLogger(__name__)

months = ('ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic')

SESSION_BAN = (
    'https://www.lacasaencendida.es/cine/poetas-2025-cine?eventId=7986',
    'https://www.lacasaencendida.es/cine/poetas-2025-cine?eventId=7987'
)


class CasaEncendidaException(Exception):
    pass


class CasaEncendida:
    URL = "https://www.lacasaencendida.es/actividades?t[0]=activity_"
    ACTIVITY = (2, 3)

    def __init__(self):
        self.__driver: Union[Driver, None] = None

    def __visit(self, url: str, wait_css: Optional[str] = None):
        if url != self.__driver.current_url:
            self.__driver.get(url)
            self.__driver.wait_ready()
            if wait_css:
                self.__driver.safe_wait(wait_css, by=By.CLASS_NAME)

    def __get_soup(self, url: str, wait_css: Optional[str] = None):
        self.__visit(url, wait_css=wait_css)
        return MyTag(url, self.__driver.get_soup())

    def __get_json(self, url: str) -> Union[Dict, List]:
        node = self.__get_soup(url)
        return node.select_one_json("body")

    def __get_ld_json(self, url: str) -> Dict:
        css_script = 'script[type="application/ld+json"]'
        js = self.__get_soup(url, wait_css=css_script).select_one_json(css_script)
        return js

    @cache
    def get_links(self):
        urls: Set[str] = set()
        with Driver(browser="firefox") as f:
            self.__driver = f
            for a in CasaEncendida.ACTIVITY:
                urls = urls.union(self.__get_links(CasaEncendida.URL+str(a)))
        return tuple(sorted(urls))

    def __get_links(self, url_cat):
        css_list = "div.results-list"
        urls: Set[str] = set()
        page = 0
        while True:
            page = page + 1
            soup = self.__get_soup(url_cat+f"&page={page}", wait_css=css_list)
            rsls = soup.select_one(css_list)
            links = rsls.select("a.results-list__link")
            for a in links:
                urls.add(a.attrs["href"])
            if len(links) == 0:
                return tuple(sorted(urls))

    @property
    @TupleCache("rec/casaencendida.json", builder=Event.build)
    def events(self):
        logger.info("Casa Encendida: Buscando eventos")
        events: Set[Event] = set()
        for url in self.get_links():
            events.add(self.__url_to_event(url))
        return tuple(sorted(events))

    def __url_to_event(self, url):
        soup = self.__get_soup(url)
        info = self.__get_ld_json(url)
        self.__validate_info_event(info)
        idevent = info[0]['identifier'].split("-")[-1]
        FM.dump(f"rec/casaencendida/{idevent}.json", info)
        return Event(
            id="ce"+idevent,
            url=url,
            name=info[0]['name'],
            category=self.__find_category(soup, info),
            img=info[0]['image'],
            place=Places.CASA_ENCENDIDA.value,
            sessions=self.__find_sessions(url, info),
            price=self.__find_price(info),
            duration=self.__find_duration(info)
        )

    def __validate_info_event(self, info: List):
        if not isinstance(info, list):
            raise CasaEncendidaException("MUST TO BE A LIST: "+str(info))
        if len(info) == 0:
            raise CasaEncendidaException("MUST TO BE A LIST NOT EMPTY: "+str(info))
        for i in info:
            if not isinstance(i, dict):
                raise CasaEncendidaException("MUST TO BE A LIS OF DICTs: "+str(info))
        identifier = info[0].get('identifier')
        if not isinstance(identifier, str):
            raise CasaEncendidaException("MUST TO BE A LIS OF DICTs with a identifier: "+str(info))
        idevent = identifier.split("-")[-1]
        if not idevent.isdigit():
            raise CasaEncendidaException("MUST TO BE A LIS OF DICTs with a int identifier: "+str(info))
        return True

    def __find_sessions(self, url: str, info: List[Dict]):
        if len(info) == 1:
            if url in SESSION_BAN:
                return tuple()
            return tuple((Session(
                url=url,
                date=info[0]["startDate"][:16].replace("T", " ")
            ), ))
        sessions: Set[Session] = set()
        for i in info[1:]:
            s_url = i['location']['url']
            if s_url in SESSION_BAN:
                continue
            sessions.add(Session(
                url=s_url,
                date=i["startDate"][:16].replace("T", " ")
            ))
        return tuple(sorted(sessions))

    def __find_price(self, info: List[Dict]):
        prices = set({0, })
        for i in info:
            if not i.get("offers"):
                continue
            for o in i["offers"]:
                prices.add(float(o["price"]))
        return max(prices)

    def __find_category(self, soup: MyTag, info: List[Dict]):
        for li in map(get_text, soup.node.select("ul.item-detail__list li")):
            if li and "No está permitida la entrada a mayores si no van acompañados de un menor" in li:
                return Category.CHILDISH
        tags = set()
        for tag in soup.select_txt(", ".join(
            (
                "div.tags",
                "div.item-detail__info__tags a",
                "div.breadcrumb__item a",
                "div.item-detail__hero__info__content a.group-link"
            )
        )):
            for t in re.split(r"\s*[,/\.]\s+", tag):
                t = re.sub(r"^#\s*", "", t.strip())
                tags.add(t.lower())
        if tags.intersection(("en familia", "espacio nido")):
            return Category.CHILDISH
        if tags.intersection(("cine", "audiovisuales")):
            return Category.CINEMA
        if tags.intersection(("conciertos", "música")):
            return Category.MUSIC
        name: str = info[0]['name'].lower()
        if "concierto" in name:
            return Category.MUSIC
        desc = soup.select_one_txt("div.item-detail__info__content")
        if "canciones" in desc:
            return Category.MUSIC
        if "film" in name.split():
            return Category.CINEMA
        logger.critical(str(CategoryUnknown(soup.url, ", ".join(sorted(tags)))))
        return Category.UNKNOWN

    def __find_duration(self, info: List[Dict]):
        def to_date(s: str):
            if s is not None:
                return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S%z")
        under24: Set[int] = set()
        over24: Set[int] = set()
        for i in info:
            startDate = to_date(i.get('startDate'))
            endDate = to_date(i.get('endDate'))
            if startDate and endDate:
                d = round((endDate - startDate).total_seconds() / 60)
                if d < 0:
                    continue
                if d < (24*60):
                    under24.add(d)
                else:
                    over24.add(d)
        if len(under24.union(over24)) == 0:
            raise FileNotFoundError("duration", info)
        if under24:
            return max(under24)
        return max(over24)


if __name__ == "__main__":
    from .log import config_log
    config_log("log/casaencendida.log", log_level=(logging.DEBUG))
    print(CasaEncendida().events)
