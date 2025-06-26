from .web import get_text, Driver, WebException, MyTag
from .cache import TupleCache, HashCache
from typing import Set, Dict, Union, List, Tuple
import logging
from .event import Event, Place, Session, Category, FieldNotFound, CategoryUnknown
import re
from bs4 import Tag
from datetime import datetime
from selenium.webdriver.common.by import By
from .util import plain_text, re_or
from bs4 import BeautifulSoup

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
        self.__category = self.get__category()

    def get__category(self):
        done: Set[int] = set()
        category: Dict[Tuple[int, ...], Category] = {}
        with Driver(browser="firefox", wait=5) as f:
            self.__driver = f
            for cat, url in {
                Category.CHILDISH: "https://caixaforum.org/es/madrid/familia?p=999",
            }.items():
                ids = set(d.id for d in self.__get_div_events(url))
                category[tuple(sorted(ids.difference(done)))] = cat
                done = done.union(ids)
        return category

    @HashCache("rec/caixaforum/{}_sp.txt")
    def __get_html(self, url: str):
        self.__driver.get(url)
        self.__driver.wait_ready()
        if re.search(r"_a\d+$", url):
            for slc in (
                "div.card-detail div.card-block-btn span",
                "#description-read",
                "div.card-detail div.reserva-ficha"
            ):
                self.__driver.safe_wait(slc, by=By.CSS_SELECTOR)
        return str(self.__driver.get_soup())

    @HashCache("rec/caixaforum/{}_ld.json")
    def __get_ld_json(self, url: str) -> Dict:
        return self.get_soup(url).select_one_json('script[type="application/ld+json"]')

    def get_soup(self, url: str):
        html = self.__get_html(url)
        soup = BeautifulSoup(html, "lxml")
        return MyTag(url, soup)

    @HashCache("rec/caixaforum/{}_js.json")
    def get_json(self, url: str) -> Union[Dict, List]:
        node = self.get_soup(url)
        return node.select_one_json("body")

    def get_ld_json(self, url: str) -> Dict:
        js = self.__get_ld_json(url)
        for k in ("startDate", "endDate"):
            v = js.get(k)
            if v:
                js[k] = datetime.fromisoformat(v)
        return js

    @property
    @TupleCache("rec/caixaforum.json", builder=Event.build)
    def events(self):
        events: Set[Event] = set()
        with Driver(browser="firefox", wait=5) as f:
            self.__driver = f
            for url in CaixaForum.URLS:
                divs = self.__get_div_events(url)
                for div in divs:
                    events.add(self.__div_to_event(div))
        if None in events:
            events.remove(None)
        return tuple(sorted(events))

    def __get_div_events(self, url: str) -> Tuple[MyIdTag, ...]:
        events: List[MyIdTag] = []
        soup = self.get_soup(url).node
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
        info = self.get_ld_json(url)
        ficha = self.__get_ficha(div.id)
        sessions = self.__find_session(url, ficha, info)
        if len(sessions) == 0:
            return None
        event_soup = self.get_soup(url)
        price = self.__find_price(div.id, event_soup)
        if price < 0:
            return None
        category = self.__find_category(div, event_soup)
        return Event(
            id=f"cf{div.id}",
            url=url,
            name=get_text(h2),
            img=self.__find_img(event_soup),
            price=price,
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
                address="Paseo del Prado, 36, Centro, 28014 Madrid",
                latlon="40.41134208472603,-3.6935713500263523"
            )
        )

    def __find_img(self, div: MyTag):
        try:
            nmg = div.select_one('figure img, div.card-detail img')
        except WebException as e:
            logger.warning("Imagen no encontrada: " + str(e))
            return None
        img = nmg.attrs.get("data-src") or nmg.attrs.get("src")
        img = re.sub(r"(\.jpe?g)/.*$", r"\1", img)
        return img

    def __get_ficha(self, eid: int) -> Dict:
        url = "https://caixaforum.org/es/web/madrid/actividades?p_p_id=headersearch_INSTANCE_HeaderSearch&p_p_lifecycle=2&p_p_resource_id=%2Fsearch%2FoneBox&_headersearch_INSTANCE_HeaderSearch_cpDefinitionId="+str(eid)
        js = self.get_json(url)
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
        price = (div.select_one_txt(
            "div.card-detail div.card-block-btn span, div.card-detail div.reserva-ficha",
            warning=True
        ) or "").lower()
        if "gratuita" in price:
            return 0
        if "reserva de entradas disponible próximamente" in price:
            return -1
        prcs = tuple(map(int, re.findall(r"(\d+)\s*€", price)))
        if len(prcs) == 0:
            n = div.select_one("#description-read")
            prcs = tuple(map(int, re.findall(r"(\d+)\s*€", get_text(n))))
        if div.url == "https://caixaforum.org/es/madrid/p/ndv-normalmente-o-viceversa_a172958300":
            with open("/tmp/a.html", "w") as f:
                f.write(str(div.node))
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

    def __find_category(self, div: MyIdTag, event_soup: MyTag):
        for ids, cat in self.__category.items():
            if div.id in ids:
                return cat
        for txt in event_soup.select_txt("div.activity-detail-block"):
            if re_or(plain_text(txt), "los niños y niñas tienen que ir siempre acompañados de un adulto"):
                return Category.CHILDISH
        try:
            txt = div.select_one_txt("p.on-title, div.pre-title")
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
        try:
            txt_tit = div.select_one_txt("#title-read, div.card-txt-item h2")
        except WebException as e:
            logger.critical(str(CategoryUnknown(div.url, str(e))))
            return Category.UNKNOWN
        tit = plain_text(txt_tit.lower())
        if re_or(tit, "magia"):
            return Category.MAGIC
        if re_or(tit, "muestra de moda"):
            return Category.EXPO
        try:
            txt_des = div.select_one_txt("div.primary-text p")
        except WebException as e:
            logger.critical(str(CategoryUnknown(div.url, str(e))))
            return Category.UNKNOWN
        des = plain_text(txt_des.lower())
        if re_or(des, "encuentro coreografico", "danza tradicional"):
            return Category.DANCE
        try:
            href = div.select_one_attr("div.card-viewmore a", "href")
        except WebException as e:
            logger.critical(str(CategoryUnknown(div.url, str(e))))
            return Category.UNKNOWN
        plain_href = plain_text(href).lower()
        if re_or(plain_href, "circo"):
            return Category.CIRCUS
        if re_or(plain_href, "danza"):
            return Category.DANCE
        logger.critical(str(CategoryUnknown(div.url, f"{txt} {txt_tit} {txt_des} {href}")))
        return Category.UNKNOWN


if __name__ == "__main__":
    from .log import config_log
    config_log("log/caixaforum.log", log_level=(logging.DEBUG))
    (CaixaForum().events)
