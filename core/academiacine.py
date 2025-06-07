from .web import Web, get_text
from .cache import TupleCache
from typing import Set
from functools import cached_property
import logging
from .event import Event, Place, Session, Category, FieldNotFound, CategoryUnknown
import re
from datetime import datetime
from .util import plain_text, re_or


logger = logging.getLogger(__name__)
NOW = datetime.now()


class AcademiaCine(Web):
    URL = "https://entradas.aliro.academiadecine.com/"

    def get(self, url, auth=None, parser="lxml", **kwargs):
        if url == self.url:
            return self.soup
        logger.debug(url)
        return super().get(url, auth, parser, **kwargs)

    @cached_property
    def calendar(self):
        urls: Set[str] = set()
        self.get(AcademiaCine.URL)
        for a in self.soup.select("div.activities-wrapper a"):
            urls.add((a.attrs["href"], a.find("img").attrs["src"]))
        return tuple(sorted(urls))

    @property
    @TupleCache("rec/academiacine.json", builder=Event.build)
    def events(self):
        events: Set[Event] = set()
        for url, img in self.calendar:
            events.add(self.__url_to_event(url, img))
        if None in events:
            events.remove(None)
        return tuple(sorted(events))

    def __url_to_event(self, url: str, img: str):
        self.get(url)
        ev = Event(
            id="ac"+url.split("/")[-1],
            url=url,
            name=get_text(self.select_one("div.fs-1")),
            img=img,
            price=self.__find_price(),
            category=self.__find_category(),
            duration=self.__find_duration(),
            sessions=tuple((Session(
                date=self.__find_session()
            ),)),
            place=Place(
                name="Academia de cine",
                address="Calle de Zurbano, 3, Chamberí, 28010 Madrid"
            )
        )
        # hay que tener en cuenta que 2 entradas
        # son para silla de ruedas
        error = self.__get_error_in_buy(url, 4)
        if len(error) > 0:
            logger.warning(f"{ev.id}: {ev.name}: {' - '.join(error)}")
            return None
        return ev

    def __get_error_in_buy(self, url: str, entradas: int):
        url = f"{url}/compra"
        self.get(url)
        csrfmiddlewaretoken = self.select_one_attr('input[name="csrfmiddlewaretoken"]', "value")
        self.get(f"{url}?csrfmiddlewaretoken={csrfmiddlewaretoken}&entradas={entradas}")
        return tuple(filter(lambda x: x is not None, map(get_text, self.soup.select("ul.errorlist li"))))

    def __find_session(self):
        months = ("ene", "feb", "mar", "abr", "may", "jun", "jul", "ago", "sep", "oct", "nov", "dic")
        selector = "div.fs-5"
        txt = get_text(self.select_one(selector))
        dat = txt.split("|")[0].lower()
        match = re.search(r"(\d+) de (" + "|".join(months) + r")\S+ de (\d+) a las (\d+):(\d+)", dat)
        if match is None:
            raise FieldNotFound("date", self.url)
        d, month, y, h, mm = match.groups()
        m = months.index(month) + 1
        d, y, h, mm = map(int, (d, y, h, mm))
        return f"{y}-{m:02d}-{d:02d} {h:02d}:{mm:02d}"

    def __find_price(self):
        prices = set()
        for p in map(get_text, self.soup.select("div.session-info div")):
            prices = prices.union(map(lambda x: float(x.replace(",", ".")), re.findall(r"([\d,.]+)\s+euro\(s\)", p)))
        if len(prices) == 0:
            raise FieldNotFound("price", self.url)
        return max(prices)

    def __find_duration(self):
        td = self.soup.find("td", string=re.compile(r"^\s*\d+\s+minutos\s*$"))
        if td is None:
            logger.warning(str(FieldNotFound("duration", self.url)))
            return 60
        txt = get_text(td)
        return int(txt.split()[0])

    def __find_category(self):
        tds = tuple(map(plain_text, self.soup.select("th")))
        if len(set({"duracion", "idioma", "formato"}).difference(tds)) == 0:
            return Category.CINEMA
        tit = plain_text(get_text(self.select_one("div.fs-1")))
        txt = get_text(self.select_one("div.fs-5"))
        cat = plain_text(txt.split("|")[-1]).lower()
        if cat in ("la academia preestrena", "aniversarios de cine", "series de cine"):
            return Category.CINEMA
        if cat in ("los oficios del cine", "libros de cine"):
            return Category.CONFERENCE
        if re.search(r"\bcorto\b", tit):
            return Category.CINEMA
        if re_or(cat, "podcast"):
            return Category.CONFERENCE
        if re_or(tit, "jornada sobre"):
            return Category.CONFERENCE
        desc = plain_text(get_text(self.select_one("div.session-desc")))
        if re_or(desc, "sesion informativa"):
            return Category.CONFERENCE
        logger.critical(str(CategoryUnknown(self.url, txt)))
        return Category.UNKNOWN


if __name__ == "__main__":
    from .log import config_log
    config_log("log/academiacine.log", log_level=(logging.DEBUG))
    print(AcademiaCine().events)
