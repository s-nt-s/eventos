from core.web import Web, get_text, get_query
from bs4 import Tag
from functools import cached_property
from urllib.parse import urljoin
import re
from core.event import Event, Category, Places, Session, Place
from core.util import re_or, plain_text
import logging


logger = logging.getLogger(__name__)
re_num = re.compile(r"\d+")
re_date = re.compile(r"^\d{1,2}/\d{1,2}/20\d{2}$")
re_hour = re.compile(r"^\d{2}:\d{2}$")


def _get_date(s: str):
    if s is None or not re_date.match(s):
        return None
    d, m, y = map(int, re_num.findall(s))
    return f"{y}-{m:02d}-{d:02d}"


def _get_hour(s: str):
    if s is None or not re_hour.match(s):
        return None
    h, m = map(int, re_num.findall(s))
    return f"{h:02d}:{m:02d}"


class KineTike:
    SALA_EQUIS = "cine=EQUIS"
    ERROR_URL = "https://kinetike.com:83/views/error.aspx"

    def __init__(self, sala: str, place: Place):
        self.__w = Web()
        self.__w.get(f"https://kinetike.com:83/views/sesionesFuturas.aspx?{sala}")
        id_button = self.__select_one_attr("#UpdatePanelCabecera input", "name")
        self.__w.submit("#formulario", **{
            f"{id_button}.x": "0",
            f"{id_button}.y": "0"
        })
        self.__root = self.__w.response.url
        self.__place = place

    def __get(self, url: str):
        soup = self.__w.get(url)
        if self.__w.url == KineTike.ERROR_URL:
            raise ValueError(self.__w.soup.get_text(strip=True))
        return soup

    def __select_one_attr(self, slc: str, attr: str):
        n = self.__w.select_one(slc)
        if n is None:
            raise ValueError(f"{slc} not found in {self.__w.url}")
        if attr == ':text':
            val = get_text(n)
        else:
            val = n.attrs.get(attr)
        if val is None:
            raise ValueError(f"{slc}[{attr}] empty in in {self.__w.url}")
        if attr in ('src', ):
            val = urljoin(self.__w.url, val)
        return val

    @cached_property
    def urls(self):
        urls: set[str] = set()
        self.__get(self.__root)
        for i in self.__w.soup.select("input[type='image'][onclick]"):
            click = i.attrs.get("onclick")
            if not isinstance(click, str):
                continue
            m = re.match(r'^.*"(sesionesFuturas.aspx[^"]+).*', click)
            if m:
                url = urljoin(self.__root, m.group(1))
                urls.add(url)
        return tuple(sorted(urls))

    @cached_property
    def events(self):
        logger.info("KineTike: Buscando eventos")
        evs: set[Event] = set()
        for url in self.urls:
            ev = self.__get_event_from_url(url)
            if ev:
                evs.add(ev)
        logger.info(f"KineTike: Buscando eventos = {len(evs)}")
        return tuple(sorted(evs))

    def __get_event_from_url(self, url):
        qr = get_query(url)
        soup = self.__visit_event(url)
        duration = self.__select_one_attr("#lblDuracion", ':text')

        ev = Event(
            id=qr['cine']+qr['idPelicula'],
            url=url,
            name=get_text(soup.select_one("#tituloPeli")),
            price=-1,
            category=Category.UNKNOWN,
            place=self.__place,
            duration=int(re.findall(r"\d+", duration)[0]),
            img=self.__select_one_attr("#ImPelicula", "src"),
            sessions=self.__get_sessions(soup)
        )
        if len(ev.sessions) == 0:
            return None
        price = self.__get_price(soup)
        ev = ev.merge(
            price=price,
            category=self.__find_category(ev, price)
        )
        return ev

    def __find_category(self, ev: Event, price: float):
        name = plain_text(ev.name)
        if re_or(name, "vhz"):
            return Category.CONFERENCE
        if price < 10:
            return Category.CINEMA
        if re_or(name, "fiesta nochevieja"):
            return Category.PARTY
        return Category.UNKNOWN

    def __get_sessions(self, soup: Tag):
        complete = False
        sessions: set[Session] = set()
        dt = None
        for n in soup.select("#PanelSesiones span, #PanelSesiones input"):
            dt = _get_date(get_text(n)) or dt
            hm = _get_hour(n.attrs.get('value'))
            if 'btn-danger' in n.attrs.get('class', []):
                complete = True
                continue
            if dt and hm:
                sessions.add(Session(date=dt+' '+hm))
        if len(sessions) == 0 and not complete:
            raise ValueError(f"No se ha encontrado sesiones en {self.__w.url}")
        return tuple(sorted(sessions))

    def __get_price(self, soup: Tag):
        re_price = re.compile(r"(\d+(?:[,\.\d]+))\s*€")
        session: dict[str, str] = dict()
        action, data = self.__w.prepare_submit("#formulario")
        for n in soup.select("#UpdatePanelSesiones input"):
            name = n.attrs.get('name')
            hm = _get_hour(n.attrs.get('value'))
            if name and hm:
                session[name] = hm
        for n in session.values():
            if n in data:
                del data[n]
        prices: set[float] = set()
        for k, v in session.items():
            dt = dict(data)
            dt[k] = v
            self.__w.get(action, **dt)
            for txt in map(get_text, self.__w.soup.select("#PanelPrecios div")):
                for p in re_price.findall(txt or ""):
                    prices.add(float(p))
        if len(prices) == 0:
            raise ValueError(f"Precio no encontrado en {self.__w.url}")
        return max(prices)

    def __visit_event(self, url: str):
        soup = self.__w.get(url)
        val_button = 'SESIONES'
        button: set[str] = set()
        for i in soup.select("#PanelSesiones input[name]"):
            name = i.attrs.get('name')
            if name and i.attrs.get('value') == val_button:
                button.add(name)
        if len(button) == 0:
            raise ValueError(f"No se han encontrado sesiones en {self.__w.url}")
        action, data = self.__w.prepare_submit("#formulario")
        for b in button:
            if b in data:
                del data[b]
        for b in sorted(button):
            dt = dict(data)
            dt[b] = val_button
            self.__w.get(action, **dt)
        return self.__w.soup


if __name__ == "__main__":
    k = KineTike(KineTike.SALA_EQUIS, Places.SALA_EQUIS.value)
    print(*k.events, sep="\n")
