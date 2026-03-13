from core.web import Web, get_text, buildSoup, Tag
from functools import cached_property
from core.event import Event, Cinema, Places, Category, Session, CategoryUnknown
from core.util import plain_text, to_uuid, find_euros, re_or
import re
from datetime import date, datetime
from core.fetcher import Getter
from aiohttp import ClientResponse
from core.cache import TupleCache
import logging

logger = logging.getLogger(__name__)

re_date = re.compile(r"^\d{1,2}[/\.]\d{1,2}[/\.]20\d{2}$")
TODAY = date.today()


def _det_date(s: str):
    if s is None:
        return None
    if not re_date.match(s):
        return None
    d, m, y = tuple(map(int, re.findall(r"\d+", s)))
    return date(y, m, d)


async def rq_to_events(r: ClientResponse):
    url = str(r.url)
    soup = buildSoup(url, await r.text())
    if url.startswith(CirculoBellasArtes.URL_CINEMA):
        return await soup_to_cinema(url, soup)
    return await soup_to_event(url, soup)


def dl_to_dict(*dls: Tag):
    info: dict[str, str | None] = {}
    for dl in dls:
        for dt, dd in zip(dl.select("dt"), dl.select("dd")):
            k = plain_text(get_text(dt))
            if k is None:
                continue
            k = k.lower()
            v = get_text(dd)
            if k in info and info[k] != v:
                raise ValueError()
            info[k] = v
    return info


def table_to_dict(table: Tag):
    info: dict[str, str | None] = {}
    if not table:
        return info
    for tr in table.select("tr"):
        tds = tuple(map(get_text, tr.select("td")))
        if len(tds) != 2:
            continue
        k = plain_text(tds[0])
        if k is None:
            continue
        k = k.lower()
        v = tds[1]
        if k in ("duration", ) and v is not None:
            m = re.match(r"^(\d+)h\s+(\d+)min$", v, flags=re.I)
            if m is None:
                raise ValueError(f"?duración={v}?")
            v = int(m.group(1))*60 + int(m.group(1))
        if k in info and info[k] != v:
            raise ValueError()
        info[k] = v
    return info


async def soup_to_cinema(url: str, soup: Tag):
    if soup.find(string=re.compile(r"^\s*Este\s+evento\s+ha\s+finalizado\s*$")):
        return None
    h1 = soup.select_one("div[data-post-id] h1")
    h3 = h1
    while h3 and h3.name != "h3":
        h3 = h3.find_parent("div")
        aux = h3.select_one("h3")
        if aux:
            h3 = aux
    inf = table_to_dict(soup.select_one("table.cba_tabla_ficha"))
    img = soup.select_one('div.fl-col-small div.fl-photo[role="figure"] img.entered[data-src]')
    template = Cinema(
        id="cba"+to_uuid(url),
        url=url,
        name=get_text(h1),
        director=(inf.get("direccion") or get_text(h3),),
        place=Places.CIRCULO_BELLAS_ARTES.value,
        category=Category.CINEMA,
        sessions=tuple(),
        duration=inf.get("duration"),
        img=img.attrs.get("data-src") if img else None,
        price=None,
    )
    price_event: dict[float, Cinema] = {}
    sessions = table_to_dict(soup.select_one("table.cba_tabla_sesiones"))
    for k, v in sessions.items():
        v = plain_text(v)
        if v:
            v = v.lower()
        price = {
            "precio reducido": 5.50, #(18/5)
            None: 8
        }.get(v)
        if price is None:
            logger.warning(f"NOT FOUND price={k} {url}")
            continue
        ev: Cinema = price_event.get(price, template)
        d, m, h, mm = map(int, re.findall(r"\d+", k))
        dt = datetime(TODAY.year, m, d, h, mm)
        if TODAY.month == 1 and dt.month == (11, 12):
            dt = dt.replace(year=TODAY.year-1)
        elif TODAY.month == 12 and dt.month in (1, 2):
            dt = dt.replace(year=TODAY.year+1)
        if dt.date() >= TODAY:
            ev_se = set(ev.sessions)
            ev_se.add(Session(
                date=dt.strftime("%Y-%m-%d %H:%M")
            ))
            price_event[price] = ev.merge(
                price=price,
                sessions=tuple(sorted(ev_se))
            )
    return tuple(price_event.values())


async def soup_to_event(url: str, soup: Tag):
    if soup.find(string=re.compile(r"^\s*Invitaciones\s+agotadas\s*$")):
        return None
    inf = dl_to_dict(*soup.select(".cba-events-details dl"))
    price = find_euros(inf.get("precio"))
    if price is None:
        logger.warning(f"NOT FOUND price {url}")
        return None
    fc = inf.get("fecha")
    hr = inf.get("horario")
    if None in (fc, hr):
        logger.warning(f"NOT FOUND fecha/horario {url}")
        return None
    dt_int = list(map(int, re.findall(r"\d+", f"{fc} {hr}")))
    if len(dt_int) == 4:
        dt_int.append(0)
    if len(dt_int) != 5:
        logger.warning(f"NOT FOUND fecha/horario {url}")
        return None
    d, m, y, h, mm = dt_int
    dt = datetime(y, m, d, h, mm)
    name = get_text(soup.select_one("div[data-post-id] h1"))
    meta = soup.select_one('meta[property="og:image"][content]')
    ev = Event(
        id="cba"+to_uuid(url),
        img=meta.attrs.get("content") if meta else None,
        url=url,
        name=name,
        place=Places.CIRCULO_BELLAS_ARTES.value,
        price=price,
        sessions=(Session(date=dt.strftime("%Y-%m-%d %H:%M")), ),
        duration=60,
        category=_find_category(url, name, soup)
    )
    return ev


def _find_category(url: str, title: str, soup: Tag):
    sub_title = get_text(soup.select_one("#fl-main-content div[data-post-id] h3"))
    if re_or(
        sub_title,
        r"Presentaci[óo]n del libro",
        r"Presentaci[oó]n de la revista",
        flags=re.I
    ):
        return Category.LITERATURE
    if re_or(
        title,
        r"Conferencias de",
        flags=re.I
    ):
        return Category.CONFERENCE
    n_desc = soup.select_one('div:has(+ footer) div.fl-col:not(.fl-col-small) div.fl-module-rich-text[data-node]')
    if n_desc:
        for n in n_desc.select("br, p"):
            n.append("\n")
    desc = get_text(n_desc)
    if re_or(
        desc,
        "Beethoven crepuscular",
        flags=re.I
    ):
        return Category.MUSIC
    if re_or(
        desc,
        "proyecci[óo]n del documental",
        flags=re.I
    ):
        return Category.CINEMA
    if re_or(
        desc,
        r"La presentaci[oó]n del libro",
        r"la pr[oó]xima publicaci[oó]n del libro",
        flags=re.I
    ):
        return Category.LITERATURE
    if re_or(
        desc,
        r"panel de conversaci[óo]n",
        flags=re.I
    ):
        return Category.CONFERENCE
    logger.critical(str(CategoryUnknown(url, "")))
    return Category.UNKNOWN


class CirculoBellasArtes:
    URL_CINEMA = "https://www.circulobellasartes.com/ciclos-cine/peliculas/"

    def __init__(self):
        self.__w = Web()
        self.__w.s.headers.update({
            'Accept-Encoding': 'gzip, deflate'
        })

    @cached_property
    def urls(self):
        urls: set[str] = set()
        soup = self.__w.get("https://www.circulobellasartes.com/cine-estudio/")
        for a in soup.select("a[href]"):
            url = a.attrs["href"]
            if url.startswith(CirculoBellasArtes.URL_CINEMA):
                urls.add(url)
        soup = self.__w.get("https://www.circulobellasartes.com/agenda/")
        for p in soup.select("p.carousel-item-fecha"):
            dt = _det_date(get_text(p))
            if dt and dt >= TODAY:
                div = p.find_parent("div")
                a = div.select_one("a")
                urls.add(a.attrs["href"])
        return tuple(sorted(urls))

    @cached_property
    @TupleCache("rec/circulo.json", builder=Event.build)
    def events(self):
        evs: set[Event] = set()
        for x in Getter(
            onread=rq_to_events
        ).get(*self.urls).values():
            if x is None:
                continue
            if isinstance(x, (Event, Cinema)):
                evs.add(x)
                continue
            for i in x:
                evs.add(i)
        return tuple(sorted(evs))


if __name__ == "__main__":
    c = CirculoBellasArtes()
    c.events
