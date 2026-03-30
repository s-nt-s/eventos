from core.fetcher import Getter
from aiohttp import ClientResponse
from core.web import buildSoup, get_text, Tag
from core.event import Category, Event, CategoryUnknown, Session, FIX_EVENT, find_book_category
from core.place import Places
from core.util import get_obj, re_or, find_euros, to_uuid, get_main_value, get_domain, clean_url
from typing import NamedTuple, Optional
from core.cache import TupleCache
from collections import defaultdict
import re
from datetime import datetime
import logging
from core.md import MD

logger = logging.getLogger(__name__)


MONTHS = ('ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic')
RE_MONTHS = r"(" + "|".join(map(str.capitalize, MONTHS)) + r")"
RE_DATE_1 = re.compile(r"^(\d+) de " + RE_MONTHS + r" de (\d+)[·\s]+(\d+):(\d+)\s*-\s*(\d+):(\d+)$")
RE_DATE_2 = re.compile(r"^Del (\d+) de "+RE_MONTHS+r"\S* al (\d+) de "+RE_MONTHS+r"\S* de (\d+)$")
re_sp = re.compile(r"\s+")
MAX_YEAR = datetime.now().year + 1

_SEP = r"\-\.\|"
_TRIM = (
    r"Presentaci[oó]n del? libro",
    r"Ciclo de cine",
    r"Primavera \d+",
    r"Visitas Xtraordinarias",
    r"Obra Teatral",
    r"Conferencia magistral de [^"+_SEP+"]+",
    r"El sueño de Madrid \d+",
    r"Club de lectura",
    r"Primera sesi[oó]n",
    r"Segunda sesi[oó]n",
    r"Tercera sesi[oó]n",
    r"Cuarta sesi[oó]n",
    r"\d+º? proyecci[oó]n",
    r"Taller literario",
    r"M[eé]xico (Lindo|Tropical)",
    r"MEXES",
    r"Taller literario",
    r"Curso",
    r"\d+ sesiones",
    r"Conversaciones transatl[aá]nticas",
    r"Ciclo de cine familiar",
)

_RE_PRE = re.compile(r"^\s*(" + "|".join(_TRIM) + r")\s*["+_SEP+r":]+\s*", flags=re.I)
_RE_SUF = re.compile(r"\s*[" + _SEP + r"]+\s*(" + "|".join(_TRIM) + r")\s*$", flags=re.I)


def _get_attr(tag: Tag, slc: str, attr: str):
    if tag is None:
        return None
    n = tag.select_one(slc)
    if n is None:
        return None
    v = n.attrs[attr]
    if isinstance(v, str):
        v = v.strip()
        if len(v):
            return v


def _like_in(s: str, arr: list[str]):
    if len(s) < 5:
        return False
    for i in arr:
        if s in i:
            return True
    return False


def _clean_name(name: str):
    if name is None:
        return None
    if not isinstance(name, str):
        raise ValueError(f"name must be a str, but is a {type(name)}: {name}")
    bak = ['']

    while bak[-1] != name:
        bak.append(name)
        spl = set(re.split(r"\s+-\s+", name))
        if len(spl) == 1:
            n = spl.pop().strip()
            if len(n):
                name = n
        spl = [i for i in re.split(r"\s+\|\s+", name) if i]
        if len(spl) > 1:
            new_name = []
            while spl:
                i = spl.pop(0)
                if not _like_in(i, new_name+spl):
                    new_name.append(i)
            name = " | ".join(new_name)

        name = _RE_PRE.sub("", name)
        name = _RE_SUF.sub("", name)
        if len(name) < 2:
            name = bak[-1]
    w1 = name[0]
    if w1.isalpha():
        name = w1.upper()+name[1:]
    return name


def _to_datetimes(f: str):
    if f is None:
        return None
    m = RE_DATE_1.match(f)
    if m is not None:
        a = datetime(int(m.group(3)), MONTHS.index(m.group(2).lower())+1, int(m.group(1)), int(m.group(4)), int(m.group(5)))
        z = a.replace(hour=int(m.group(6)), minute=int(m.group(7)))
        return a, z
    m = RE_DATE_2.match(f)
    if m is not None:
        y = int(m.group(5))
        a = datetime(y, MONTHS.index(m.group(2).lower())+1, int(m.group(1)), 0, 0)
        z = datetime(y, MONTHS.index(m.group(4).lower())+1, int(m.group(3)), 23, 59)
        return a, z
    return None


class Dt(NamedTuple):
    start: str
    duration: int
    url: Optional[str] = None
    full: Optional[bool] = None


class Item(NamedTuple):
    url: str
    name: str
    img: str
    description: str
    tags: tuple[str, ...]
    dates: tuple[Dt, ...] = tuple()
    price: Optional[float] = None
    director: Optional[float] = None
    year: Optional[int] = None
    public: Optional[str] = None
    place: Optional[str] = None
    links: tuple[str, ...] = tuple()

    @staticmethod
    def build(*args, **kwargs):
        obj = get_obj(*args, **kwargs)
        if obj is None:
            return None
        dt = obj.get("dates")
        if dt:
            obj['dates'] = tuple(Dt(**x) for x in dt)
        for k, v in list(obj.items()):
            if isinstance(v, list):
                obj[k] = tuple(v)
        return Item(**obj)


async def rq_to_items(r: ClientResponse):
    items: set[Item] = set()
    root = str(r.url)
    soup = buildSoup(root, await r.text())
    for div in soup.select("#add-agenda div.actividad"):
        url = div.select_one("a").attrs["href"]
        name = get_text(div.select_one(".info-nombre"))
        img = div.select_one("img.imagen").attrs["src"]
        tags: list[str] = []
        for t in map(
            str.strip,
            re.split(
                r"\s*,\s*",
                (get_text(div.select_one(".info-tipo-actividad")) or '').lower()
            )
        ):
            if t and t not in tags:
                tags.append(t)
        info_date = get_text(div.select_one(".info-fecha"))
        if info_date is None:
            logger.critical(f".info-fecha en {url} via {root}")
            continue
        az = _to_datetimes(info_date)
        if az is None:
            raise ValueError(f".info-fecha={info_date} en {url} via {root}")
        a, z = az
        btn = div.select_one(".info-ver-reserva, .info-ver-reserva-table")
        rsv = (_get_attr(btn, "a[href]", "href") or "").replace("?aff=oddtdtcreator", "")
        if rsv in ("", url):
            rsv = None
        items.add(Item(
            url=url,
            name=name,
            img=img,
            description=get_text(div.select_one("div.info-descripcion")),
            tags=tuple(tags),
            dates=(Dt(
                start=a.strftime("%Y-%m-%d %H:%M"),
                duration=int((z-a).total_seconds()//60),
                url=clean_url(rsv),
                full=re_or(
                    get_text(btn),
                    "Entradas agotadas",
                    flags=re.I
                ) is not None
            ),),
        ))
    return tuple(sorted(items))


class Page(NamedTuple):
    price: Optional[float] = None
    director: Optional[float] = None
    year: Optional[int] = None
    public: Optional[str] = None
    place: Optional[str] = None
    description: Optional[str] = None
    evenbrite: tuple[str, ...] = tuple()
    links: tuple[str, ...] = tuple()

    @staticmethod
    def build(*args, **kwargs):
        obj = get_obj(*args, **kwargs)
        if obj is None:
            return None
        for k, v in list(obj.items()):
            if isinstance(v, list):
                obj[k] = tuple(v)
        return Page(**obj)


def _find_div_img(n: Tag, src: str) -> Tag | None:
    img = n.select_one(f'img[src="{src}"]')
    if img:
        div = img.find_parent("div")
        while get_text(div) is None:
            div = div.find_parent("div")
        return div


async def rq_to_page(r: ClientResponse):
    if r.status == 404:
        return None
    r.raise_for_status()
    root = str(r.url)
    soup = buildSoup(root, await r.text())
    div = soup.select_one("div.e-con-inner")
    director = None
    year = None
    price = find_euros(get_text(_find_div_img(
        div,
        "https://www.casademexico.es/wp-content/uploads/2023/12/precio.svg"
    )))
    public = get_text(_find_div_img(
        div,
        "https://www.casademexico.es/wp-content/uploads/2023/12/publico.svg"
    ))
    place = get_text(_find_div_img(
        div,
        "https://www.casademexico.es/wp-content/uploads/2023/12/localizacion.svg"
    ))

    description = None
    links: list[str] = []
    evenbrite: list[str] = []
    if price is None:
        logger.warning(f"NOT FOUND price {r.url}")
    if div:
        description = MD.convert(div.select_one("div.elementor-element.elementor-widget.elementor-widget-text-editor"))
        for li in map(get_text, div.select("div.elementor-widget-container ul li")):
            if not isinstance(li, str):
                continue
            if li.startswith("Dir. "):
                director = li.split(None, 1)[1]
                continue
            for i in map(int, re.findall(r"\d+", li)):
                if i > 1895 and i <= MAX_YEAR and (year is None or year < i):
                    year = i
        for a in div.select("a.btn-evenbrite[href]"):
            href = (a.attrs.get("href") or "").replace("?aff=oddtdtcreator", "").strip()
            if len(href) > 0 and href not in evenbrite:
                evenbrite.append(href)
        for a in div.select("a[href]"):
            href = a.attrs.get("href")
            if get_domain(href) == "casademexico.es" and href not in links:
                links.append(href)
    return Page(
        price=price,
        year=year,
        director=director,
        public=public,
        place=place,
        description=description,
        evenbrite=tuple(evenbrite),
        links=tuple(links)
    )


class CasaMexico:
    URL_LIST = "https://www.casademexico.es/wp-content/themes/hello-elementor/rellenar-agenda.php?fecha=todas"

    def __init__(self):
        self.__get_items = Getter(
            onread=rq_to_items
        )
        self.__get_pages = Getter(
            onread=rq_to_page,
            raise_for_status=False
        )

    @TupleCache(r"rec/casamexico/items.json", builder=Item.build)
    def _get_items(self):
        data: dict[str, tuple[Item, ...]] = self.__get_items.get(
            f"{CasaMexico.URL_LIST}&tipo=musica",
            f"{CasaMexico.URL_LIST}&tipo=literatura",
            f"{CasaMexico.URL_LIST}&tipo=teatro",
            f"{CasaMexico.URL_LIST}&tipo=academicas",
            f"{CasaMexico.URL_LIST}&tipo=cine",
            f"{CasaMexico.URL_LIST}&tipo=exposiciones",
        )
        url_item: dict[str, set[Item]] = defaultdict(set)
        for its in data.values():
            for i in its:
                url_item[i.url].add(i)
        pages: dict[str, Page] = self.__get_pages.get(*url_item.keys())
        items: set[Item] = set()
        for u, itms in url_item.items():
            dct_dates: dict[str, set[str]] = defaultdict(str)
            tags: list[str] = []
            dct_dates: dict[str, set[Dt]] = defaultdict(set)
            for x in itms:
                for t in x.tags:
                    if t not in tags:
                        tags.append(t)
                for d in x.dates:
                    dct_dates[d.start].add(d)
            dates: set[str] = set()
            for s, dts in dct_dates.items():
                dates.add(Dt(
                    start=s,
                    duration=get_main_value((x.duration for x in dts)),
                    url=get_main_value((x.url for x in dts)),
                    full=any(x.full is True for x in dts)
                ))
            description = "\n\n".join(sorted(set(
                x.description for x in itms if x.description
            )))
            i = Item(
                url=u,
                description=description,
                name=sorted((x.name for x in itms), key=lambda x: (len(x), x))[0],
                img=get_main_value((x.img for x in itms), default=None),
                tags=tuple(tags),
                dates=tuple(sorted(dates))
            )
            p = pages.get(i.url)
            if p:
                lst_dates = list(i.dates)
                if len(p.evenbrite) == 1:
                    dts = [d for d in lst_dates if not d.start.endswith("00:00")]
                    if len(dts) == 1 and dts[0].url is None:
                        lst_dates.remove(dts[0])
                        lst_dates.append(dts[0]._replace(url=p.evenbrite[0]))
                desc = p.description
                if desc is None or len(desc) < len(i.description or ''):
                    desc = i.description
                i = i._replace(
                    dates=tuple(sorted(lst_dates)),
                    description=desc,
                    price=p.price,
                    director=p.director,
                    year=p.year,
                    public=p.public,
                    place=p.place,
                    links=p.links
                )
            items.add(i)
        return tuple(sorted(items))

    @property
    @TupleCache("rec/casamexico.json", builder=Event.build)
    def events(self):
        dct_events: dict[Event, Item] = {}
        for i in self._get_items():
            category = self.__find_easy_category(i)
            duration, sessions = self.__get_duration_and_sessions(i)
            if len(sessions) == 0:
                logger.debug(f"Descartado por no tener sesiones {i.url}")
                continue
            cycle, name = self.__get_cycle_name(i)
            e = Event(
                id=CasaMexico.get_id(i.url),
                name=name,
                url=i.url,
                category=category,
                price=i.price or 0,
                img=i.img,
                duration=duration,
                sessions=sessions,
                place=Places.CASA_MEXICO.value,
                cycle=cycle if category != Category.CINEMA else None
            )
            if e.category == Category.CINEMA and (i.director or i.year):
                e = e.fix_type().merge(
                    director=(i.director, ) if i.director else tuple(),
                    year=i.year
                )
            dct_events[e] = i

        e_more: dict[Event, set[str]] = defaultdict(set)
        is_more: set[str] = set()
        for e, i in dct_events.items():
            for lk in i.links:
                for x in dct_events.keys():
                    if x.url in lk:
                        is_more.add(e.url)
                        e_more[x.url].add(e.url)
        events: set[str] = set()
        for e, i in dct_events.items():
            if i.links and re_or(i.name, r"Ciclo", flags=re.I):
                if len(set(i.links).difference(e_more.keys())) == 0:
                    continue
            if e.more is None and e.category != Category.CINEMA and e.url in e_more:
                e = e.merge(more=get_main_value(e_more[e.url]))
            events.add(e)
        return tuple(sorted(events))

    def __get_cycle_name(self, i: Item):
        name = _clean_name(i.name)
        spl = tuple(x for x in re.split(r"\s+\|\s+", name) if x)
        if len(spl) == 2:
            return tuple(spl)
        if len(spl) == 3 and re_or(spl[1], "encuentro de escritores hispanoamericanos", flags=re.I):
            return "Encuentro de escritores hispanoamericanos", re.sub(r"Mesa \d+\s+", "", spl[2], flags=re.I)
        return None, name

    def __get_duration_and_sessions(self, i: Item):
        sessions: set[Session] = set()
        sessions_0: set[Session] = set()
        duration: set[int] = set()
        duration_0: set[int] = set()
        for f in i.dates:
            if f.full:
                continue
            s = Session(
                date=f.start,
                url=f.url
            )
            if s.url is None and f.start.endswith("00:00"):
                sessions_0.add(s)
                duration_0.add(f.duration)
            else:
                sessions.add(s)
                duration.add(f.duration)
        if len(sessions) == 0:
            sessions = sessions_0
            duration = duration_0
        d = max(duration) if duration else None
        return d, tuple(sorted(sessions))

    @staticmethod
    def get_id(url: str):
        return "mx" + to_uuid(url)

    def __find_easy_category(self, i: Item):
        cat = FIX_EVENT.get(CasaMexico.get_id(i.url), {}).get('category')
        if isinstance(cat, str):
            return Category[cat]
        if re_or(
            i.name,
            "Presentaci[oó]n del? libro",
            flags=re.I
        ):
            return find_book_category(i.name, i.description, Category.LITERATURE)
        for t, c in {
            "familia": Category.CHILDISH,
            "cine": Category.CINEMA,
            "música": Category.MUSIC,
            "teatro": Category.THEATER,
            "talleres": Category.WORKSHOP
        }.items():
            if t in i.tags:
                return c
        if re_or(
            i.name,
            r"Coloquio",
            r"^Conferencia",
            r"^Conversaciones Transatl[aá]nticas",
            r"encuentro de escritores",
            flags=re.I
        ):
            return Category.CONFERENCE
        if re_or(
            i.name,
            "^Curso",
            r"Sesi[óo]n de dibujo",
        ):
            return Category.WORKSHOP
        if re_or(
            i.name,
            "^Visitas Xtraordinarias",
        ):
            return Category.VISIT
        if re_or(
            i.name,
            "^Club de lectura",
        ):
            return find_book_category(i.name, i.description, Category.READING_CLUB)

        logger.critical(str(CategoryUnknown(i.url, i.tags)))
        return Category.UNKNOWN


if __name__ == "__main__":
    c = CasaMexico()
    c.events
    #print(*c._get_items(), sep="\n")
