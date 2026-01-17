from core.web import Web, WebException, Driver, get_text, get_query, buildSoup
from urllib.parse import urljoin
from bs4 import Tag, BeautifulSoup
import re
import logging
from functools import cache
from types import MappingProxyType
from typing import NamedTuple
from core.fetcher import Getter
from core.util import normalize_url, get_obj
from urllib.parse import urlencode
from typing import Optional, Iterable
from aiohttp import ClientResponse
from core.cache import TupleCache
from core.madrid_es.tp import Place
from html import unescape
from icalendar import Calendar
from core.ics import IcsEventWrapper
from core.util import KO_IMG, KO_MORE, get_domain


logger = logging.getLogger(__name__)
re_sp = re.compile(r"\s+")

KO_URL = KO_MORE + KO_IMG


def get_vgnextoid(url: str | Tag):
    if isinstance(url, Tag):
        url = url.attrs.get("href")
    if url is None:
        return None
    if not isinstance(url, str):
        raise ValueError(url)
    url = url.strip()
    if len(url) == 0 or get_domain(url) != "madrid.es":
        return None
    qr = get_query(url)
    id = qr.get("vgnextoid")
    if not isinstance(id, str):
        return None
    id = id.strip()
    if len(id) == 0:
        return None
    return id


class Item(NamedTuple):
    vgnextoid: str
    url: str
    title: str
    place: Optional[Place] = None
    audience: tuple[str, ...] = tuple()
    category: tuple[str, ...] = tuple()
    free: bool = False

    @staticmethod
    def build(*args, **kwargs):
        obj = get_obj(*args, **kwargs)
        if obj is None:
            return None
        for k, v in list(obj.items()):
            if isinstance(v, list):
                obj[k] = tuple(v)
            if k == "place" and isinstance(v, dict):
                obj[k] = Place(**v)
        return Item(**obj)


class Index(NamedTuple):
    total: int
    urls: tuple[str, ...]
    items: tuple[Item, ...] = tuple()


class Page(NamedTuple):
    description: Tag
    free: bool
    price: tuple[str, ...] = tuple()
    more: tuple[str, ...] = tuple()
    img: tuple[str, ...] = tuple()


def _get_district(lc: Tag):
    if lc is None:
        return None
    text = get_text(lc)
    if text is None:
        return None
    tail = text.rsplit(".", 1)[-1].strip()
    if len(tail) > 0 or tail.upper() == tail:
        return tail.title()
    for k, v in {
        "(Retiro)": "Retiro",
    }.items():
        if text.endswith(k):
            return v
    raise ValueError(f"Distrito no encontrado en {lc}")


def _get_attr(lc: Tag, attr: str, cast=None):
    v = lc.attrs.get(attr)
    if isinstance(v, str):
        v = v.strip()
        if len(v) == 0:
            return None
    if v is None:
        return None
    if cast:
        v = cast(v)
    return v


def tag_to_location(lc: Tag):
    if lc is None:
        return None
    address = _get_attr(lc, "data-direction")
    latitude = _get_attr(lc, "data-latitude", cast=float)
    longitude = _get_attr(lc, "data-longitude", cast=float)
    if None in (latitude, longitude):
        latitude = None
        longitude = None
        if address is None:
            return None

    return Place(
        latitude=latitude,
        longitude=longitude,
        location=_get_attr(lc, "title"),
        address=address,
        district=_get_district(lc)
    )


def soup_to_items(soup: BeautifulSoup):
    items: set[Item] = set()
    for div in soup.select("#listSearchResults ul.events-results li div.event-info"):
        a = div.select_one("a.event-link")
        vgnextoid = get_vgnextoid(a)
        if vgnextoid is None:
            continue
        lc = div.select_one("a.event-location")
        pl = tag_to_location(lc)
        it = Item(
            vgnextoid=vgnextoid,
            url=a.attrs['href'],
            title=get_text(a),
            place=pl
        )
        items.add(it)
    return tuple(items)


async def rq_to_items(r: ClientResponse):
    soup = buildSoup(str(r.url), await r.read())
    items = soup_to_items(soup)
    return items


def _get_urls(soup: Tag, slc: str):
    obj = {
        "a": "href",
        "img": "src",
    }
    urls: list[str] = []
    for x in soup.select(slc):
        attr = obj[x.name]
        val = x.attrs.get(attr)
        if val not in urls and isinstance(val, str):
            val = val.strip()
            prc = val.split("://", 1)[0].lower()
            if prc in ("http", "https") and not val.startswith("https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/Actividades-en-"):
                dom = get_domain(val)
                if val not in KO_URL and dom not in KO_URL:
                    urls.append(val)
    return tuple(urls)


async def rq_to_page(r: ClientResponse):
    soup = buildSoup(str(r.url), await r.read())
    description = soup.select_one("div.tramites-content div.tiny-text")
    more = _get_urls(
        soup,
        "div.tramites-content a[href]",
    )
    img = _get_urls(
        soup,
        "div.image-content img, div.tramites-content div.tiny-text img, div.detalle img",
    )

    price: list[str] = []
    for p in map(get_text, soup.select("div.tramites-content, #importeVenta p")):
        if p is not None and p not in price:
            price.append(p)
    free = soup.select_one("ul li p.gratuita") is not None

    return Page(
        description=description,
        price=tuple(price),
        more=more,
        img=img,
        free=free
    )


async def rq_to_ics(r: ClientResponse):
    txt = await r.text()
    if len(re_sp.sub("", txt)) == 0:
        logger.critical(f"Calendario vació {r.url}")
        return tuple()
    lines = txt.splitlines()
    for i, ln in enumerate(lines):
        m = re.match(r"^\s*(DTSTAMP|DTSTART|DTEND)\s*:\s*(\d+T[\d:Z]+)\s*$", ln)
        if m:
            k, v = m.groups()
            if re.match(r"^\d{8}T\d\d:\d\d:\d\dZ$", v):
                v = v.replace(":", "")[:-1]
                lines[i] = f"{k}:{v}"
    txt = "\n".join(lines)
    arr: list[IcsEventWrapper] = []
    try:
        cal = Calendar.from_ical(txt)
        for e in cal.walk("VEVENT"):
            e = IcsEventWrapper(e, source=r.url)
            arr.append(e)
    except Exception as e:
        logger.critical(f"Calendario erróneo {r.url} {e}", exc_info=True)
    return tuple(arr)


async def rq_to_index(r: ClientResponse):
    soup = buildSoup(str(r.url), await r.read())
    total = int(get_text(soup.select_one(".results-total strong")) or "0")
    if total == 0:
        return Index(
            total=total,
            urls=tuple(),
        )
    slc_size = ".results-displayed strong"
    text = get_text(soup.select_one(".results-displayed strong"))
    if text is None:
        raise WebException(f"{slc_size} NOT FOUND in {r.url}")
    m = re.match(r"^\s*1\s*-\s*(\d+)\s*$", text)
    if m is None:
        raise WebException(f"{slc_size} INVALID FORMAT '{text}' in {r.url}")
    page_size = int(m.group(1))
    if page_size > total:
        raise WebException(f"{slc_size} TOO BIG {page_size} > {total} in {r.url}")
    items = soup_to_items(soup)
    if page_size == total:
        return Index(
            total=total,
            items=items,
            urls=tuple(),
        )
    slc_a_next = "li.next a.pagination-text"
    a_next = soup.select_one(slc_a_next)
    if a_next is None:
        raise WebException(f"{slc_a_next} NOT FOUND in {r.url}")
    href = a_next.attrs["href"]
    onclick = a_next.attrs.get("onclick")
    if isinstance(onclick, str):
        m = re.match(
            r".*ajaxDivChange\(['\"]([^'\"]+).*",
            onclick
        )
        if m:
            href = urljoin(str(r.url), m.group(1))
    pages = ((total - 1) // page_size + 1)
    href = normalize_url(href, "page")
    urls = [href]
    while len(urls) < pages:
        last_url = urls[-1]
        next_page = int(re.search(r"page=(\d+)", last_url).group(1)) + 1
        next_url = re.sub(r"page=\d+", f"page={next_page}", last_url)
        urls.append(next_url)
    return Index(
        total=total,
        items=items,
        urls=tuple(urls),
    )


class IdGetter(Getter):
    def __init__(self, onread, headers = None, cookie_jar = None):
        super().__init__(onread, headers, cookie_jar)
        self.__cache = {}

    def get(self, *urls: str):
        if len(urls) == 0:
            return MappingProxyType({})
        url_id: dict[str, str] = {}
        for url in sorted(set(urls)):
            k = get_vgnextoid(url)
            if k is None:
                raise ValueError(f"vgnextoid not in {url}")
            if k in self.__cache:
                data[k] = self.__cache[k]
                continue
            if k in url_id:
                raise ValueError(f"vgnextoid {k} duplicates")
            url_id[url] = k
        data: dict[str] = {}
        for k, v in super().get(*url_id.keys()).items():
            k = url_id[k]
            self.__cache[k] = v
            data[k] = v
        return MappingProxyType(data)


class FormSearch:
    AGENDA = "https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/?vgnextfmt=default&vgnextchannel=ca9671ee4a9eb410VgnVCM100000171f5a0aRCRD"
    TAXONOMIA = "https://www.madrid.es/ContentPublisher/jsp/apl/includes/XMLAutocompletarTaxonomias.jsp?taxonomy=/contenido/actividades&idioma=es&onlyFirstLevel=true"

    def __init__(self):
        self.__w = Web()
        self.__w.s = Driver.to_session(
            "firefox",
            "https://www.madrid.es",
            session=self.__w.s,
        )
        self.__getter_index = Getter(
            onread=rq_to_index,
            headers=self.__w.s.headers,
            cookie_jar=self.__w.s.cookies
        )
        self.__getter_items = Getter(
            onread=rq_to_items,
            headers=self.__w.s.headers,
            cookie_jar=self.__w.s.cookies
        )
        self.__getter_page = IdGetter(
            onread=rq_to_page,
            headers=self.__w.s.headers,
            cookie_jar=self.__w.s.cookies
        )
        self.__getter_ics = IdGetter(
            onread=rq_to_ics,
            headers=self.__w.s.headers,
            cookie_jar=self.__w.s.cookies
        )
        self.usuarios = MappingProxyType(self.__get_options("#usuario"))
        self.tipos = MappingProxyType(self.__get_tipos())
        self.__loaded = False
        self.__all: tuple[Item, ...] = tuple()
        self.__free: tuple[str, ...] = tuple()
        self.__user: MappingProxyType[str, tuple[str, ...]] = {}
        self.__typ: MappingProxyType[str, tuple[str, ...]] = {}

    def __load(self):
        if self.__loaded:
            return
        logger.info("INI: FormSearch.preload()")
        ALL = self.build_search_url()
        FREE = self.build_search_url(gratuita="1")
        USER = {}
        TYP = {}
        for k in self.usuarios.keys():
            USER[k] = self.build_search_url(usuario=k)
        for k in self.tipos.keys():
            TYP[k] = self.build_search_url(tipo=k)

        urls: set[str] = set({
            ALL,
            FREE,
            *USER.values(),
            *TYP.values()
        })
        url_index: dict[str, Index] = self.__getter_index.get(*urls)
        pages: set[str] = set()
        for i in url_index.values():
            pages.update(i.urls)
        url_items: dict[str, tuple[Item, ...]] = self.__getter_items.get(*pages)

        def _get_items(u: str):
            index = url_index[u]
            items = set(index.items)
            for x in index.urls:
                items.update(url_items[x])
            size = len(items)
            if size != index.total:
                logger.critical(f"{size} != {index.total} items {u}")
            return tuple(items)

        user_items: dict[str, tuple[Item, ...]] = {}
        typ_items: dict[str, tuple[Item, ...]] = {}
        all_items = set(_get_items(ALL))
        free_items = _get_items(FREE)
        all_items.update(free_items)
        for k, u in USER.items():
            user_items[k] = _get_items(u)
            all_items.update(user_items[k])
        for k, u in TYP.items():
            typ_items[k] = _get_items(u)
            all_items.update(free_items)

        def _to_ids(arr: Iterable[Item]):
            return tuple(get_vgnextoid(i.url) for i in arr)

        dupes: dict[str, int] = {}
        for e in all_items:
            dupes[e.vgnextoid] = dupes.get(e.vgnextoid, 0) + 1
        dupes = sorted(k for k, v in dupes.items() if v > 1)
        if dupes:
            raise ValueError(f"ids duplicates: {', '.join(dupes)}")

        self.__all = tuple(all_items)
        self.__free = _to_ids(free_items)
        self.__user = MappingProxyType({
            k: _to_ids(items)
            for k, items in
            user_items.items()
        })
        self.__typ = MappingProxyType({
            k: _to_ids(items)
            for k, items in
            typ_items.items()
        })
        logger.info("FIN: FormSearch.preload()")
        self.__loaded = True

    def get(self, url, *args, **kwargs) -> BeautifulSoup:
        if self.__w.url != url:
            logger.debug(url)
            self.__w.get(url, *args, **kwargs)
        title = get_text(self.__w.soup.select_one("title"))
        if title == "Access Denied":
            body = get_text(self.__w.soup.select_one("body"))
            body = re.sub(r"^Access Denied\s+", "", body or "")
            raise ValueError(f"{url} {title} {body}".strip())
        return self.__w.soup

    @cache
    def __prepare_search(self):
        self.get(FormSearch.AGENDA)
        action, data = self.__w.prepare_submit("#generico1", enviar="buscar")
        if action is None:
            raise WebException(f"#generico1 NOT FOUND in {self.__w.url}")
        for k in ("gratuita", "movilidad"):
            if k in data:
                del data[k]
        data["tipo"] = "-1"
        data["distrito"] = "-1"
        data["usuario"] = "-1"
        return action, MappingProxyType(data)

    def build_search_url(self, **kwargs):
        action, action_data = self.__prepare_search()
        for k, v in action_data.items():
            if k not in kwargs:
                kwargs[k] = v
        url = action + '?' + urlencode(kwargs)
        return url

    def __get_options(self, slc: str):
        data: dict[str, str] = {}
        soup = self.get(FormSearch.AGENDA)
        for o in soup.select(slc+" option"):
            k = o.attrs["value"]
            v = re_sp.sub(" ", o.get_text()).strip()
            if k != "-1":
                data[k] = unescape(v)
        return data

    def __get_tipos(self):
        data: dict[str, str] = {}
        soup = self.get(FormSearch.TAXONOMIA, parser="xml")
        for n in soup.find_all('item'):
            value = n.find('value').string.strip()
            text = re_sp.sub(" ", n.find('text').string).strip()
            data[value] = unescape(text)
        return data

    @TupleCache("rec/apimadrides/form.json", builder=Item.build)
    def get_events(self):
        self.__load()
        items: set[Item] = set()
        for i in self.__all:
            vid = get_vgnextoid(i.url)
            aud: set[str] = set()
            category: set[str] = set()
            for k, v in self.usuarios.items():
                if vid in self.__user[k]:
                    aud.add(v)
            for k, v in self.tipos.items():
                if vid in self.__typ[k]:
                    category.add(v)
            i = i._replace(
                free=vid in self.__free,
                audience=tuple(sorted(aud)),
                category=tuple(sorted(category))
            )
            items.add(i)
        return tuple(sorted(items))

    def get_page(self, *urls: str) -> MappingProxyType[str, Page]:
        return self.__getter_page.get(*urls)

    def get_ics(self, *ids: str) -> MappingProxyType[str, tuple[IcsEventWrapper, ...]]:
        urls: set[str] = set()
        for i in ids:
            urls.add(f"https://www.madrid.es/ContentPublisher/jsp/cont/microformatos/obtenerVCal.jsp?vgnextoid={i}")
        return self.__getter_ics.get(*urls)

if __name__ == "__main__":
    from core.log import config_log
    config_log("log/search.log")
    FS = FormSearch()
    evs = FS.get_events()
    print(len(evs))
