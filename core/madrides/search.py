from core.web import Web, WebException, Driver, get_text, get_query, buildSoup
from urllib.parse import urljoin
from bs4 import Tag, BeautifulSoup
import re
from core.util import plain_text, get_domain
import logging
from functools import cached_property, cache
from types import MappingProxyType
from typing import NamedTuple
from core.fetcher import Getter, rq_to_text
from core.util import normalize_url
from urllib.parse import urlencode


logger = logging.getLogger(__name__)
re_sp = re.compile(r"\s+")


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


class FormSearchResult(NamedTuple):
    vgnextoid: str
    a: Tag
    div: Tag


class IndexSearch(NamedTuple):
    total: int
    urls: tuple[str, ...]


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
        self.__getter = Getter(
            onread=rq_to_text,
            headers=self.__w.s.headers,
            cookie_jar=self.__w.s.cookies
        )
        self.distritos = MappingProxyType(self.__get_options("#distrito"))
        self.usuarios = MappingProxyType(self.__get_options("#usuario"))
        self.tipos = MappingProxyType(self.__get_tipos())
        self.__cached_soup: dict[str, BeautifulSoup] = {}

    def preload(self):
        logger.info("INI: FormSearch.preload()")
        arr_kwargs: list[dict] = []
        for k in self.usuarios.keys():
            arr_kwargs.append({"usuario": k})
        for k in self.tipos.keys():
            arr_kwargs.append({"tipo": k})
        for k in self.distritos.keys():
            arr_kwargs.append({"distrito": k})
        urls: set[str] = set()
        for kw in arr_kwargs:
            s_url = self.build_search_url(**kw)
            urls.add(s_url)
        self.__add_cached_soup(*urls)

        urls: set[str] = set()
        for kw in arr_kwargs:
            urls.update(self.get_index_search(**kw).urls)
        self.__add_cached_soup(*urls)
        logger.info("FIN: FormSearch.preload()")

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

    def __get_cached_soup(self, url: str) -> BeautifulSoup:
        if url not in self.__cached_soup:
            self.__cached_soup[url] = self.get(url)
        return self.__cached_soup[url]

    def __add_cached_soup(self, *urls: str) -> BeautifulSoup:
        urls = sorted(set(urls).difference(self.__cached_soup.keys()))
        if len(urls) == 0:
            return None
        if len(urls) == 1:
            self.__get_cached_soup(urls[0])
            return
        for u, b in self.__getter.get(*urls).items():
            soup = buildSoup(u, b)
            self.__cached_soup[u] = soup

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

    @cache
    def get_vgnextoid(self, **kwargs):
        results = self.get_results(**kwargs)
        return tuple(sorted(set(r.vgnextoid for r in results)))

    def __get_index_search(self, url: str):
        urls = [url]
        soup = self.__get_cached_soup(url)
        total = int(get_text(soup.select_one(".results-total strong")) or "0")
        if total == 0:
            return IndexSearch(
                total=total,
                urls=tuple(urls),
            )
        slc_size = ".results-displayed strong"
        text = get_text(soup.select_one(".results-displayed strong"))
        if text is None:
            raise WebException(f"{slc_size} NOT FOUND in {url}")
        m = re.match(r"^\s*1\s*-\s*(\d+)\s*$", text)
        if m is None:
            raise WebException(f"{slc_size} INVALID FORMAT '{text}' in {url}")
        page_size = int(m.group(1))
        if page_size > total:
            raise WebException(f"{slc_size} TOO BIG {page_size} > {total} in {url}")
        if page_size == total:
            return IndexSearch(
                total=total,
                urls=tuple(urls),
            )
        slc_a_next = "li.next a.pagination-text"
        a_next = soup.select_one(slc_a_next)
        if a_next is None:
            raise WebException(f"{slc_a_next} NOT FOUND in {url}")
        href = a_next.attrs["href"]
        onclick = a_next.attrs.get("onclick")
        if isinstance(onclick, str):
            m = re.match(
                r".*ajaxDivChange\(['\"]([^'\"]+).*",
                onclick
            )
            if m:
                href = urljoin(url, m.group(1))
        pages = ((total - 1) // page_size + 1)
        href = normalize_url(href, "page")
        urls.append(href)
        while len(urls) < pages:
            last_url = urls[-1]
            next_page = int(re.search(r"page=(\d+)", last_url).group(1)) + 1
            next_url = re.sub(r"page=\d+", f"page={next_page}", last_url)
            urls.append(next_url)
        return IndexSearch(
            total=total,
            urls=tuple(urls),
        )

    def build_search_url(self, **kwargs):
        action, action_data = self.__prepare_search()
        for k, v in action_data.items():
            if k not in kwargs:
                kwargs[k] = v
        url = action + '?' + urlencode(kwargs)
        return url

    @cache
    def get_index_search(self, **kwargs):
        url = self.build_search_url(**kwargs)
        return self.__get_index_search(url)

    @cache
    def get_results(self, **kwargs):
        inx = self.get_index_search(**kwargs)

        def _get_items():
            rt_arr: list[Tag] = []
            self.__add_cached_soup(*inx.urls)
            for u in inx.urls:
                soup = self.__get_cached_soup(u)
                arr = soup.select(slc_result)
                for div in arr:
                    rt_arr.append(div)
            total_get = len(rt_arr)
            if total_get == inx.total:
                logger.debug(f"{total_get} div en {inx.urls[0]}")
            else:
                logger.warning(f"TOTAL MISMATCH {total_get} != {inx.total} en {inx.urls[0]}")
            return rt_arr

        slc_result = "#listSearchResults ul.events-results li div.event-info"

        rt_arr: dict[str, FormSearchResult] = {}
        for div in _get_items():
            a = div.select_one("a.event-link")
            vgnextoid = get_vgnextoid(a)
            if vgnextoid is None:
                continue
            rt_arr[vgnextoid] = FormSearchResult(
                vgnextoid=vgnextoid,
                a=a,
                div=div
            )
        logger.info(f"{len(rt_arr)} ids en {inx.urls[0]}")
        return tuple(rt_arr.values())

    @cached_property
    def zona(self):
        data: dict[str, str] = {}
        for k, v in self.distritos.items():
            if re.search(r"arganzuela|centro|moncloa|chamberi|retiro|salamaca|villaverde|carabanchel", plain_text(v)):
                data[k] = v
        return data

    def __get_options(self, slc: str):
        data: dict[str, str] = {}
        soup = self.get(FormSearch.AGENDA)
        for o in soup.select(slc+" option"):
            k = o.attrs["value"]
            v = re_sp.sub(" ", o.get_text()).strip()
            if k != "-1":
                data[k] = v
        return data

    def __get_tipos(self):
        data: dict[str, str] = {}
        soup = self.get(FormSearch.TAXONOMIA, parser="xml")
        for n in soup.find_all('item'):
            value = n.find('value').string.strip()
            text = re_sp.sub(" ", n.find('text').string).strip()
            data[value] = text
        return data


if __name__ == "__main__":
    from core.log import config_log
    config_log("log/search.log")
    from core.madrides.api import ApiMadridEs
    logger.info("INI: API 1")
    AP = ApiMadridEs()
    logger.info("INI: API 2")
    api_urls = {get_vgnextoid(r.url): r.url for r in AP.get_events()}
    logger.info("FIN: API")
    logger.info("INI: FORM 1")
    FS = FormSearch()
    FS.preload()
    logger.info("INI: FORM 2")
    frm_urls = {r.vgnextoid: r.a.attrs["href"] for r in FS.get_results()}
    logger.info("FIN: FORM")
    ko_api = sorted(set(frm_urls.keys()) - set(api_urls.keys()))
    ko_frm = sorted(set(api_urls.keys()) - set(frm_urls.keys()))
    for k in ko_api:
        continue
        print(frm_urls[k])
    for k in ko_frm:
        continue
        print(api_urls[k])
