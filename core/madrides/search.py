from core.web import Web, WebException, Driver, get_text, get_query
from urllib.parse import urljoin
from bs4 import Tag, BeautifulSoup
import re
from core.util import plain_text, get_domain
import logging
from functools import cached_property, cache
from types import MappingProxyType
from typing import NamedTuple
from core.fetcher import Getter
from url_normalize import url_normalize
from urllib.parse import urlparse, parse_qsl, urlencode, urlunparse

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


def normalize_url_with_param_last(url: str, param: str) -> str:
    norm_url = url_normalize(url)
    parsed = urlparse(norm_url)
    query_params = parse_qsl(parsed.query, keep_blank_values=True)

    new_params = [p for p in query_params if p[0] != param]
    if param in dict(query_params):
        new_params.append((param, dict(query_params)[param]))

    new_query = urlencode(new_params)
    return urlunparse(parsed._replace(query=new_query))


class FormSearchResult(NamedTuple):
    vgnextoid: str
    a: Tag
    div: Tag


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
            headers=self.__w.s.headers,
            cookie_jar=self.__w.s.cookies
        )
        self.distritos = MappingProxyType(self.__get_options("#distrito"))
        self.usuarios = MappingProxyType(self.__get_options("#usuario"))
        self.distritos = MappingProxyType(self.__get_options("#distrito"))
        self.tipos = MappingProxyType(self.__get_tipos())

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
        return action, data

    @cache
    def get_vgnextoid(self, **kwargs):
        results = self.get_results(**kwargs)
        return tuple(sorted(set(r.vgnextoid for r in results)))

    @cache
    def get_results(self, **kwargs):
        action, action_data = self.__prepare_search()

        def _get(url: str):
            soup = self.get(url)
            arr = soup.select("#listSearchResults ul.events-results li div.event-info")
            a_next = soup.select_one("li.next a.pagination-text")
            logger.debug(f"{len(arr)} en {url}")
            if a_next is None:
                return None, arr
            href = a_next.attrs["href"]
            onclick = a_next.attrs.get("onclick")
            if isinstance(onclick, str):
                m = re.match(
                    r".*ajaxDivChange\(['\"]([^'\"]+).*",
                    onclick
                )
                if m:
                    href = urljoin(url, m.group(1))
            href = normalize_url_with_param_last(href, "page")
            return href, arr

        for k, v in action_data.items():
            if k not in kwargs:
                kwargs[k] = v

        start_url = action + '?' + urlencode(kwargs)
        rt_arr: dict[str, FormSearchResult] = {}
        url = str(start_url)
        while url:
            url, arr = _get(url)
            for div in arr:
                a = div.select_one("a.event-link")
                vgnextoid = get_vgnextoid(a)
                if vgnextoid is None:
                    continue
                rt_arr[vgnextoid] = FormSearchResult(
                    vgnextoid=vgnextoid,
                    a=a,
                    div=div
                )
        logger.debug(f"{len(rt_arr)} TOTAL en {start_url}")
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
    logger.info("INI: FORM 2")
    frm_urls = {r.vgnextoid: r.a.attrs["href"] for r in FS.get_results()}
    logger.info("FIN: FORM")
    ko_api = sorted(set(frm_urls.keys()) - set(api_urls.keys()))
    ko_frm = sorted(set(api_urls.keys()) - set(frm_urls.keys()))
    for k in ko_api:
        print(frm_urls[k])
    for k in ko_frm:
        print(api_urls[k])
