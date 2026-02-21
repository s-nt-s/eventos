from cloudscraper import create_scraper
from requests import Session
import re
from core.util import get_domain
import logging
from functools import cache
from core.proxy import PM

logger = logging.getLogger(__name__)

re_sp = re.compile(r"\s+")


@cache
def getProxy(dom: str):
    if dom in ("march.es", "giglon.com"): #("madrid.es", ):
        prx = PM.get_proxy()
        if prx:
            logger.info(f"{dom} usará proxy {prx}")
            return prx


def buildSession():
    s = Session()
    _orig = s.request

    def _wrapped(method, url, *a, **kw):
        prx = getProxy(get_domain(url))
        if prx:
            kw.setdefault("proxies", {
                "http": prx,
                "https": prx
            })
        return _orig(method, url, *a, **kw)

    s.request = _wrapped
    return s


def buildScraper():
    s = create_scraper()
    _orig = s.request

    def _wrapped(method, url, *a, **kw):
        prx = getProxy(get_domain(url))
        if prx:
            kw.setdefault("proxies", {
                "http": prx,
                "https": prx
            })
        return _orig(method, url, *a, **kw)

    s.request = _wrapped
    return s
