from cloudscraper import create_scraper
from requests import Session
from os import environ
from typing import NamedTuple
import re
from core.util import get_domain
import logging
from functools import cache

logger = logging.getLogger(__name__)

re_sp = re.compile(r"\s+")


class MyProxy(NamedTuple):
    host: str
    port: int
    user: str
    pssw: str

    def __str__(self):
        if self.user:
            return f"http://***:***@{self.host}:{self.port}"
        return f"http://{self.host}:{self.port}"

    def get_full_url(self):
        if self.user:
            return f"http://{self.user}:{self.pssw}@{self.host}:{self.port}"
        return f"http://{self.host}:{self.port}"

    @classmethod
    def build(cls, proxy: str = None):
        if not isinstance(proxy, str):
            return None
        proxy = re_sp.sub(" ", proxy).strip()
        proxy = re.sub(r"^https?://", "", proxy)
        fields = re.split(r":|@", proxy)
        if len(fields) not in (2, 4):
            raise ValueError(proxy)
        if len(fields) == 2:
            fields = [None, None] + fields
        user, pssw, host, port = fields
        if not port.isdecimal():
            raise ValueError(proxy)
        return cls(
            host=host,
            port=int(port),
            user=user,
            pssw=pssw
        )


SPAIN_PROXY = MyProxy.build(environ.get("SPAIN_PROXY"))


@cache
def getProxy(dom: str):
    if SPAIN_PROXY and dom:
        if dom in ("madrid.es", ):
            logger.info(f"{dom} usará SPAIN_PROXY")
            return SPAIN_PROXY


def buildSession():
    s = Session()
    _orig = s.request

    def _wrapped(method, url, *a, **kw):
        prx = getProxy(get_domain(url))
        if prx:
            kw.setdefault("proxies", {
                "http": prx.get_full_url(),
                "https": prx.get_full_url()
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
                "http": prx.get_full_url(),
                "https": prx.get_full_url()
            })
        return _orig(method, url, *a, **kw)

    s.request = _wrapped
    return s
