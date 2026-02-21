from functools import cache
from bs4 import BeautifulSoup
from typing import Optional
from os import environ
import requests
import re
import logging


logger = logging.getLogger(__name__)


def soup_to_proxy(soup: BeautifulSoup):
    proxies: list[str] = []
    table = soup.select_one("#proxylister-table")
    for tr in table.select("tr"):
        tds = tr.select("td")
        if len(tds) < 3:
            continue
        ip = tds[0].text.strip()
        port = tds[1].text.strip()
        prot = tds[2].text.strip().lower()
        if prot != "http":
            continue
        if not re.match(r"^\d{1,3}(\.\d{1,3}){3}$", ip) or not re.match(r"^\d{1,5}$", port):
            continue
        pr = f"{prot}://{ip}:{port}"
        if pr not in proxies:
            proxies.append(pr)
    return tuple(proxies)


class ProxyManager:
    def __init__(self):
        self.__timeout = 1
        self.__spain_proxy = environ.get("SPAIN_PROXY")
        self.__s = requests.Session()

    def __iter_proxies(self):
        if self.__spain_proxy:
            yield self.__spain_proxy
        for u in (
            'https://proxyelite.info/free/europe/spain/',
            'https://proxyelite.info/free/europe/portugal/',
            'https://proxyelite.info/free/europe/france/',
            'https://proxyelite.info/free/europe/italy/',
        ):
            r = self.__s.get(u)
            soup = BeautifulSoup(
                r.content,
                "html.parser"
            )
            table = soup.select_one("#proxylister-table")
            for tr in table.select("tr"):
                tds = tr.select("td")
                if len(tds) < 3:
                    continue
                ip = tds[0].text.strip()
                port = tds[1].text.strip()
                prot = tds[2].text.strip().lower()
                if prot != "http":
                    continue
                if not re.match(r"^\d{1,3}(\.\d{1,3}){3}$", ip) or not re.match(r"^\d{1,5}$", port):
                    continue
                pr = f"{prot}://{ip}:{port}"
                yield pr

    def iter_proxies(self):
        done: set[str] = set()
        for p in self.__iter_proxies():
            if p not in done:
                done.add(p)
                yield p

    @cache
    def get_proxy(self):
        for p in self.iter_proxies():
            if self.__check_proxy(p):
                logger.info(f"[OK] {p}")
                return p
            logger.info(f"[KO] {p}")

    def __check_proxy(self, proxy: str) -> bool:
        if not self.__check_status(proxy):
            return False
        if proxy == self.__spain_proxy:
            return True
        real_ip = self.__get_ip()
        proxy_ip = self.__get_ip(proxy)
        if None in (real_ip, proxy_ip):
            return False
        if real_ip == proxy_ip:
            logger.debug(f"proxy={proxy} no cambia IP")
            return False
        return True

    @cache
    def __check_status(self, proxy: str) -> bool:
        url = 'https://detectportal.firefox.com/success.txt'
        try:
            r = requests.get(
                url,
                proxies={"http": proxy, "https": proxy},
                timeout=self.__timeout,
            )
            if r.status_code == 200 and r.text.strip() == "success":
                return True
            logger.debug(f"proxy={proxy} url={url} -> {r.status_code} {r.text}")
        except requests.RequestException as e:
            logger.debug(f"proxy={proxy} url={url} -> {e}")
        return False

    @cache
    def __get_ip(self, proxy: Optional[str] = None) -> str | None:

        def _get_origin(r: requests.Response) -> Optional[str]:
            if r.status_code != 200:
                return None
            try:
                js = r.json()
            except requests.exceptions.JSONDecodeError:
                return None
            if not isinstance(js, dict):
                return None
            o = js.get("origin")
            if isinstance(o, str):
                o = o.strip()
                if len(o) > 0:
                    return o

        url = 'https://httpbin.org/ip'
        proxies = {"http": proxy, "https": proxy} if proxy else None
        try:
            r = requests.get(
                url,
                proxies=proxies,
                timeout=self.__timeout,
            )
            ip = _get_origin(r)
            if ip is not None:
                return ip
            if proxy:
                logger.debug(f"proxy={proxy} url={url} -> {r.status_code} {r.text}")
            else:
                logger.debug(f"url={url} -> {r.status_code} {r.text}")
        except requests.RequestException as e:
            if proxy:
                logger.debug(f"proxy={proxy} url={url} -> {e}")
            else:
                logger.debug(f"url={url} -> {e}")


PM = ProxyManager()

if __name__ == "__main__":
    from core.log import config_log
    config_log("log/proxy.log", log_level=(logging.DEBUG))
    print(PM.get_proxy())
    print(PM.get_proxy())
