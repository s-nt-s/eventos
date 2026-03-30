from functools import cache
from typing import Optional
from os import environ
import requests
import logging
from types import MappingProxyType


logger = logging.getLogger(__name__)


class ProxyManager:
    def __init__(self):
        self.__timeout = 1
        self.__proxies = MappingProxyType(self.__get_proxies("PROXY_LIST"))

    def __get_proxies(self, env_name: str):
        prx: dict[str, str] = {}
        val = environ.get(env_name)
        if val is None:
            return prx
        words = val.split()
        for i in range(int(len(words)/2)):
            label = words[i]
            prx[words[i+1]] = label
            logger.info(f"proxy {len(prx)}: {label}")
        return prx

    @cache
    def get_proxy(self):
        for p, lb in self.__proxies.items():
            if self.__check_proxy(p):
                logger.info(f"[OK] {lb}")
                return lb, p
            logger.info(f"[KO] {lb}")

    def __check_proxy(self, proxy: str) -> bool:
        if not self.__check_status(proxy):
            return False
        proxy_ip = self.__get_ip(proxy)
        if proxy_ip is None:
            return False
        real_ip = self.__get_ip()
        if real_ip is None:
            logger.warning("No se pudo obtener la IP real")
            return True
        if real_ip == proxy_ip:
            lb = self.__proxies[proxy]
            logger.debug(f"proxy={lb} no cambia IP")
            return False
        return True

    @cache
    def __check_status(self, proxy: str) -> bool:
        lb = self.__proxies[proxy]
        url = 'https://detectportal.firefox.com/success.txt'
        try:
            r = requests.get(
                url,
                proxies={"http": proxy, "https": proxy},
                timeout=self.__timeout,
            )
            if r.status_code == 200 and r.text.strip() == "success":
                return True
            logger.debug(f"proxy={lb} url={url} -> {r.status_code} {r.text}")
        except requests.RequestException as e:
            logger.debug(f"proxy={lb} url={url} -> {e}")
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
        lb = self.__proxies.get(proxy)
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
                logger.debug(f"proxy={lb} url={url} -> {r.status_code} {r.text}")
            else:
                logger.debug(f"url={url} -> {r.status_code} {r.text}")
        except requests.RequestException as e:
            if proxy:
                logger.debug(f"proxy={lb} url={url} -> {e}")
            else:
                logger.debug(f"url={url} -> {e}")


PM = ProxyManager()

if __name__ == "__main__":
    from core.log import config_log
    config_log("log/proxy.log", log_level=(logging.DEBUG))
    print(PM.get_proxy())
    print(PM.get_proxy())
