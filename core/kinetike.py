from core.web import Web, Driver
from functools import cached_property
from urllib.parse import urljoin
import re


class KineTike:
    SALA_EQUIS = "INTERNETm4czuhkoa622"
    ERROR_URL = "https://kinetike.com:83/views/error.aspx"

    def __init__(self, sala: str):
        self.__root = f"https://kinetike.com:83/views/peliculas.aspx?s={sala}"
        self.__w = Web()

    def __get(self, url: str):
        soup = self.__w.get(url)
        if self.__w.url == KineTike.ERROR_URL:
            raise ValueError(self.__w.soup.get_text(strip=True))
        return soup

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


if __name__ == "__main__":
    k = KineTike(KineTike.SALA_EQUIS)
    for url in k.urls:
        print(url)
