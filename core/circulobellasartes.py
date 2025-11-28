from core.web import Web, get_text
from functools import cached_property
from core.event import Event, Cinema, Places, Category
import re
from datetime import date

re_date = re.compile(r"^\d{1,2}[/\.]\d{1,2}[/\.]20\d{2}$")
TODAY = date.today()


def _det_date(s: str):
    if s is None:
        return None
    if not re_date.match(s):
        return None
    d, m, y = tuple(map(int, re.findall(r"\d+", s)))
    return date(y, m, d)


class CirculoBellasArtes:
    URL_PELICULAS = "https://www.circulobellasartes.com/ciclos-cine/peliculas/"

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
            if url.startswith(CirculoBellasArtes.URL_PELICULAS):
                soup = self.__w.get_cached_soup(url)
                if not soup.find(string=re.compile(r"^\s*Este\s+evento\s+ha\s+finalizado\s*$")):
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
    def events(self):
        evs: set[Event] = set()
        for url in self.urls:
            ev = self.__get_event_from_url(url)
            if ev:
                evs.add(ev)
        return tuple(sorted(evs))

    def __get_event_from_url(self, url: str):
        if url.startswith(CirculoBellasArtes.URL_PELICULAS):
            return self.__get_event_from_url_cine(url)
        soup = self.__w.get_cached_soup(url)
        ev = Event(
            url=url,
            name=get_text(soup.select_one("div[data-post-id] h1")),
            place=Places.CIRCULO_BELLAS_ARTES.value
        )

    def __get_event_from_url_cine(self, url: str):
        soup = self.__w.get_cached_soup(url)
        h1 = soup.select_one("div[data-post-id] h1")
        h3 = h1
        while h3 and h3.name != "h3":
            h3 = h3.find_parent("div")
            aux = h3.select_one("h3")
            if aux:
                h3 = aux
        ev = Cinema(
            url=url,
            name=get_text(h1),
            director=(get_text(h3),),
            price=8,
            place=Places.CIRCULO_BELLAS_ARTES.value,
            category=Category.CINEMA,
        )

if __name__ == "__main__":
    c = CirculoBellasArtes()
    print(*c.urls, sep="\n")