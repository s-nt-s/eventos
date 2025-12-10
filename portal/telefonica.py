from core.web import Web, get_text
from core.cache import TupleCache, Cache
from typing import Set, Dict, List
from functools import cached_property
import logging
from core.event import Event, Places, Session, Category, CategoryUnknown, FieldUnknown
from datetime import datetime
from core.util import plain_text, re_or, get_a_href, to_uuid
import re

logger = logging.getLogger(__name__)
NOW = datetime.now()


class Telefonica(Web):
    URL = "https://espacio.fundaciontelefonica.com/events/lista/pagina"
    slc_data1 = "script:not(.aioseo-schema)[type='application/ld+json']"
    slc_data2 = "script.aioseo-schema[type='application/ld+json']"

    def __init__(self, refer=None, verify=True):
        super().__init__(refer, verify)
        self.s.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/123.0.0.0 Safari/537.36"
            ),
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;"
                "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
            ),
            "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
            "Accept-Encoding": "gzip, deflate",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1"
        })

    def get(self, url, auth=None, parser="lxml", **kwargs):
        if url == self.url:
            return self.soup
        logger.debug(url)
        return super().get(url, auth, parser, **kwargs)

    @cached_property
    def url_events(self):
        urls: List[str] = []
        size = -1
        i = 0
        while len(urls) != size:
            i = i + 1
            url = Telefonica.URL + f"/{i}/"
            size = len(urls)
            self.get(url)
            for href in map(get_a_href, self.soup.select("a.tribe-events-calendar-list__event-title-link")):
                if href and href not in urls:
                    urls.append(href)
        return tuple(urls)

    @property
    @TupleCache("rec/telefonica.json", builder=Event.build)
    def events(self):
        logger.info("Telefonica: Buscando eventos")
        events: Set[Event] = set()
        for url in self.url_events:
            ev = self.__url_to_event(url)
            if ev:
                events.add(ev)
        return tuple(sorted(events))

    @Cache("rec/telefonica/{}data.json")
    def __get_script_data(self, url: str):
        self.get(url)
        error = []
        slc_error1 = f"{Telefonica.slc_data1} is not a Tuple[Dict]"
        slc_error2 = f"{Telefonica.slc_data2} is not a Dict['@graph, List[Dict[type, WebPage]]]"
        data1 = self.select_one_json(Telefonica.slc_data1)
        data2 = self.select_one_json(Telefonica.slc_data2)
        if not (isinstance(data1, list) and len(data1) == 1 and isinstance(data2, dict)):
            error.append(slc_error1)
        g = data2.get('@graph') if isinstance(data2, dict) else None
        if not (isinstance(g, list) and len(g) > 1):
            error.append(slc_error2)
        else:
            webpage = [i for i in g if isinstance(i, dict) and i.get('@type') == 'WebPage']
            if len(webpage) != 1:
                error.append(slc_error2)
        data = {
            Telefonica.slc_data1: data1,
            Telefonica.slc_data2: data2,
        }
        if len(error) > 0:
            data['error'] = error
        return data

    def get_script_data(self, url: str):
        data = self.__get_script_data(url)
        error = data.get('error')
        if error:
            raise FieldUnknown(self.url, "script data", error)
        event: Dict = data[Telefonica.slc_data1][0]
        graph: List[Dict] = data[Telefonica.slc_data2]['@graph']
        webpage = [i for i in graph if isinstance(i, dict) and i.get('@type') == 'WebPage'][0]
        return event, webpage

    def __url_to_event(self, url: str):
        self.get(url)
        if self.soup.select_one("div.participar") and not self.soup.select_one("div.participar a.reservabtn"):
            logger.warning(f"{url} no tiene reservas")
            return None
        data, webpage = self.get_script_data(url)

        duration, session = self.__get_session(data)
        if duration > (60*24):
            return None
        name = get_text(self.soup.select_one("span.titulo"))
        return Event(
            id="tl"+to_uuid(url),
            url=url,
            name=name or data['name'],
            img=data['image'],
            price=0,
            category=self.__find_category(data, webpage),
            duration=duration,
            sessions=(session,),
            place=self.__find_place(),
        )

    def __get_session(self, data: Dict):
        ini = datetime.fromisoformat(data['startDate'])
        fin = datetime.fromisoformat(data['endDate'])
        start = ini.strftime("%Y-%m-%d %H:%M")
        duration = int((fin - ini).total_seconds() / 60)
        return duration, Session(date=start)

    def __find_place(self):
        dir = self.select_one_txt("span.direccion")
        if dir == "C/ Fuencarral, 3, Madrid":
            return Places.FUNDACION_TELEFONICA.value
        raise FieldUnknown(self.url, "place", dir)

    def __find_category(self, data: Dict, webpage: Dict):
        name = plain_text(data['name'])
        if re_or(name, "madresfera"):
            return Category.MATERNITY
        cat = plain_text(self.select_one_txt("span.categoria"))
        if cat == "exposicion":
            return Category.EXPO
        if cat == "taller":
            return Category.WORKSHOP
        description = plain_text(webpage.get('description', '') + ' ' + self.select_one_txt("#textoread"))
        if re_or(
            description,
            r"encuentro con (el|la|los|las) escrito(ra|re)s?",
            r'recibimos al autora?',
            "un libro sobre",
            r"conversar con (el|la) escritora?",
            "presentacion del nuevo trabajo",
            "todopoderosos",
            "publicacion de su ultima",
            "acogemos un nuevo encuentro",
            "accesibilidad auditiva",
            "una conversacion entre",
            r"se presentar[áa]n diversos proyectos",
            flags=re.I
        ):
            return Category.CONFERENCE
        logger.critical(str(CategoryUnknown(self.url, cat)))
        return Category.UNKNOWN


if __name__ == "__main__":
    from core.log import config_log
    config_log("log/telefonica.log", log_level=(logging.DEBUG))
    print(Telefonica().events)
