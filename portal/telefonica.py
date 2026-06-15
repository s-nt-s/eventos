from core.web import Web, get_text
from core.cache import Cache
from typing import Set, Dict, List
import logging
from core.event import Event, Session, Category, CategoryUnknown, FieldUnknown, find_book_category
from core.place import Places
from datetime import datetime, date, timedelta
from core.util import plain_text, re_or
import re
import pytz
from portal.base import Base
from base64 import b64decode
import json
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


def gNow():
    return datetime.now(tz=pytz.timezone('Europe/Madrid'))


def to_datetime(s: str):
    dt = datetime(*map(int, re.findall(r"\d+", s)))
    return dt.replace(
        tzinfo=ZoneInfo("Europe/Madrid")
    )


class Telefonica(Base):
    URL = "https://espacio.fundaciontelefonica.com/agenda/este-mes/"
    slc_data1 = "script:not(.aioseo-schema)[type='application/ld+json']"
    slc_data2 = "script.aioseo-schema[type='application/ld+json']"

    def __init__(self, refer=None, verify=True, cache: str | bool = True):
        super().__init__(cache=cache)
        self.__w = Web(refer, verify)
        self.__w.s.headers.update({
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
        if url == self.__w.url:
            return self.__w.soup
        logger.debug(url)
        return self.__w.get(url, auth, parser, **kwargs)

    def __iter_js(self):
        for script in self.__w.soup.select("script[src]"):
            src = script.attrs.get("src")
            if not (src or '').startswith('data:text/javascript;base64,'):
                continue
            base64_part = src.split(",", 1)[1]
            js = b64decode(base64_part).decode("utf-8")
            yield js

    @Cache("rec/telefonica/data.json")
    def get_data(self):
        dt = (date.today() - timedelta(days=32)).replace(day=1)
        data: dict[str, dict] = {}
        size = -1
        while len(data) != size:
            dt = (dt + timedelta(days=32)).replace(day=1)
            url = Telefonica.URL + f"?idm={dt.month}&a={dt.year}"
            size = len(data)
            self.get(url)
            for script in self.__iter_js():
                spl = script.split('events:[{"id":', 1)
                if len(spl) != 2:
                    continue
                js = '[{"id":' + spl[1]
                spl = js.split(',eventRender:function', 1)
                if len(spl) != 2:
                    logger.warning(f"Revisar separador en {url}")
                    continue
                js = spl[0]
                events = json.loads(js)
                if not isinstance(events, list):
                    logger.warning(f"Revisar json en {url}")
                    continue
                if len(events) == 0:
                    continue
                ko = 0
                for i in events:
                    if not isinstance(i, dict) or not isinstance(i.get("id"), int):
                        ko = ko + 1
                        continue
                    data[i['id']] = i
                if ko:
                    logger.warning(f"Revisar json en {url}")
        now = gNow()
        return sorted(
            (d for d in data.values() if to_datetime(d['end']) > now),
            key=lambda x: x['id']
        )

    def _get_events(self):
        events: Set[Event] = set()
        for data in self.get_data():
            ev = self.__data_to_event(data)
            if ev:
                events.add(ev)
        return tuple(sorted(events))

    @Cache("rec/telefonica/{}data.json")
    def __get_script_data(self, url: str):
        self.get(url)
        error = []
        slc_error1 = f"{Telefonica.slc_data1} is not a Tuple[Dict]"
        slc_error2 = f"{Telefonica.slc_data2} is not a Dict['@graph, List[Dict[type, WebPage]]]"
        data1 = self.__w.select_one_json(Telefonica.slc_data1)
        data2 = self.__w.select_one_json(Telefonica.slc_data2)
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
            raise FieldUnknown(self.__w.url, "script data", error)
        event: Dict = data[Telefonica.slc_data1][0]
        graph: List[Dict] = data[Telefonica.slc_data2]['@graph']
        webpage = [i for i in graph if isinstance(i, dict) and i.get('@type') == 'WebPage'][0]
        return event, webpage

    def __data_to_event(self, item: dict):
        url = item['url']
        self.get(url)
        if self.__w.soup.select_one("div.participar") and not self.__w.soup.select_one("div.participar a.reservabtn"):
            logger.warning(f"{url} no tiene reservas")
            return None
        data, webpage = self.get_script_data(url)

        duration, session = self.__get_session(data)
        if duration > (60*24):
            return None
        name = get_text(self.__w.soup.select_one("span.titulo"))
        ev = Event(
            id=f"tl{item['id']}",
            url=url,
            name=name or item.get('title') or data.get('name'),
            img=data.get('image') or item.get('image'),
            price=0,
            category=self.__find_category(data, webpage),
            duration=duration,
            sessions=(session,),
            place=self.__find_place(),
        )
        if ev.category == Category.LITERATURE:
            m = re.match(r"^Encuentro con (.+?)\. (.+)$", ev.name, flags=re.I)
            if m:
                ev = ev.merge(
                    name=m.group(1)+" presenta: "+m.group(2)
                )
        return ev

    def __get_session(self, data: Dict):
        url = None
        link = self.__w.soup.select_one('a.reservabtn[id^="eventbrite-widget-modal-trigger-"]')
        if link:
            _id_ = link.attrs["id"].split("-")[-1]
            if _id_ and _id_.isdecimal():
                url = f"https://eventbrite.es/e/{_id_}"
        ini = datetime.fromisoformat(data['startDate'])
        fin = datetime.fromisoformat(data['endDate'])
        start = ini.strftime("%Y-%m-%d %H:%M")
        duration = int((fin - ini).total_seconds() / 60)
        return duration, Session(date=start, url=url)

    def __find_place(self):
        dir = self.__w.select_one_txt("span.direccion")
        if dir == "C/ Fuencarral, 3, Madrid":
            return Places.FUNDACION_TELEFONICA.value
        raise FieldUnknown(self.__w.url, "place", dir)

    def __find_category(self, data: Dict, webpage: Dict):
        name = plain_text(data['name'])
        cat = plain_text(self.__w.select_one_txt("span.categoria"))
        description = ' '.join([
            webpage.get('description', ''),
            get_text(self.__w.soup.select_one("#textoread")) or ''
        ])
        plain_description = plain_text(description)
        if cat == "taller":
            if re_or(plain_description, "taller para (familias|niñ[oa@xe]s)", flags=re.I):
                return Category.CHILDISH
            return Category.WORKSHOP
        if re_or(
            plain_description,
            r"presenta su( [úu]ltima)? novela",
            r"publicaci[oó]n de su( [úu]ltima| nuevo)? ensayo",
        ):
            return find_book_category(name, plain_description, Category.LITERATURE)
        if re_or(name, "madresfera"):
            return Category.MATERNITY
        if re_or(name, "^encuentro con", flags=re.I):
            return find_book_category(name, plain_description, Category.CONFERENCE)
        if cat == "exposicion":
            return Category.EXPO
        if re_or(
            description,
            "CONVERSAN"
        ):
            return Category.CONFERENCE
        if re_or(
            plain_description,
            r"encuentro con (el|la|los|las) escrito(ra|re)s?",
            r'recibimos al autora?',
            r"El primer encuentro del año",
            "un libro sobre",
            r"conversar con (el|la) escritora?",
            "presentacion del nuevo trabajo",
            "todopoderosos",
            "publicacion de su ultima",
            "acogemos un nuevo encuentro",
            "accesibilidad auditiva",
            "una conversacion entre",
            r"se presentar[áa]n diversos proyectos",
            r"mesas? redondas?",
            r"ponencias?",
            r"debates?",
            r"coloquios?",
            r"conferencias?",
            ("CONVERSAN", "MODERA"),
            flags=re.I
        ):
            return Category.CONFERENCE
        logger.critical(str(CategoryUnknown(self.__w.url, cat)))
        return Category.UNKNOWN


if __name__ == "__main__":
    from core.log import config_log
    config_log("log/telefonica.log", log_level=(logging.DEBUG))
    Telefonica().get_events()
