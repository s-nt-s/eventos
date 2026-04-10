from core.web import get_text, MyTag, Web
from typing import Set, Dict, List, Optional
from functools import cache
from core.cache import TupleCache
import logging
from core.event import Event, Session, Category, CategoryUnknown
from core.place import Places
import re
from datetime import datetime
from core.util import re_or, KO_IMG
from core.md import MD

logger = logging.getLogger(__name__)

months = ('ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic')


class CasaEncendidaException(Exception):
    pass


class CasaEncendida:
    URL = "https://www.lacasaencendida.es/actividades?t[0]=activity_"
    ACTIVITY_OK = {
        2: Category.CINEMA,
        3: Category.MUSIC,
        4: Category.CONFERENCE, # Encuentros
        5: None, # Escenicas
    }
    ACTIVITY_KO = {
        5: Category.CHILDISH, # Familia
        14: Category.ONLINE, # Online
    }

    def __init__(self):
        self.__w = Web()
        self.__w.s.headers.update({'Accept-Encoding': 'gzip, deflate'})
        self.__url_cat: dict[str, Category] = {}

    def __get_soup(self, url: str):
        soup = self.__w.get(url)
        status_code = self.__w.response.status_code
        tag = MyTag(self.__w.url, soup, status_code)
        if status_code == 500:
            logger.warning(f"status_code={status_code} {url}")
        return tag

    def __get_ld_json(self, soup: MyTag) -> Optional[Dict]:
        css_script = 'script[type="application/ld+json"]'
        js = soup.select_one_json(css_script, if_none={
            500: "silent"
        }.get(soup.status_code, "raise"))
        return js

    @cache
    def get_links(self):
        urls: Set[str] = set()
        for a, cat in CasaEncendida.ACTIVITY_KO.items():
            for url in self.__get_links(CasaEncendida.URL+str(a)):
                if self.__url_cat.get(url) is None:
                    self.__url_cat[url] = cat
        for a, cat in CasaEncendida.ACTIVITY_OK.items():
            for url in self.__get_links(CasaEncendida.URL+str(a)):
                if self.__url_cat.get(url) is None:
                    self.__url_cat[url] = cat
                urls.add(url)
        return tuple(sorted(urls))

    def __get_links(self, url_cat):
        urls: Set[str] = set()
        page = 0
        while True:
            page = page + 1
            soup = self.__w.get(url_cat+f"&page={page}")
            links = soup.select("div.results-list a.results-list__link")
            if len(links) == 0:
                if page == 1:
                    logger.warning(f"NOT FOUND {url_cat}")
                return tuple(sorted(urls))
            for a in links:
                urls.add(a.attrs["href"])

    @property
    @TupleCache("rec/casaencendida.json", builder=Event.build)
    def events(self):
        logger.info("Casa Encendida: Buscando eventos")
        events: Set[Event] = set()
        for url in self.get_links():
            ev = self.__url_to_event(url)
            if ev:
                events.add(ev)
        logger.info(f"Casa Encendida: Buscando eventos = {len(events)}")
        return tuple(sorted(events))

    def __url_to_event(self, url):
        soup = self.__get_soup(url)
        for txt in soup.select_txt("div.tickets-btn.ico-tickets-full", if_none="silent"):
            if re_or(
                txt,
                "se han agotado las entradas",
                flags=re.I
            ):
                return None
        info = self.__get_ld_json(soup)
        if info is None:
            return None
        self.__validate_info_event(info)
        idevent = info[0]['identifier'].split("-")[-1]
        category = self.__find_category(soup, info)
        sessions = self.__find_sessions(soup.url, info)
        name = info[0]['name']
        more, cycle = self.__find_more_and_cycle(name, soup, category)
        ev = Event(
            id="ce"+idevent,
            url=soup.url,
            name=name,
            category=category,
            img=self.__find_img(info),
            place=Places.CASA_ENCENDIDA.value,
            sessions=sessions,
            price=self.__find_price(info),
            duration=self.__find_duration(info),
            cycle=cycle,
            more=more
        )
        if len(ev.sessions) == 1:
            s1 = ev.sessions[0]
            if s1.url and (ev.url is None or s1.url.startswith(ev.url)):
                ev = ev.merge(
                    url=s1.url,
                    sessions=(s1._replace(url=None), )
                )
        return ev

    def __find_img(self, info: list[dict]):
        for i in info:
            if isinstance(i, dict):
                img = i.get("image")
                if img and img not in KO_IMG:
                    return img

    def __validate_info_event(self, info: List):
        if not isinstance(info, list):
            raise CasaEncendidaException("MUST TO BE A LIST: "+str(info))
        if len(info) == 0:
            raise CasaEncendidaException("MUST TO BE A LIST NOT EMPTY: "+str(info))
        for i in info:
            if not isinstance(i, dict):
                raise CasaEncendidaException("MUST TO BE A LIS OF DICTs: "+str(info))
        identifier = info[0].get('identifier')
        if not isinstance(identifier, str):
            raise CasaEncendidaException("MUST TO BE A LIS OF DICTs with a identifier: "+str(info))
        idevent = identifier.split("-")[-1]
        if not idevent.isdigit():
            raise CasaEncendidaException("MUST TO BE A LIS OF DICTs with a int identifier: "+str(info))
        return True

    def __find_sessions(self, url: str, info: List[Dict]):
        if len(info) == 1:
            return tuple((Session(
                url=None,
                date=info[0]["startDate"][:16].replace("T", " ")
            ), ))
        sessions: Set[Session] = set()
        for i in info[1:]:
            s_url = i['location']['url']
            sessions.add(Session(
                url=s_url if s_url != url else None,
                date=i["startDate"][:16].replace("T", " ")
            ))
        return tuple(sorted(sessions))

    def __find_price(self, info: List[Dict]):
        prices = set({0, })
        for i in info:
            if not i.get("offers"):
                continue
            for o in i["offers"]:
                prices.add(float(o["price"]))
        return max(prices)

    def __find_category(self, soup: MyTag, info: List[Dict]):
        for li in map(get_text, soup.node.select("ul.item-detail__list li")):
            if li is None:
                continue
            if "No está permitida la entrada a mayores si no van acompañados de un menor" in li:
                return Category.CHILDISH
            if "El workshop se impartirá en inglés" in li:
                return Category.NO_EVENT
        name: str = info[0]['name'].lower()
        tags = set()
        for tag in soup.select_txt(", ".join(
            (
                "div.tags",
                "div.item-detail__info__tags a",
                "div.breadcrumb__item a",
                "div.item-detail__hero__info__content a.group-link"
            )
        )):
            for t in re.split(r"\s*[,/\.]\s+", tag):
                t = re.sub(r"^#\s*", "", t.strip())
                tags.add(t.lower())
        if tags.intersection(("en familia", "espacio nido")):
            return Category.CHILDISH
        if re_or(
            name,
            "Asamblea de juveniles",
            flags=re.I
        ):
            return Category.YOUTH
        if re_or(
            name,
            "Visita al edificio",
            flags=re.I
        ):
            return Category.VISIT
        if re_or(
            name,
            "master ?class",
            "mesa redonda",
            flags=re.I
        ):
            return Category.CONFERENCE
        if re_or(
            name,
            "Juegatorio",
            flags=re.I
        ):
            return Category.NO_EVENT
        if re_or(
            name,
            "workshop",
            "Laboratorio",
            flags=re.I
        ):
            return Category.WORKSHOP
        if tags.intersection(("ecoclub de lectura", "club de lectura")):
            return Category.READING_CLUB
        if tags.intersection(("cine", "audiovisuales")):
            return Category.CINEMA
        if tags.intersection(("conciertos", "música")):
            return Category.MUSIC
        if "concierto" in name:
            return Category.MUSIC
        desc = MD.convert(soup.select_one_txt("div.item-detail__info__content"))
        if re_or(desc, "canciones", flags=re.I):
            return Category.MUSIC
        if re_or(desc, "workshop", flags=re.I):
            return Category.WORKSHOP
        if re_or(name, r"films?"):
            return Category.CINEMA
        if re_or(
            name,
            "Diario Vivo",
            flags=re.I
        ):
            return Category.THEATER
        cat = self.__url_cat.get(soup.url)
        if cat is not None:
            return cat
        logger.critical(str(CategoryUnknown(soup.url, ", ".join(sorted(tags)))))
        return Category.UNKNOWN

    def __get_group(self, soup: MyTag):
        a_cycle = soup.node.select_one("div.item-detail__hero__info__content a.group-link")
        cycle = get_text(a_cycle)
        if cycle is not None:
            href = a_cycle.get("href")
            cycle = re.sub(r"(radio encendida) \d+$", r"\1", cycle, flags=re.I)
            m = re.match(r"^(conversar)\s*,\s*(.+)\s*$", cycle, flags=re.I)
            if m:
                cycle = m.group(2)
                cycle = cycle[0].upper() + cycle[1:]
            return href, cycle

    def __find_more_and_cycle(self, name: str, soup: MyTag, category: Category):
        href_group = self.__get_group(soup)
        if href_group:
            href, group = href_group
            if category in (Category.CONFERENCE, ):
                return href, group
            if category == Category.MUSIC:
                if re_or(group, "radio encendida", flags=re.I):
                    return href, group
                if re_or(group, "reguet[oó]n", flags=re.I):
                    return href, group
        return None, None

    def __find_duration(self, info: List[Dict]):
        def to_date(s: str):
            if s is not None:
                return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S%z")
        under24: Set[int] = set()
        over24: Set[int] = set()
        for i in info:
            startDate = to_date(i.get('startDate'))
            endDate = to_date(i.get('endDate'))
            if startDate and endDate:
                d = round((endDate - startDate).total_seconds() / 60)
                if d < 0:
                    continue
                if d < (24*60):
                    under24.add(d)
                else:
                    over24.add(d)
        if len(under24.union(over24)) == 0:
            raise FileNotFoundError("duration", info)
        if under24:
            return max(under24)
        return max(over24)


if __name__ == "__main__":
    from core.log import config_log
    config_log("log/casaencendida.log", log_level=(logging.DEBUG))
    print(CasaEncendida().events)
