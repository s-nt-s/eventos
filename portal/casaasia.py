from core.web import Web
from core.cache import Cache, TupleCache
from core.event import Event, Category, CategoryUnknown, Session
from core.place import Places, Place
import json
import logging
from functools import cached_property
from core.util import re_or, find_euros
import re
import requests
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import pytz
from portal.base import Base

logger = logging.getLogger(__name__)

TZ_ZONE = 'Europe/Madrid'
re_sp = re.compile(r"\s+")


def str_to_datetime(s: str):
    dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    dt = dt.replace(tzinfo=ZoneInfo(TZ_ZONE))
    return dt


class CasaAsia(Base):
    URL = "https://www.casaasia.es/actividades/?lugar=madrid"

    def __init__(self, cache: bool | str = True):
        super().__init__(cache=cache)
        self.__w = Web()
        self.__now = datetime.now(tz=pytz.timezone(TZ_ZONE))

    @Cache("rec/casaasia/items.json")
    def get_data(self):
        slc = "div[ref='viewData'][data-atts]"
        soup = self.__w.get(CasaAsia.URL)
        div = soup.select_one()
        if div is None:
            logger.critical(f"NOT FOUND {slc} {CasaAsia.URL}")
            return None
        data = div.attrs.get("data-atts")
        if not isinstance(data, str):
            return None
        data = data.strip()
        if len(data) == 0:
            return None
        return json.loads(data)
    
    @Cache("rec/casaasia/activities.json")
    def get_activities(self) -> list[dict]:
        url = 'https://www.casaasia.es/wp-json/wp/v2/actividad?per_page=100'
        r = requests.get(url)
        data = r.json()
        if not isinstance(data, list):
            raise ValueError(url)
        if len(data) and not all(isinstance(i, dict) for i in data):
            raise ValueError(url)
        return data

    def _get_events(self):
        activities = self.get_activities()
        if activities is None:
            return tuple()
        events: set[Event] = set()
        for a in activities:
            e = self.__activitie_to_event(a)
            if e is not None:
                events.add(e)
        return tuple(sorted(events))

    def __activitie_to_event(self, a: dict):
        place = self.__find_place(a)
        if place is None:
            return None
        sessions, duration = self.__find_sessions_duration(a)
        if not sessions:
            return None
        e = Event(
            id=f"cas{a['id']}",
            name=a['title']['rendered'],
            url=a['link'],
            img=a['yoast_head_json']['og_image'][0]['url'],
            category=self.__find_category(a),
            place=self.__find_place(a),
            price=find_euros(a['acf']['precio']),
            sessions=sessions,
            duration=duration,
        )
        return e

    def __find_place(self, a: dict):
        if 'lugar-madrid' not in a['class_list']:
            return None
        if a['acf']['ubicacion'] in ('Online.', ):
            return None
        p = a['acf']['google_maps']
        u = a['acf']['ubicacion']
        if re_or(
            u,
            r"^\W*online\W*$",
            flags=re.I
        ):
            return None
        if p is None:
            if re_or(
                a['yoast_head_json']['og_site_name'],
                "Casa Asia",
            ):
                return Places.CASA_ASIA.value
            logger.critical(f"PLACE NOT FOUND {a['link']}")
            return None
        if p['name'] == "Casa Asia Madrid":
            return Places.CASA_ASIA.value
        return Place(
            name=p['name'],
            address=p['address'],
            latlon=f"{p['lat']},{p['lng']}"
        )

    def __find_sessions_duration(self, a: dict):
        date_start = str_to_datetime(a['acf']['fecha_y_hora_de_inicio'])
        date_end = str_to_datetime(a['acf']['fecha_y_hora_de_fin'])
        if date_end < self.__now:
            return None, None
        duration = int((date_end-date_start).total_seconds() // 60)
        ss: list[Session] = []
        if duration <= (60*23):
            ss.append(Session(
                date=date_start.strftime("%Y-%m-%d %H:%M")
            ))
        else:
            new_start = date_end.replace(hour=date_start.hour, minute=date_start.minute)
            duration = int((date_end-new_start).total_seconds() // 60)
            if duration < 0:
                logger.critical(f"Fechas extrañas {a['link']}")
                return None, None
            ss = self.__fix_dates(date_start, date_end, a)
        if duration>0 and not ss:
            logger.critical(f"Fechas extrañas {a['link']}")
            return None, None
        link = a['acf']['inscripciones']
        if len(ss) == 1 and isinstance(link, str) and link.startswith(("https://", "http://")):
            ss = [ss[0]._replace(url=link)]
        return tuple(ss), duration
    
    def __fix_dates(self, d1: datetime, d2: datetime, a: dict):
        fch = a['acf']['fecha']
        if not isinstance(fch, str):
            return tuple()
        fch = re_sp.sub(" ", fch).strip()
        for k, v in {
            r"A las (\d{2})[\.:](\d{2}) h todos los s[áa]bados": (5, ),
            r"Del \d+ de \w+ al \d+ de \w+ a las (\d{2})[\.:](\d{2})": ()
        }.items():
            m = re.search(k, fch, flags=re.I)
            if m is None:
                continue
            ss: list[Session] = []
            h, mi = map(int, m.groups())
            while d1 <= d2:
                if len(v) == 0 or d1.weekday() in v:
                    ss.append(Session(
                        d1.replace(hour=h, minute=mi).strftime("%Y-%m-%d %H:%M")
                    ))
                d1 = d1 + timedelta(days=1)
            return tuple(ss)
        return tuple()

    def __find_category(self, a: dict):
        cats = a['class_list']
        for k, v in {
            "formato-actividad-conferencias": Category.CONFERENCE,
            "formato-actividad-tertulias": Category.CONFERENCE,
            "formato-actividad-cine": Category.CINEMA,
        }.items():
            if k in cats:
                return v
        title = a['title']['rendered']
        if re_or(
            title,
            "obra de teatro",
            flags=re.I
        ):
            return Category.THEATER
        logger.critical(str(CategoryUnknown(a['link'], ", ".join(sorted(cats)))))
        return Category.UNKNOWN


if __name__ == "__main__":
    from core.log import config_log
    config_log("log/casaasia.log", log_level=(logging.DEBUG))
    c = CasaAsia()
    print(c.get_events())
