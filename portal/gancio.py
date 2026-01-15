from requests import Session as ReqSession
from core.event import Event, Place, Category, Session, CategoryUnknown
from functools import cached_property
from datetime import datetime, timezone, timedelta
import logging
from core.util import re_or, plain_text, re_and
import re
from core.web import get_text, buildSoup, WEB
import feedparser
from zoneinfo import ZoneInfo
from core.dictwraper import DictWraper
from bs4 import Tag


logger = logging.getLogger(__name__)


class GancioPortal:
    def __init__(
        self,
        root: str,
        api_key: str = None,
        id_prefix: str = "ga",
        show_recurrent: bool = False,
        show_online: bool = False
    ):
        self.__root = root
        self.__id_prefix = id_prefix
        self.__show_recurrent = "true" if show_recurrent else "false"
        self.__show_online = show_online
        self.__s = ReqSession()
        if api_key:
            self.__s.headers.update({"Authorization": f"Bearer {api_key}"})

    def __get_json(self, endpoint: str):
        response = self.__s.get(f"{self.__root}/{endpoint}")
        response.raise_for_status()
        return response.json()

    def __get_datetime(self, i: int):
        if i is None:
            return None
        dt = datetime.fromtimestamp(i, tz=timezone.utc)
        dt = dt.astimezone(ZoneInfo("Europe/Madrid"))
        return dt

    def list_events(self) -> list[dict]:
        obj = self.__get_json(f"api/events?show_recurrent={self.__show_recurrent}&max=999")
        if not isinstance(obj, list):
            raise ValueError(obj)
        if not all(isinstance(e, dict) for e in obj):
            raise ValueError(obj)
        return obj

    @cached_property
    def rss(self):
        url = f"{self.__root}/feed/rss?show_recurrent={self.__show_recurrent}"
        return feedparser.parse(url)

    def get_description(self, url: str) -> Tag | None:
        for i in self.rss.entries:
            if i.link == url:
                return buildSoup(self.__root, i.description)
        soup = WEB.get_cached_soup(url)
        return soup.select_one("div.p-description")

    def __obj_to_event(self, e: DictWraper) -> Event:
        p = e.get_dict("place")
        latitude = p.get_float_or_none('latitude')
        longitude = p.get_float_or_none('longitude')
        media_list = e.get_list_or_none('media')
        start = e.get_datetime('start_datetime')
        end = e.get_datetime_or_none('end_datetime')
        url = self.__root+'/event/'+e.get_str("slug")
        img = None
        latlon = None
        if latitude is not None and longitude is not None:
            latlon = f'{latitude},{longitude}'
        if media_list:
            media = media_list[0].get("url")
            if media:
                img = f'{self.__root}/media/{media}'
        place = Place(
            name=p.get_str("name"),
            address=p.get_str("address"),
            latlon=latlon
        ).normalize()

        if not self.__show_online and re_or(place.name, r"^(online|zoom)$", flags=re.I):
            return None

        duration, sessions = self.__get_duration_sessions(start, end)

        event = Event(
            url=url,
            id=f"{self.__id_prefix}{e.get_int('id')}",
            price=0,
            name=e.get_str("title"),
            img=img,
            category=self.__find_category(url, place, e),
            duration=duration,
            sessions=sessions,
            place=place,
        )
        return event

    def __get_duration_sessions(self, start: datetime, end: datetime | None):
        duration = int((end-start).total_seconds() / 60) if end else 60
        days = [start]
        if duration > (60*24):
            hm1 = start.strftime("%H:%M")
            hm2 = end.strftime("%H:%M")
            if hm1 not in ("00:00", ) or hm2 not in ("00:00", "24:00", "23:59"):
                aux = end.replace(year=start.year, month=start.month, day=start.day)
                if aux > start:
                    duration = int((aux-start).total_seconds() / 60)
                    st = start  + timedelta(days=1)
                    nd = end.date()
                    while st.date() <= nd:
                        days.append(st)
                        st = st + timedelta(days=1)
        sessions = []
        for dt in days:
            sessions.append(Session(date=dt.strftime("%Y-%m-%d %H:%M")))
        return duration, tuple(sessions)

    def __find_category(self, url: str, place: Place, e: DictWraper) -> Category:
        _id_ = e.get_int('id')
        tags: set[str] = set()
        for t in map(str.lower, e.get_list_or_empty('tags')):
            for x in map(plain_text, map(str.strip, re.split(r"\s*#\s*", t))):
                if x is not None and len(x):
                    tags.add(x)

        def has_tag(*args):
            for a in args:
                if a in tags:
                    logger.debug(f"{_id_} tiene tag {a}")
                    return True
            return False

        def has_tag_or_title(*args):
            if has_tag(*args):
                return True
            if re_or(name, *args, flags=re.I, to_log=_id_):
                return True
            return False

        name = plain_text(e.get_str('title'))
        if has_tag_or_title("flinta"):
            return Category.NO_EVENT
        if has_tag_or_title("infantil"):
            return Category.CHILDISH
        if has_tag("asamblea") or has_tag_or_title('manifestacion', 'concentracion'):
            return Category.ACTIVISM
        if has_tag_or_title("cine", "cineforum", "cinebollum"):
            return Category.CINEMA
        if has_tag("deporte") or has_tag_or_title("yoga", "pilates"):
            return Category.SPORT
        if has_tag_or_title("taller", "formacion", "intercambio de idiomas"):
            return Category.WORKSHOP
        if has_tag_or_title("presentacion de libro"):
            return Category.LITERATURE
        if has_tag_or_title("intercambio de idiomas", "hacklab") or re_or(name, "taller", "^clases de", "^curso de", flags=re.I, to_log=_id_):
            return Category.WORKSHOP
        if re_or(name, "iniciaci[óo]n al",  flags=re.I, to_log=_id_) and has_tag("deporte", "gimnasia"):
            return Category.WORKSHOP
        if has_tag_or_title("teatro", "micro abierto", "performance"):
            return Category.THEATER
        if has_tag_or_title("club de lectura", "grupo de lectura", "clubdelectura", "grupodelectura", "bookelarre"):
            return Category.READING_CLUB
        if has_tag("concierto") or re_or("^concierto", flags=re.I, to_log=_id_):
            return Category.MUSIC
        if re_or(name, "fiesta", "Social Swing", "kabaret", flags=re.I, to_log=_id_):
            return Category.PARTY
        if re_or(name, "bicicritica", to_log=_id_):
            return Category.SPORT
        if re_and(name, "no", "compres", "cose",  flags=re.I, to_log=_id_):
            return Category.WORKSHOP
        if re_or(name, "Charla-debate", "conferencia", "Discusi[oó]n cr[ií]tica sobre", flags=re.I, to_log=_id_):
            return Category.CONFERENCE
        if re_or(name, "radio comunitaria", flags=re.I, to_log=_id_):
            return Category.WORKSHOP
        if has_tag_or_title("concierto", "swing") or has_tag("musica", "música"):
            return Category.MUSIC
        if has_tag_or_title("exposición", "exposicion", "miniexpo", "mini-expo"):
            return Category.EXPO

        desc = self.get_description(url)
        txt_desc = get_text(desc)
        if re_or(txt_desc, "Charla cr[ií]tica", "vendr[aá]n a conversar sobre", "conferencia", flags=re.I, to_log=_id_):
            return Category.CONFERENCE
        if re_or(txt_desc, "m[uú]sica electr[óo]nica", flags=re.I, to_log=_id_):
            return Category.MUSIC
        if re_or(txt_desc, "hacer arte cutre"):
            return Category.WORKSHOP
        if re_and(txt_desc, "performance", "micr[óo]fono abierto", "DJ Set(lists?)?", to_log=_id_, flags=re.I):
            return Category.PARTY
        if re_and(txt_desc, "Karaoke", r"DJ Set(s|lists?)?", to_log=_id_, flags=re.I):
            return Category.PARTY
        if re_and(txt_desc, "jornada", "auditorio", flags=re.I, to_log=_id_):
            return Category.CONFERENCE
        if re_or(txt_desc, "comedia perform[aá]tica", flags=re.I, to_log=_id_):
            return Category.THEATER

        if re_or(place.name, "librer[íi]a", flags=re.I):
            if re_or(name, "poes[íi]aa?", flags=re.I):
                return Category.POETRY
            if re_or(name, "presentaci[oó]n", flags=re.I):
                return Category.LITERATURE

        if re_or(name, "kafeta", to_log=_id_, flags=re.I):
            return Category.PARTY
        if re_or(name, "Presentaci[óo]n del libro", to_log=_id_, flags=re.I):
            return Category.LITERATURE

        logger.critical(str(CategoryUnknown(url, f"{e}")))
        return Category.UNKNOWN

    @cached_property
    def events(self):
        logger.info(f"Buscando eventos en {self.__root}")
        all_events: set[Event] = set()
        for e in map(DictWraper, self.list_events()):
            event = self.__obj_to_event(e)
            if event:
                all_events.add(event)

        return Event.fusionIfSimilar(
            all_events,
            ('name', 'place'),
            firstEventUrl=True
        )


if __name__ == "__main__":
    gp = GancioPortal(root="https://mad.convoca.la")
    list(gp.events)
    #for ev in gp.events:
    #    print(ev)
