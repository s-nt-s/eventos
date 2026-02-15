from requests import Session
from functools import cached_property
from datetime import datetime, timedelta
import logging
from core.util import re_or
import re
from core.web import buildSoup
import feedparser
from core.dictwraper import DictWrapper
from typing import NamedTuple, Callable
from core.fetcher import Getter
from aiohttp import ClientResponse


logger = logging.getLogger(__name__)


class Place(NamedTuple):
    name: str
    address: str
    lat: float | None = None
    lon: float | None = None

    def get_latlon(self) -> str | None:
        if None not in (self.lat, self.lon):
            return f"{self.lat},{self.lon}"
        return None


class Event(NamedTuple):
    id: int
    url: str
    title: str
    place: Place
    duration: int
    description: str | None
    sessions: tuple[str, ...] = tuple()
    media: tuple[str, ...] = tuple()
    links: tuple[str, ...] = tuple()
    tags: tuple[str, ...] = tuple()


async def rq_to_desc(r: ClientResponse):
    soup = buildSoup(str(r.url), await r.text())
    tag = soup.select_one("div.p-description")
    if tag:
        return str(tag)


class GancioPortal:
    def __init__(
        self,
        root: str,
        api_key: str = None,
        show_recurrent: bool = False,
        show_online: bool = False,
        isOkDate: Callable[[datetime], bool] = None,
    ):
        self.__root = root
        self.__show_recurrent = "true" if show_recurrent else "false"
        self.__show_online = show_online
        self.__isOkDate = isOkDate or (lambda x: True)
        self.__s = Session()
        if api_key:
            self.__s.headers.update({"Authorization": f"Bearer {api_key}"})
        self.__getter = Getter(
            onread=rq_to_desc
        )

    def __get_json(self, endpoint: str):
        response = self.__s.get(f"{self.__root}/{endpoint}")
        response.raise_for_status()
        return response.json()

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

    def __get_description(self, url: str) -> str | None:
        for i in self.rss.entries:
            if i.link == url:
                return str(buildSoup(self.__root, i.description))

    def __obj_to_event(self, e: DictWrapper) -> Event:
        p = e.get_dict("place")
        place = Place(
            name=p.get_str("name"),
            address=p.get_str("address"),
            lat=p.get_float_or_none('latitude'),
            lon=p.get_float_or_none('longitude')
        )
        if not self.__show_online and re_or(place.name, r"^(online|zoom)$", flags=re.I):
            return None

        start = e.get_datetime('start_datetime')
        end = e.get_datetime_or_none('end_datetime')
        duration, sessions = self.__get_duration_sessions(start, end)
        if len(sessions) == 0:
            return None

        url = self.__root+'/event/'+e.get_str("slug")
        links: list[str] = []
        for m in p.get_list_or_empty('online_locations'):
            if re.match(r"^https?://\S+$", m or '') and m not in links:
                links.append(m)

        media_list: list[str] = []
        for m in e.get_list_or_none('media'):
            media = m.get("url")
            if media:
                media_list.append(f'{self.__root}/media/{media}')

        tags: list[str] = list()
        for t in map(str.lower, e.get_list_or_empty('tags')):
            for x in map(str.strip, re.split(r"\s*#\s*", t)):
                if x is not None and len(x) and x not in tags:
                    tags.append(x)

        event = Event(
            url=url,
            id=e.get_int('id'),
            title=e.get_str("title"),
            media=tuple(media_list),
            duration=duration,
            sessions=tuple(sessions),
            links=tuple(links),
            tags=tuple(tags),
            place=place,
            description=self.__get_description(url)
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
                    st = start + timedelta(days=1)
                    nd = end.date()
                    while st.date() <= nd:
                        days.append(st)
                        st = st + timedelta(days=1)
        sessions: list[str] = []
        for dt in sorted(days):
            if self.__isOkDate(dt):
                sessions.append(dt.strftime("%Y-%m-%d %H:%M"))
        return duration, tuple(sessions)

    def get_events(self):
        logger.info(f"Buscando eventos en {self.__root}")
        all_events: set[Event] = set()
        for e in map(DictWrapper, self.list_events()):
            event = self.__obj_to_event(e)
            if event:
                all_events.add(event)
        url_desc: dict[str, str] = self.__getter.get(*(e.url for e in all_events if e.description is None))
        for ev in tuple(all_events):
            desc = url_desc.get(ev.url)
            if ev.description is None and desc is not None:
                all_events.discard(ev)
                all_events.add(ev._replace(description=desc))
        return tuple(all_events)


if __name__ == "__main__":
    gp = GancioPortal(root="https://mad.convoca.la")
    print(len(list(gp.get_events())))
    #for ev in gp.events:
    #    print(ev)
