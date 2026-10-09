from core.web import Web, get_text
from bs4 import Tag
import re
import json
from core.filemanager import FM
from typing import NamedTuple, Optional

from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo
from collections import defaultdict

MADRID = ZoneInfo("Europe/Madrid")


def to_date(value: str, hm: str) -> datetime:
    value = value.strip()

    if value.endswith("Z"):
        dt = datetime.strptime(
            value, "%Y%m%dT%H%M%SZ"
        ).replace(tzinfo=timezone.utc)
        return dt.astimezone(MADRID)

    if "T" in value:
        dt = datetime.strptime(value, "%Y%m%dT%H%M%S")
        return dt.replace(tzinfo=MADRID)

    dt = datetime.combine(
        datetime.strptime(value, "%Y%m%d").date(),
        time.fromisoformat(hm),
        tzinfo=MADRID
    )

    return dt


class Event(NamedTuple):
    uid: str
    #feed: str
    title: str
    duration: int
    location: Optional[str] = None
    dates: tuple[datetime, ...] = tuple()
    #description: str = None
    #url: str = None


class WPIcs:
    def __init__(self, url: str):
        self.__url = url
        self.__w = Web()

    @property
    def url(self) -> str:
        return self.__url

    def __select_one(self, selector: str) -> Tag:
        div = self.__w.soup.select_one(selector)
        if div is None:
            raise ValueError(f"{selector} not found in {self.__w.url}")
        return div

    def __get_r34ics_ajax_obj(self) -> dict:
        k = 'r34ics_ajax_obj'
        for script in map(get_text, self.__w.soup.select("script")):
            match = re.search(r"var\s+"+re.escape(k)+r"\s*=\s*({.*?})\s*;", script or '')
            if match:
                txt = match.group(1)
                js = json.loads(txt)
                if isinstance(js, dict):
                    return js
        raise ValueError(f"{k} not found in page")

    def get_items(self):
        self.__w.get(self.__url)
        div = self.__select_one("div.post-content div.r34ics-ajax-container[data-args!='']")
        obj = self.__get_r34ics_ajax_obj()
        url = obj["ajaxurl"]
        data = dict(
            action='r34ics_ajax',
            r34ics_nonce=obj['r34ics_nonce'],
            subaction='display_calendar',
            args=div.attrs['data-args'],
            **{
                "js_args[debug]": "true",
                "js_args[ajax]": "true",
                "js_args[eventdl]": "true",
                "js_args[attach]": "true"
            }
        )
        soup = self.__w.get(
            url,
            **data,
        )
        events: dict[Event, set[str]] = defaultdict(set)
        for bt in soup.select("button.r34ics_event_ics_download"):
            d1 = to_date(bt.attrs["data-eventdl-dtstart"], "00:00")
            d2 = to_date(bt.attrs["data-eventdl-dtend"], "23:59")
            li = bt.find_parent("li")
            e = Event(
                uid=bt.attrs["data-eventdl-uid"],
                #feed=bt.attrs["data-eventdl-feed-key"],
                title=bt.attrs["data-eventdl-label"],
                location=get_text(li.select_one("div.location")),
                duration=int((d2-d1).seconds // 60)
            )
            events[e].add(d1.strftime("%Y-%m-%d %H:%M"))
        items: set[Event] = set()
        for e, dates in events.items():
            e = e._replace(dates=tuple(sorted(dates)))
            items.add(e)

        return tuple(sorted(items))


if __name__ == "__main__":
    import sys
    w = WPIcs(sys.argv[1])
    w.get_items()
