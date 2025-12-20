import requests
from icalendar import Calendar, vDDDTypes, Component, vText
from icalendar.prop import vCategory
from functools import cached_property
from core.event import Event, Session, Category, Place, CategoryUnknown
from zoneinfo import ZoneInfo
from datetime import date, datetime
from core.util import re_and, re_or
import re

import logging

logger = logging.getLogger(__name__)

MADRID = ZoneInfo("Europe/Madrid")


class IcsEvent:
    def __init__(self, event: Component):
        self.__event = event

    def __str__(self):
        return str(self.__event)

    def __get_datetime(self, key: str):
        val = self.__event.get(key)
        if val is None:
            return None
        if not isinstance(val, vDDDTypes):
            raise ValueError(f"Valor no es vDDDTypes: {val!r}")
        dt = val.dt
        if isinstance(dt, date):
            return dt
        if not isinstance(dt, datetime):
            raise ValueError(f"Valor no es vDDDTypes con datetime: {val!r}")
        if dt.tzinfo is None:
            return dt.replace(tzinfo=MADRID)
        return dt.astimezone(MADRID)

    def __get_text(self, key: str):
        val = self.__event.get(key)
        if val is None:
            return None
        if not isinstance(val, (vText, str)):
            raise ValueError(f"Valor no es vText: {val!r}")
        s = str(val).strip()
        if len(s) == 0:
            return None
        return s

    @property
    def DTSTART(self):
        return self.__get_datetime("DTSTART")

    @property
    def DTEND(self):
        return self.__get_datetime("DTEND")

    @property
    def CREATED(self):
        return self.__get_datetime("CREATED")

    @property
    def UID(self):
        return self.__get_text("UID")

    @property
    def SUMMARY(self):
        return self.__get_text("SUMMARY")

    @property
    def duration(self):
        dtstart = self.DTSTART
        dtend = self.DTEND
        if dtstart is None or dtend is None:
            return None
        m = (dtend - dtstart).total_seconds() / 60
        return int(m)

    @property
    def CATEGORIES(self) -> tuple[str, ...]:
        val = self.__event.get("CATEGORIES")
        if val is None:
            return tuple()
        if not isinstance(val, vCategory):
            raise ValueError(f"Valor no es vCategory: {val!r}")
        cats: list[str] = []
        for c in val.cats:
            if not isinstance(c, (vText, str)):
                raise ValueError(f"Valor no es vText: {c!r}")
            s = str(c).strip()
            if len(s) and s not in cats:
                cats.append(s)
        return tuple(cats)

    @property
    def ATTACH(self):
        return self.__event.get("ATTACH")

    @property
    def URL(self):
        return self.__event.get("URL")

    @property
    def LOCATION(self):
        return self.__get_text("LOCATION")

    @property
    def publish(self):
        p = None
        for k in ("DTSTAMP", "CREATED", "LAST-MODIFIED"):
            dt = self.__get_datetime(k)
            if dt is None:
                continue
            if isinstance(dt, datetime):
                dt = dt.date()
            if dt is not None and (p is None or dt < p):
                p = dt
        return dt


class IcsToEvent:
    def __init__(self, url: str):
        self.__url = url

    def _iter_events(self):
        r = requests.get(self.__url, timeout=10)
        r.raise_for_status()
        text = r.text.strip()
        if len(text) == 0:
            logger.warning(f"Calendario vació {self.__url}")
        else:
            cal = Calendar.from_ical(text)
            for e in cal.walk("VEVENT"):
                yield IcsEvent(e)

    @cached_property
    def events(self):
        logger.info(f"Buscando eventos en {self.__url}")
        events: set[Event] = set()
        for e in self._iter_events():
            if e.DTSTART is None:
                logger.critical(f"{self.__url} Evento sin fecha de inicio: {e}")
                continue
            if e.UID is None:
                logger.critical(f"{self.__url} Evento sin UID: {e}")
                continue
            if e.SUMMARY is None:
                logger.critical(f"{self.__url} Evento sin SUMMARY: {e}")
                continue
            if e.LOCATION is None:
                logger.critical(f"{self.__url} Evento sin LOCATION: {e}")
                continue

            event = Event(
                id=e.UID,
                url=e.URL,
                name=e.SUMMARY,
                duration=e.duration or 60,
                img=e.ATTACH,
                price=0,
                publish=e.publish.strftime("%Y-%m-%d") if e.publish else None,
                category=self.__find_category(e),
                place=Place(
                    name=e.LOCATION,
                    address=e.LOCATION
                ),
                sessions=(
                    Session(
                        date=e.DTSTART.strftime("%Y-%m-%d %H:%M"),
                    ),
                ),
            )
            events.add(event)
        return tuple(sorted(events))

    def __find_category(self, e: IcsEvent):
        if re_and(e.SUMMARY, "presentaci[oó]n del?", ("libro", "novela"), flags=re.I, to_log=e.UID):
            return Category.LITERATURE
        if re_or(e.SUMMARY, "exposici[oó]n", flags=re.I, to_log=e.UID):
            return Category.EXPO
        logger.critical(str(CategoryUnknown(self.__url, f"{e}")))
        return Category.UNKNOWN


class BulkIcsToEvent:
    def __init__(self, *urls: str):
        self.__urls = urls

    @property
    def events(self):
        all_events: set[Event] = set()
        for url in self.__urls:
            all_events.update(IcsToEvent(url).events)
        return tuple(sorted(all_events))


if __name__ == "__main__":
    ics_to_event = IcsToEvent(
        "https://fal.cnt.es/events/lista/?ical=1"
    )
    for event in ics_to_event.events:
        print(event)
