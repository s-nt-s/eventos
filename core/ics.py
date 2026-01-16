from textwrap import dedent
from datetime import datetime
import pytz
from dataclasses import dataclass, asdict
import re
from core.filemanager import FM
from typing import Union
from core.util import to_uuid
from icalendar import Calendar, vDDDTypes, Component, vText
from icalendar.prop import vCategory
from datetime import date
from zoneinfo import ZoneInfo
import logging
from core.my_session import buildSession
from functools import cached_property

logger = logging.getLogger(__name__)


TZ_ZONE = 'Europe/Madrid'
NOW = datetime.now(tz=pytz.timezone(TZ_ZONE))


ICS_BEGIN = dedent(
    f'''
    BEGIN:VCALENDAR
    PRODID:-//Eventos//python3.10//ES
    VERSION:2.0
    CALSCALE:GREGORIAN
    METHOD:PUBLISH
    X-WR-TIMEZONE:{TZ_ZONE}
    '''
).strip()

ICS_END = "END:VCALENDAR"


def _fix_width(s: str, prefix: int):
    arr = []
    max_line = 70 - prefix
    while len(s) > max_line:
        arr.append(s[:max_line])
        s = s[max_line:]
        max_line = max_line + prefix
        prefix = 0
    if s:
        arr.append(s)
    return "\n ".join(arr)


@dataclass(frozen=True)
class SimpleIcsEvent:
    dtstamp: str
    uid: str
    url: str
    categories: str
    summary: str
    dtstart: str
    dtend: str
    description: str
    location: str
    organizer: str

    def __post_init__(self):
        for f, v in asdict(self).items():
            if f in ('dtstamp', 'dtstart', 'dtend'):
                object.__setattr__(self, f, self.parse_dt(f, v))
                continue
            f_parse = getattr(self, f'parse_{f}', None)
            if callable(f_parse):
                object.__setattr__(self, f, f_parse(v))

    def parse_dt(self, k: str, d: Union[datetime, str]):
        if isinstance(d, str):
            if not re.match(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}$", d):
                return d
            tz = pytz.timezone(TZ_ZONE)
            dt = datetime.strptime(d, "%Y-%m-%d %H:%M")
            d = tz.localize(dt)
        if d is None:
            if k != 'dtstamp':
                return None
            d = NOW

        return d.strftime('%Y%m%dT%H%M%S')

    def parse_uid(self, s: str):
        return to_uuid(s)

    def parse_description(self, s: str):
        desc = re.sub(r"\n", r"\\n", s)
        return desc

    def __str__(self):
        lines = ["BEGIN:VEVENT", "STATUS:CONFIRMED"]
        for k, v in asdict(self).items():
            if v is None:
                continue
            lines.append(f"{k.upper()}:{_fix_width(v, prefix=len(k)+1)}")
        lines.append("END:VEVENT")
        return "\n".join(lines)

    def __lt__(self, o: "SimpleIcsEvent"):
        return self.key_order < o.key_order

    @property
    def key_order(self):
        return (self.dtstart, self.dtend, self.uid)

    @staticmethod
    def dump(path, *events: "SimpleIcsEvent"):
        events = sorted(events)
        ics = ICS_BEGIN+"\n"+("\n".join(map(str, events)))+"\n"+ICS_END
        ics = re.sub(r"[\r\n]+", r"\r\n", ics)
        FM.dump(path, ics)

    def dumpme(self, path):
        SimpleIcsEvent.dump(path, self)


class IcsEventInvalid(ValueError):
    def __init__(self, msg: str):
        super().__init__(msg)


class IcsEventMandatory(ValueError):
    def __init__(self, field: str):
        super().__init__(f"Campo obligatorio {field} es None")


class IcsEventWrapper:
    def __init__(self, event: Component, source: str = None):
        self.__event = event
        self.__source = source

    @property
    def source(self):
        return self.__source

    def __str__(self):
        return str(self.__event)

    def __get_datetime(self, key: str, mandatory: bool = False) -> datetime | None:
        val = self.__event.get(key)
        if val is None:
            if mandatory:
                raise IcsEventMandatory(key)
            return None
        if not isinstance(val, vDDDTypes):
            raise IcsEventInvalid(f"Valor no es vDDDTypes: {val!r}")
        dt = val.dt
        if isinstance(dt, date) and not isinstance(dt, datetime):
            return datetime.combine(dt, datetime.min.time(), tzinfo=ZoneInfo(TZ_ZONE))
        if not isinstance(dt, datetime):
            raise IcsEventInvalid(f"Valor no es vDDDTypes con datetime: {val!r}")
        if dt.tzinfo is None:
            return dt.replace(tzinfo=ZoneInfo(TZ_ZONE))
        return dt.astimezone(tz=ZoneInfo(TZ_ZONE))

    def __get_text(self, key: str, mandatory: bool = False):
        val = self.__event.get(key)
        if val is None:
            if mandatory:
                raise IcsEventMandatory(key)
            return None
        if not isinstance(val, (vText, str)):
            raise IcsEventInvalid(f"Valor no es vText: {val!r}")
        s = str(val).strip()
        if len(s) == 0:
            return None
        return s

    @property
    def UID(self) -> str:
        return self.__get_text("UID", mandatory=True)

    @property
    def SUMMARY(self) -> str:
        return self.__get_text("SUMMARY", mandatory=True)

    @property
    def DTSTART(self) -> datetime:
        return self.__get_datetime("DTSTART", mandatory=True)

    @property
    def LOCATION(self) -> str:
        return self.__get_text("LOCATION", mandatory=True)

    @property
    def DTEND(self):
        return self.__get_datetime("DTEND")

    @property
    def CREATED(self):
        return self.__get_datetime("CREATED")

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
        return self.__get_text("ATTACH")

    @property
    def URL(self):
        return self.__get_text("URL")

    @property
    def DESCRIPTION(self):
        return self.__get_text("DESCRIPTION")

    @property
    def publish(self):
        p = None
        for k in ("DTSTAMP", "CREATED", "LAST-MODIFIED"):
            dt = self.__get_datetime(k)
            if dt is not None and dt <= NOW and (p is None or dt < p):
                p = dt
        return p

    @property
    def str_publish(self):
        if self.publish:
            return self.publish.strftime("%Y-%m-%d")


class IcsReader:
    def __init__(self, *urls: str, verify_ssl: bool = True):
        self.__urls = urls
        self.__s = buildSession()
        self.__verify_ssl = verify_ssl

    def __from_ical(self, url: str):
        r = self.__s.get(url, timeout=10, verify=self.__verify_ssl)
        try:
            r.raise_for_status()
        except Exception as e:
            logger.critical(f"Calendario status_code={r.status_code} {url} {e}", exc_info=True)
        if r.text is None:
            logger.warning(f"Calendario vació {url}")
            return None
        text = r.text.strip()
        if len(text) == 0:
            logger.warning(f"Calendario vació {url}")
            return None
        try:
            return Calendar.from_ical(text)
        except Exception as e:
            logger.critical(f"Calendario erróneo {url} {e}", exc_info=True)
        return None

    def __iter_events(self):
        for url in self.__urls:
            cal = self.__from_ical(url)
            if cal is not None:
                logger.info(f"Recuperando eventos de {url}")
                for e in cal.walk("VEVENT"):
                    e = IcsEventWrapper(e, source=url)
                    try:
                        if None not in (
                            e.UID,
                            e.DTSTART,
                            e.SUMMARY,
                            e.LOCATION
                        ):
                            yield e
                    except IcsEventMandatory as err:
                        logger.warning(f"{err} {url} {e}")
                        continue

    @cached_property
    def events(self):
        return tuple(self.__iter_events())


if __name__ == "__main__":
    ics = IcsReader(
        "https://fal.cnt.es/events/lista/?ical=1",
    )
    for e in ics.events:
        print(e.DTSTART, e.DTEND)
