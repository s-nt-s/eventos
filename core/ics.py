from textwrap import dedent
from datetime import datetime
import pytz
from dataclasses import dataclass, asdict
import re
from .filemanager import FM
from typing import Union
from .util import to_uuid


ICS_BEGIN = dedent(
    '''
    BEGIN:VCALENDAR
    PRODID:-//Eventos//python3.10//ES
    VERSION:2.0
    CALSCALE:GREGORIAN
    METHOD:PUBLISH
    X-WR-TIMEZONE:Europe/Madrid
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
class IcsEvent:
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
            tz = pytz.timezone('Europe/Madrid')
            dt = datetime.strptime(d, "%Y-%m-%d %H:%M")
            d = tz.localize(dt)
        if d is None:
            if k != 'dtstamp':
                return None
            d = datetime.now(tz=pytz.timezone('Europe/Madrid'))

        d_utc = d.astimezone(pytz.UTC)
        return d_utc.strftime('%Y%m%dT%H%M%SZ')

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

    def __lt__(self, o: "IcsEvent"):
        return self.key_order < o.key_order

    @property
    def key_order(self):
        return (self.dtstart, self.dtend, self.uid)

    @staticmethod
    def dump(path, *events: "IcsEvent"):
        events = sorted(events)
        ics = ICS_BEGIN+"\n"+("\n".join(map(str, events)))+"\n"+ICS_END
        ics = re.sub(r"[\r\n]+", r"\r\n", ics)
        FM.dump(path, ics)

    def dumpme(self, path):
        IcsEvent.dump(path, self)
