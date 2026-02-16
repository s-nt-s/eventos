from requests import Session
from core.cache import HashCache
from datetime import datetime
from zoneinfo import ZoneInfo
from core.filemanager import FM
import re
from typing import NamedTuple
from core.util import get_obj, trim


class Event(NamedTuple):
    id: int
    name: str
    permalink: str
    start: int
    end: int
    event_subtitle: str
    image_url: str
    location_name: str
    location_address: str
    location_lat: float
    location_lon: float
    event_types: tuple[str, ...]
    customfields: tuple[str, ...]
    repeats: tuple[tuple[int, int], ...]
    content: str
    details: str
    year_long_event: bool
    month_long_event: bool
    all_day_event: bool

    @staticmethod
    def build(*args, **kwargs):
        obj = get_obj(*args, **kwargs)
        if obj is None:
            return None
        r = obj.get('repeats') or []
        r = tuple(tuple(i) for i in r)
        obj['repeats'] = r
        for k, v in list(obj.items()):
            if isinstance(v, list):
                obj[k] = tuple(v)
        return Event(**obj)


def is_past(start_end: list[list[int]]) -> bool:
    timestamp = 0
    for s_e in start_end:
        for x in s_e:
            if x is not None and x > timestamp:
                timestamp = x
    if timestamp == 0:
        return False
    tz = ZoneInfo("Europe/Madrid")
    event_dt = datetime.fromtimestamp(timestamp, tz=tz)
    now_madrid = datetime.now(tz=tz)
    return event_dt < now_madrid


def extract_prefix(url: str, obj: dict, prefix: str):
    r = re.compile(r"^" + prefix + r"_(\d+)$")
    for k, v in list(obj.items()):
        ord = None
        if k == prefix:
            ord = 1
        else:
            m = r.match(k)
            if m:
                ord = int(m.group(1))
        if ord is not None:
            del obj[k]
            if not isinstance(v, dict):
                raise ValueError(f"{url} {k}!=dict")
            yield ord, v


class EventOn:
    def __init__(self, root: str):
        self.__s = Session()
        self.__root = root.rstrip("/")

    def __get_json(self, url: str):
        r = self.__s.get(url)
        js = r.json()
        return js

    def __get_dict(self, url: str):
        js = self.__get_json(url)
        if not isinstance(js, dict):
            raise ValueError(f"{url} is not a dict")
        return js

    @HashCache("rec/eventon/{}.json")
    def __get_eventon(self, url: str):
        js = self.__get_dict(url)
        events = js.get("events")
        if not isinstance(events, dict):
            raise ValueError(f"{url} is not a eventon endpoint")
        obj = []
        for k, v in events.items():
            if not isinstance(v, dict):
                raise ValueError(f"{url} is not a eventon endpoint")
            post_status = v.get('post_status')
            if not isinstance(post_status, str):
                raise ValueError(f"{url} is not a eventon endpoint")
            if post_status != "publish":
                continue
            del v['post_status']
            for x in ('start', 'end'):
                v[x] = int(v[x])
            repeats = v.get('repeats') or []
            if not isinstance(repeats, list):
                raise ValueError(f"{url} repeats is not a list")
            for i, r in enumerate(repeats):
                repeats[i] = list(map(int, r))
            v['repeats'] = repeats
            if is_past([[v['start'], v['end']]] + repeats):
                continue
            customfields = []
            event_type = []
            for c_id, kv in extract_prefix(url, v, "customfield"):
                if kv.get('x') != c_id:
                    raise ValueError(f"{url} x = {kv.get('x')} != {c_id}")
                if kv.get('value') not in ('Whatsapp', 'whatsapp'):
                    customfields.append(kv)
            for c_id, kv in extract_prefix(url, v, "event_type"):
                event_type.append(kv)
            v['customfields'] = customfields
            v['event_types'] = event_type
            v['_id_'] = int(k)
            obj.append(v)

        def _yes_no(yn):
            if yn is None:
                return None
            if yn not in ("yes", "no"):
                raise ValueError(f"{yn} not in (yes, no)")
            return yn == "yes"

        def _re_parse(o):
            if not isinstance(o, dict):
                return None
            for cast, fields in {
                float: ('location_lat', 'location_lon'),
                _yes_no: ('featured', 'year_long_event', 'month_long_event', 'all_day_event', '_target')
            }.items():
                for k in fields:
                    v = o.get(k)
                    if v is not None:
                        o[k] = cast(v)

        obj: list[dict] = FM.parse_obj(obj, compact=True, re_parse=_re_parse)
        return obj

    def get_eventon(self):
        events: set[Event] = set()
        for e in self.__get_eventon(f"{self.__root}/eventon/events"):
            repeats = e.get('repeats') or []
            repeats = tuple(tuple(i) for i in repeats)
            event_types: list[str] = []
            for et in e['event_types']:
                for t in et.values():
                    if t not in event_types:
                        event_types.append(t)
            x = Event(
                id=e['_id_'],
                name=trim(e['name']),
                permalink=trim(e['permalink']),
                start=e['start'],
                end=e['end'],
                event_subtitle=trim(e['event_subtitle']),
                image_url=trim(e['image_url']),
                location_name=trim(e['location_name']),
                location_address=trim(e['location_address']),
                location_lat=e.get('location_lat'),
                location_lon=e.get('location_lon'),
                event_types=tuple(event_types),
                customfields=tuple(i['value'] for i in e['customfields']),
                content=trim(e['content']),
                details=trim(e['details']),
                repeats=repeats,
                year_long_event=e['year_long_event'],
                month_long_event=e['month_long_event'],
                all_day_event=e['all_day_event'],
            )
            events.add(x)
        return tuple(sorted(events, key=lambda x: x.id))


if __name__ == "__main__":
    ev = EventOn("https://culturalcala.es/wp-json")
    print(len(ev.get_eventon()))
