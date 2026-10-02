from typing import NamedTuple
from requests import get
import logging
from os import environ
from datetime import datetime
from zoneinfo import ZoneInfo
from requests.exceptions import JSONDecodeError
from time import sleep


logger = logging.getLogger(__name__)


def to_date(s: str):
    dt = datetime.strptime(s, "%Y-%m-%d %H:%M")
    dt = dt.replace(tzinfo=ZoneInfo("Europe/Madrid"))
    return dt


def _safe_get_int(env_url: str, tries=3) -> list:
    url = environ.get(env_url)
    if not url:
        return tuple()
    r = get(url)
    if r.status_code == 404 and tries < 3:
        sleep(2)
        return _safe_get_int(env_url, tries + 1)
    if r.status_code != 200:
        logger.critical(f"{env_url} status_code={r.status_code}")
        return []
    js = None
    try:
        js = r.json()
    except JSONDecodeError:
        logger.critical(f"{env_url} NOT JSON")
        return []
    if not isinstance(js, list):
        logger.critical(f"{env_url} {js}")
        return []
    return js


class Interval(NamedTuple):
    start: datetime
    end: datetime

    def is_in(self, dt: datetime) -> bool:
        if dt is None:
            return False
        if not isinstance(dt, datetime):
            raise ValueError(dt)
        return self.start <= dt <= self.end

    @classmethod
    def safe_load(cls, env_url: str) -> tuple["Interval"]:
        vals: set["Interval"] = set()
        for i in _safe_get_int(env_url):
            if not isinstance(i, list):
                continue
            if len(i) != 2:
                continue
            vals.add(Interval(start=to_date(i[0]), end=to_date(i[1])))
        logger.info(f"{env_url} {len(vals)} intervals")
        return tuple(sorted(vals))
