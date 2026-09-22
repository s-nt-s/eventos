from requests import Session
from os.path import isfile
import re
from os import environ
from core.event import Event
from datetime import datetime
from core.util import clean_url, normalize_url, get_domain
import logging
import pytz


logger = logging.getLogger(__name__)
_S = Session()

DT_NOW = datetime.now(tz=pytz.timezone('Europe/Madrid'))
NOW = DT_NOW.strftime("%Y-%m-%d")


def url_to_key(url: str):
    #if get_domain(url) == "madrid.es":
    #    vgnextoid = get_query(url).get("vgnextoid")
    #    if isinstance(vgnextoid, str) and vgnextoid:
    #        return vgnextoid
    url = normalize_url(clean_url(url))
    if get_domain(url) == "tienda.madrid-destino.com":
        url = re.sub(r"/mapa/?$", "", url)
    k = str(url)
    #k = re.sub(r"^https?://(www\.)?", "", url)
    k = k.rstrip("/")
    return k


def re_url_to_key(line: str):
    return re.sub(r"\bhttps?://\S+", lambda x: url_to_key(x.group()), line)


def safe_json(url: str):
    try:
        r = _S.get(url)
        return r.json()
    except Exception:
        return {}


class DictFile(dict):
    def __init__(
        self,
        name: str,
        local: str,
        remote: str
    ):
        self.__name = name
        self.__local = local
        self.__remote = remote
        self.__load()

    @property
    def local(self):
        return f"{self.__local}/{self.__name}"

    @property
    def remote(self):
        return f"{self.__remote}/{self.__name}"

    def __load(self):
        txt = self.__read().strip()
        txt = re_url_to_key(txt)
        for ln in map(str.strip, re.split(r"\n", txt)):
            spl = ln.split(None, 1)
            if len(spl) == 2:
                v, k = spl
                if v < NOW:
                    self[k] = v

    def dump(self):
        with open(self.local, "w") as f:
            for k, v in self.items():
                if None not in (k, v) and v <= NOW:
                    f.write(f"{v} {k}\n")

    def __read(self):
        if isfile(self.local):
            with open(self.local, "r") as f:
                return f.read()
        try:
            r = _S.get(self.remote)
            return r.text
        except Exception as e:
            logger.debug(f"{self.remote} {e}")
        return ''


class PublishDB:
    def __init__(
        self,
        name: str,
        remote: str,
        local: str,
    ):
        self.__data = DictFile(
            name=name,
            remote=remote,
            local=local
        )

    def items(self):
        return self.__data.items()

    def __iter_keys(self, e: Event):
        for s in e.sessions:
            if e.url:
                yield f"{s.date} {url_to_key(e.url)}"
            if s.url:
                yield f"{s.date} {url_to_key(s.url)}"

    def set(self, e: Event):
        for k in self.__iter_keys(e):
            if k not in self.__data:
                self.__data[k] = NOW

    def get(self, e: Event):
        self.set(e)
        dates: set[str] = set()
        for k in self.__iter_keys(e):
            dt = self.__data[k]
            dates.add(dt)
        if len(dates) == 0:
            return NOW
        return max(dates)

    def dump(self):
        self.__data.dump()


if __name__ == "__main__":
    from core.filemanager import FM
    from core.event import Event
    evs = tuple(map(Event.build, FM.load("rec/events.json")))
    PUBLISHDB = PublishDB(
        name="publish.txt",
        local="out/",
        remote=environ['PAGE_URL'],
        backup=environ['PAGE_URL']+"/publish.json"
    )
    print(PUBLISHDB.get(evs[0]))
