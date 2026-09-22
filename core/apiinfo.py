import requests
from os import environ
from core.event import Event, Cinema, Category
from core.util import parse_obj, get_obj
import json
import logging
from typing import NamedTuple

logger = logging.getLogger(__name__)


class Info(NamedTuple):
    id: str
    ideologia: int
    recurrente: bool
    descripcion: str
    comic: bool
    ensayo: bool
    novela: bool
    politica: bool
    fiesta: bool
    conferencia: bool
    teatro: bool
    infantil: bool
    musica: bool
    poesia: bool
    taller: bool

    @classmethod
    def build(cls, *args, **kwargs):
        obj = get_obj(*args, **kwargs)
        if obj is None:
            return None
        obj = {k: obj.get(k) for k in cls._fields}
        if len(obj) == 0:
            return None
        return cls(**obj)


class ApiInfo:
    def __init__(self, key: str, url: str):
        self.__root = url
        self.__s = requests.Session()
        self.__s.headers.update({
            "Authorization": f"Bearer {key}"
        })

    @classmethod
    def build(cls, env_key, env_url: str):
        key = environ.get(env_key)
        url = environ.get(env_url)
        if None in (key, url):
            logger.info(f"Debe definir {env_key} y {env_url}")
            return None
        return cls(key, url)

    def __get_info(self, *events: Event | Cinema) -> dict[str, Info]:
        info: dict[str, Info] = {}
        if len(events) == 0:
            return info
        ids = set((e.id for e in events))
        logger.info(f"Obteniendo información de {len(events)} eventos")
        payload = json.dumps(
            parse_obj(events, compact=True),
            ensure_ascii=False,
            indent=0,
            separators=(',', ':')
        )
        try:
            r = self.__s.get(
                self.__root,
                params={"ask": payload}
            )
            data = r.json()
            if not isinstance(data, dict):
                logger.critical(f"response is not a dict ({type(data)})")
                return info
            rpl = data.get("reply")
            error = data.get("error")
            if error or not isinstance(rpl, list):
                logger.critical(f"response error {data}")
                return info
            for i in rpl:
                if not isinstance(i, dict) or i.get("id") is None:
                    logger.critical(f"response error {data}")
                    return None
                if i['id'] not in ids:
                    continue
                d = Info.build(i)
                if d:
                    info[i['id']] = d
        except Exception as e:
            logger.critical(str(e))
        logger.info(f"Información de {len(info)} eventos recuperada con éxito")
        return data

    def complete(self, *events: Event | Cinema):
        done: set[Event | Cinema] = set()
        need_info: list[Event | Cinema] = []
        for e in events:
            if e.category in (
                Category.UNKNOWN,
                Category.LITERATURE,
                Category.CONFERENCE,
                Category.READING_CLUB
            ):
                need_info.append(e)
                continue
            done.add(e)
        ok = self.__complete(*need_info)
        return tuple(sorted(done.union(ok)))

    def __complete(self, *events: Event | Cinema):
        info = self.__get_info(*events)
        if info is None:
            return None
        evs = set(events)
        for e in list(evs):
            i = info.get(e.id)
            if i is None:
                continue
            evs.remove(e)
            if e.category in (Category.UNKNOWN, ):
                nc = self.__find_category(e, i)
                if nc and nc != e.category:
                    logger.info(f"{e.id} {e.category} -> {nc} {e.url}")
                    e = e.merge(category=nc)
            evs.add(e)

        return tuple(sorted(evs))

    def __find_category(self, e: Event | Cinema, i: Info):
        if i.infantil:
            return Category.CHILDISH
        maybeBook = (Category.UNKNOWN, Category.LITERATURE, Category.CONFERENCE, Category.READING_CLUB)

        if maybeBook and not i.comic and not i.ensayo:
            if i.poesia:
                return Category.POETRY
            if i.novela:
                return Category.NARRATIVE
        if i.taller:
            return Category.WORKSHOP
        if i.teatro:
            return Category.THEATER


if __name__ == "__main__":
    from core.log import config_log
    config_log("log/apiinfo.log", log_level=logging.INFO)
    from core.filemanager import FM
    evs: list[Event | Cinema] = []
    for i in FM.load("out/eventos.json"):
        e = Event.build(i, fill_with_none=True)
        evs.append(e)
    a = ApiInfo.build("API_INFO_KEY", "API_INFO_URL")
    a.complete(*evs)
