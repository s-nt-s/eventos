from core.event import Event
from abc import abstractmethod
from core.filemanager import FM
import time
import os
import logging
import requests

logger = logging.getLogger(__name__)


def safe_json(url: str) -> list[dict] | None:
    try:
        r = requests.get(url)
        r.raise_for_status()
        data = r.json()
        if not isinstance(data, list):
            logger.critical(f"NOT list {url}")
            return None
        if not all(isinstance(i, dict) for i in data):
            logger.critical(f"NOT list[dict] {url}")
            return None
        return data
    except (requests.RequestException, ValueError):
        logger.critical(f"NOT JSON {url}")
        return None


class Base:
    def __init__(self, cache: str | bool = True, cache_ttl: int = 3):
        self.__out = FM.resolve_path(os.environ.get("PAGE_OUT"))
        self.__site = os.environ["PAGE_URL"]
        if cache is True:
            cache = f"events/{self.__class__.__name__}.json"
        if cache is False:
            cache = None
        if isinstance(cache, str):
            cache = str(self.__out / cache)
        self.__cache = FM.resolve_path(
            cache
        )
        self.__cache_ttl = 0 if cache_ttl is None else time.time() - (cache_ttl * 86400)

    @abstractmethod
    def _get_events(self) -> tuple[Event, ...]:
        raise NotImplementedError()

    def __load_cache(self):
        if self.__cache and self.__cache.exists():
            if os.stat(self.__cache).st_mtime < self.__cache_ttl:
                return None
            data = FM.load(self.__cache)
            if isinstance(data, list):
                return tuple((e for e in map(Event.build, data) if e is not None))

    def __dump_cache(self, data: tuple[Event, ...]):
        if self.__cache:
            FM.dump(self.__cache, data)

    def get_events(self):
        data = self.__load_cache()
        if data is not None:
            logger.info(f"{self.__class__.__name__} = {len(data)} eventos")
            return data
        logger.info(f"{self.__class__.__name__} buscando eventos ")
        data = self._get_events()
        logger.info(f"{self.__class__.__name__} {len(data)} eventos encontrados")
        self.__dump_cache(data)
        return data

    def safe_get_events(self, *ex: Exception) -> tuple[Event, ...]:
        if len(ex) == 0:
            raise ValueError()
        try:
            return self.get_events()
        except ex as e:
            logger.critical(str(e))
        if self.__cache is not None and self.__cache.is_relative_to(self.__out):
            url = f"{self.__site}/{self.__cache.relative_to(self.__out)}"
            data = safe_json(url)
            if data is not None:
                logger.info(f"Recuperando de la versión anterior {url}")
                data = tuple(map(Event.build, data))
                self.__dump_cache(data)
                return data
        return tuple()
