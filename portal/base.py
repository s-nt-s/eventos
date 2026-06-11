from core.event import Event
from abc import abstractmethod
from core.filemanager import FM
import time
import os
import logging

logger = logging.getLogger(__name__)


class Base:
    def __init__(self, cache: str | bool = True, cache_ttl: int = 3):
        if cache is True:
            cache = f"out/events/{self.__class__.__name__}.json"
        if cache is False:
            cache = None
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

    def safe_get_events(self, *ex: Exception):
        if len(ex) == 0:
            return ValueError()
        try:
            return self.get_events()
        except ex as e:
            logger.critical(str(e))
        return tuple()