from core.apiesmadrid import EsMadridEvent, ApiEsMadrid
from core.event import Category, Place
from core.zone import Circles
from functools import cache
import logging
from typing import Callable

logger = logging.getLogger(__name__)


class EsMadrid:
    def __init__(
        self,
        max_price: float,
        categories: tuple[Category, ...],
        max_sessions: int,
        isOkPlace: Callable[[Place | tuple[float, float] | str], bool] = None
    ):
        self.__api = ApiEsMadrid()
        self.__max_price = max_price
        self.__categories = categories
        self.__max_sessions = max_sessions
        self.__isOkPlace = isOkPlace or (lambda *_: True)

    def _get_events(self):
        evs: list[EsMadridEvent] = []
        for e in self.__api.get_events():
            if None not in (self.__max_price, e.price) and e.price > self.__max_price:
                continue
            if None not in (e.latitude, e.longitude) and not self.__isOkPlace((e.latitude, e.longitude)):
                continue
            if self.__max_sessions is not None and self.__max_sessions < len(e.dates):
                continue
            evs.append(e)
        return tuple(evs)


if __name__ == "__main__":
    e = EsMadrid(
        max_price=10,
        categories=tuple(),
        max_sessions=5,
    )
    evs = e._get_events()
    for e in evs:
        print(e.web)
