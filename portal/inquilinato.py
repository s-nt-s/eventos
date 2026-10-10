from core.event import Event, Category, Session, Place
import logging
from functools import cached_property
from portal.base import Base
from core.wpics import WPIcs, Event as IEvent
from core.util import re_or
import re

logger = logging.getLogger(__name__)


class Inquilinato(Base):

    @cached_property
    def __wp(self):
        return WPIcs("https://inquilinato.org/agenda/")

    def _get_events(self):
        evs: set[Event] = set()
        for i in self.__wp.get_items():
            if re.match(r".*_R\d+T\d+@google\.com$", i.uid):
                continue
            p = self.__get_place(i)
            if p is None:
                continue
            e = Event(
                id=i.uid,
                url=self.__wp.url,
                name=i.title,
                img=None,
                price=0,
                category=self.__get_category(i),
                place=p.normalize(),
                duration=i.duration,
                sessions=tuple((
                    Session(date=s) for s in i.dates
                )),
            )
            evs.add(e)
        return tuple(sorted(evs))

    def __get_place(self, i: IEvent):
        if i.location is None:
            return None
        return Place(
            name=i.location,
            address=i.location
        )

    def __get_category(self, i: IEvent):
        if re_or(
            i.title,
            "GT Mañanas?",
            flags=re.I
        ):
            return Category.NO_EVENT
        if re_or(
            i.title,
            r"formaci[oó]n(es)?",
            flags=re.I
        ):
            return Category.WORKSHOP
        return Category.ACTIVISM


if __name__ == "__main__":
    from core.log import config_log
    config_log("log/inquilinato.log", log_level=logging.INFO)
    g = Inquilinato()
    print(len(g.get_events()))
