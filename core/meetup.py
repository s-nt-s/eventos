from core.ics import IcsReader, IcsEventWrapper
from portal.base import Base
from core.util import re_or, find_euros
from core.event import Event, Place, Session, Category, CategoryUnknown
import logging
import re

logger = logging.getLogger(__name__)


class MeetUp(Base):
    def __init__(
        self,
        *groups: str,
        cache: str | bool = True,
    ):
        super().__init__(cache=cache)
        self.__ics = IcsReader(*(
          f"http://www.meetup.com/{g}/events/ical" for g in groups
        ))

    def _get_events(self):
        done: set[str] = set()
        ok_events: set[Event] = set()
        for e in self.__ics.events:
            if e.UID in done:
                continue
            done.add(e.UID)
            event = self.__ics_to_event(e)
            if event:
                ok_events.add(event)

        rt = tuple(sorted(e.merge(id=f"mtp{e.id}") for e in ok_events))
        return rt

    def __ics_to_event(self, e: IcsEventWrapper):
        if e.SUMMARY is None:
            return
        place = self.__find_ics_place(e)
        if place is None:
            logger.critical(f"NOT FOUND PLACE")
            #return
        #place = place.normalize()
        event = Event(
            id=e.UID,
            url=e.URL,
            name=e.SUMMARY,
            duration=e.duration or 60,
            img=e.ATTACH,
            price=self.__find_ics_price(e),
            category=self.__find_ics_category(e),
            place=place,
            sessions=(
                Session(
                    date=e.DTSTART.strftime("%Y-%m-%d %H:%M"),
                ),
            ),
        )
        return event

    def __find_ics_place(self, e: IcsEventWrapper):
        if e.LOCATION:
            return Place(
                name=e.LOCATION,
                address=e.LOCATION
            )


    def __find_ics_price(self, e: IcsEventWrapper):
        prc = find_euros(e.DESCRIPTION)
        if prc is not None:
            return prc
        return 0

    def __find_ics_category(self, e: IcsEventWrapper):
        def _has_cat(*args):
            for c in e.CATEGORIES:
                if re_or(c, *args, flags=re.I):
                    return True
            return False

        if e.CATEGORIES:
            logger.critical(str(CategoryUnknown(e.source, f"{e.CATEGORIES} -- {e.SUMMARY}")))
        else:
            logger.critical(str(CategoryUnknown(e.source, f"{e}")))
        return Category.UNKNOWN


if __name__ == "__main__":
    from core.log import config_log
    config_log("log/meetup.log", log_level=logging.INFO)
    m = MeetUp(
        "club-de-lectura-de-ensayo",
        "madrid-independent-cinema-group",
        cache=False,
    )
    evs = m.get_events()
    print("")
    print(*evs, sep="\n")