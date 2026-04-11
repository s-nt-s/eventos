from core.eventim import EventimApi, Item
from core.event import Event, Place, Session, CategoryUnknown, Category
from datetime import datetime
import logging
from core.util import re_or
import re

logger = logging.getLogger(__name__)


class Eventim:
    def __init__(self, id: str):
        self.__api = EventimApi(id)

    @property
    def id(self):
        return self.__api.id

    @property
    def events(self):
        events: set[Event] = set()
        for i in self.__api.get_info():
            d, ss = self.__get_duration_sessions(i)
            e = Event(
                id=f"tim{self.__api.id}{i.seriesId or i.id}",
                name=i.title,
                url=i.get_serie_url() or i.get_url(),
                price=i.price,
                category=self.__find_category(i),
                place=Place(
                    name=i.place.name,
                    address=i.place.adders
                ).normalize(),
                duration=d,
                img=i.image,
                sessions=ss,
            )
            events.add(e)
        return Event.fusionIfSimilar(events, ('id', ))

    def __get_duration_sessions(self, i: Item):
        s = datetime.strptime(i.start, "%Y-%m-%d %H:%M")
        e = datetime.strptime(i.end, "%Y-%m-%d %H:%M")
        duration = int((e-s).total_seconds() // 60)
        ss = Session(
            date=i.start,
            url=i.get_url() if i.seriesId else None,
            full=i.soldout
        )
        return duration, (ss, )

    def __find_category(self, i: Item):
        if re_or(
            i.category,
            "m[úu]sica",
            flags=re.I
        ):
            return Category.MUSIC
        if re_or(
            i.category,
            "visita",
            flags=re.I
        ):
            return Category.VISIT
        logger.critical(str(CategoryUnknown(i.id, f"category={i.category}")))
        return Category.UNKNOWN


if __name__ == "__main__":
    Eventim("67349f8ab667c57a7581e251").events