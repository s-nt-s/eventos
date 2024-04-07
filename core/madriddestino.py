from .web import Driver
from typing import Set, Dict
from functools import cached_property, cache
import logging
from .cache import Cache
import json
from .event import Event, Session, Place, Category
from .cache import TupleCache
from datetime import datetime, timezone, timedelta
import re


logger = logging.getLogger(__name__)


class MadridDestinoException(Exception):
    pass


def timestamp_to_date(timestamp):
    tz = timezone(timedelta(hours=2))
    d = datetime.fromtimestamp(timestamp, tz)
    return d.strftime("%Y-%m-%d %H:%M")


class MadridDestino:
    URL = "https://tienda.madrid-destino.com/es"

    @Cache("rec/madriddestino/state.json")
    def __get_state(self) -> Dict:
        with Driver(browser="firefox") as f:
            f.get(MadridDestino.URL)
            f.wait_ready()
            js = f.execute_script("return JSON.stringify(window.__NUXT__.state)")
            obj = json.loads(js)
            if not isinstance(obj, dict) or obj.get("errorApi") is True:
                raise ValueError(obj)
            for k, v in list(obj.items()):
                if v in (None, [], '', {}):
                    del obj[k]
            return obj

    @cached_property
    def state(self):
        return self.__get_state()

    @property
    @TupleCache("rec/madriddestino.json", builder=Event.build)
    def events(self):
        events: Set[Event] = set()
        for e in self.state['events']:
            if len(e['eventCategories']) == 0:
                continue
            if e['freeCapacity'] == 0:
                continue
            logger.debug("event.id="+str(e['id']))
            org = self.__find("organizations", e['organization_id'])
            events.add(Event(
                id="md"+str(e['id']),
                url=MadridDestino.URL+'/'+org['slug']+'/'+e['slug'],
                name=e['title'],
                img=e['featuredImage']['url'],
                price=e['highestPrice'],
                category=self.__find_category(e),
                place=self.__find_place(e),
                sessions=self.__find_sessions(e)
            ))
        return tuple(sorted(events))

    def __find_place(self, e: Dict):
        space_id = set()
        for s in e['rooms']:
            space_id.add(s['space_id'])
        if len(space_id) == 0:
            raise MadridDestinoException(f"Unknown place in {e['id']}")
        if len(space_id) > 1:
            raise MadridDestinoException(f"Indeterminate place in {e['id']}: " + ", ".join(sorted(space_id)))
        space = self.__find("spaces", space_id.pop())
        return Place(
            name=re.sub(r"\s+Madrid$", "", space['name']),
            address=space['address']
        )

    def __find_sessions(self, e: Dict):
        sessions: Set[Session] = set()
        for s in e['uAvailableDates']:
            sessions.add(Session(
                date=timestamp_to_date(s)
            ))
        return tuple(sorted(sessions))

    @cache
    def __find(self, k: str, id: int):
        for i in self.state[k]:
            if isinstance(i, dict) and i.get('id') == id:
                return i
        raise MadridDestinoException(f"NOT FOUND {k}.id={id}")

    def __find_category(self, e: Dict):
        cats: Set[str] = set()
        for c in self.state['categories']:
            if c['id'] in e['eventCategories']:
                cats.add(c['label'].lower())
            for ch in c.get('children', []):
                if ch['id'] in e['eventCategories']:
                    cats.add(ch['label'].lower())
                    cats.add(c['label'].lower())
        for c in list(cats):
            if " / " in c:
                cats = cats.union(c.split(" / "))
        if "cine" in cats:
            return Category.CINEMA
        if "concierto" in cats:
            return Category.CONCERT
        if "circo" in cats:
            return Category.CIRCUS
        if "taller" in cats:
            return Category.WORKSHOP
        if "danza" in cats:
            return Category.DANCE
        if "títeres" in cats:
            return Category.PUPPETRY
        if "teatro" in cats:
            return Category.THEATER
        if cats.intersection(("pintura", "exposición")):
            return Category.EXPO
        if "conferencia" in cats:
            return Category.CONFERENCE
        if "música" in cats:
            return Category.CONCERT
        if "visitas" in cats:
            return Category.VISIT
        if e['id'] == 3706:
            return Category.CONCERT
        raise MadridDestinoException(f"Unknown category in {e['id']}: " + ", ".join(sorted(cats)))


if __name__ == "__main__":
    from .log import config_log
    config_log("log/madriddestino.log", log_level=(logging.DEBUG))
    print(MadridDestino().events)
