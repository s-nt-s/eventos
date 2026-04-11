from portal.universidad import Universidad
from portal.eventim import Eventim
from core.event import Event
from core.cache import TupleCache


class Ucm:
    def __init__(self):
        self.__uni = Universidad(
            "https://eventos.ucm.es/ics/location/espana/lo-1.ics",
            verify_ssl=False,
        )
        self.__tim = Eventim("67349f8ab667c57a7581e251")

    @property
    @TupleCache("rec/ucm.json", builder=Event.build)
    def events(self):
        events: set[Event] = set()
        events.update(self.__uni.events)
        events.update(self.__tim.events)

        for e in list(events):
            ss = tuple(s for s in e.sessions if not s.full)
            if e.sessions != ss:
                events.remove(e)
                if ss:
                    events.add(e.merge(sessions=ss))
        return tuple(sorted(e.merge(id=f"ucm{e.id}") for e in events))


if __name__ == "__main__":
    Ucm().events
