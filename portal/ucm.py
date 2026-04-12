from portal.universidad import Universidad
from portal.eventim import Eventim
from core.event import Event, Place
from core.cache import TupleCache
from core.util import re_or
from core.zone import Zones
import re


def parse_place(p: Place):
    if re_or(
        p.name,
        ("Facultad", "Bellas Artes"),
        flags=re.I
    ):
        return Place(
            name="UCM Bellas artes",
            address=p.address,
            map="https://maps.app.goo.gl/GtpqE4qjc6L7Emsw5",
            latlon="40.43953915583213,-3.7330606614535937",
            zone=Zones.COMPLUTENSE.value.name
        )
    if re_or(
        p.name,
        ("Facultad", "Matemáticas"),
        flags=re.I
    ):
        return Place(
            name="UCM Matemáticas",
            address=p.address,
            zone=Zones.COMPLUTENSE.value.name
        )
    if re_or(
        p.name,
        ("Facultad", "Educación"),
        flags=re.I
    ):
        return Place(
            name="UCM Educación",
            address=p.address,
            zone=Zones.COMPLUTENSE.value.name
        )
    if re_or(
        p.name,
        ("Deportivo", "Zona Sur"),
        flags=re.I
    ):
        return Place(
            name="UCM Deportivo (Sur)",
            address=p.address,
            zone=Zones.COMPLUTENSE.value.name
        )
    if re_or(
        p.name,
        ("Centro", "Arte Complutense"),
        "c arte c",
        flags=re.I
    ):
        return Place(
            name="UCM Centro de Arte",
            address=p.address,
            zone=Zones.COMPLUTENSE.value.name
        )



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
        for e in self.__tim.events:
            pl = parse_place(e.place)
            if pl:
                e = e.merge(place=pl)
            events.add(e)

        for e in list(events):
            ss = tuple(s for s in e.sessions if not s.full)
            if e.sessions != ss:
                events.remove(e)
                if ss:
                    events.add(e.merge(
                        sessions=ss
                    ))
        return tuple(sorted(e.merge(id=f"ucm{e.id}") for e in events))


if __name__ == "__main__":
    Ucm().events
