from portal.universidad import Universidad
from portal.eventim import Eventim
from core.event import Event, Place
from core.cache import TupleCache
from core.util import re_or, get_domain
from core.zone import Zones
import re


dom_eventim = "eventim-light.com"


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
            map="https://maps.app.goo.gl/b87tstQr6M5aRtdJ7",
            latlon="40.449769018450226,-3.725813888434875",
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
            latlon="40.451000941293515,-3.7177499307621042",
            map="https://maps.app.goo.gl/wfrrsQfadSR3NX7a8",
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
            latlon="40.438861452263204,-3.7310277461082375",
            map="https://maps.app.goo.gl/c7b7pQQb1nH1sQ968",
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
            latlon='40.44047337583415,-3.7290323134909302',
            map="https://maps.app.goo.gl/2P7np7abqA1hTbBj8",
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
            events.remove(e)
            ss = tuple(s for s in e.sessions if not s.full)
            if len(ss) == 1 and get_domain(ss[0].url) == dom_eventim and get_domain(e.url) in (None, dom_eventim):
                e = e.merge(
                    url=ss[0].url,
                    sessions=(
                        ss[0]._replace(url=None),
                    )
                )
            elif e.sessions != ss:
                e = e.merge(sessions=ss)
            if e.sessions:
                events.add(e)
        return tuple(sorted(e.merge(id=f"ucm{e.id}") for e in events))


if __name__ == "__main__":
    Ucm().events
