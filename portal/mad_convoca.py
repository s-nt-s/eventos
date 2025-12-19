from portal.gancio import GancioPortal
from portal.icsevent import IcsToEvent
from core.event import Event
from functools import cached_property
from core.util import plain_text, find_duplicates


class MadConvoca:
    def __init__(self):
        self.__gancio = GancioPortal(root="https://mad.convoca.la")
        self.__fal = IcsToEvent(
            "https://fal.cnt.es/events/lista/?ical=1"
        )

    @cached_property
    def events(self):
        ok_events = set(self.__gancio.events).union(self.__fal.events)
        ok_events = set(Event.fusionIfSimilar(
            ok_events,
            ('name', 'place')
        ))

        def _mk_key_mame_place(e: Event):
            return (e.place, plain_text(e.name))

        ok_events = set()
        for evs in find_duplicates(
            ok_events,
            _mk_key_mame_place
        ):
            for e in evs:
                ok_events.remove(e)
            e = Event.fusion(*evs)
            ok_events.add(e)

        return tuple(sorted(ok_events))
