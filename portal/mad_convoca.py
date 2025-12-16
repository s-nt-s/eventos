from portal.gancio import GancioPortal
from portal.icsevent import IcsToEvent
from core.event import Event
from functools import cached_property


class MadConvoca:
    def __init__(self):
        self.__gancio = GancioPortal(root="https://mad.convoca.la")
        self.__fal = IcsToEvent(
            "https://fal.cnt.es/events/lista/?ical=1"
        )

    @cached_property
    def events(self):
        all_events = set(self.__gancio.events).union(self.__fal.events)
        events = Event.fusionIfSimilar(
            all_events,
            ('name', 'place')
        )
        return events
