from portal.gancio import GancioPortal
from portal.icsevent import IcsToEvent
from core.event import Event
from functools import cached_property
from core.util import plain_text, find_duplicates
import re
import logging

logger = logging.getLogger(__name__)

re_sp = re.compile(r"\s+")


class MadConvoca:
    def __init__(self):
        self.__gancio = GancioPortal(root="https://mad.convoca.la", id_prefix="")
        self.__fal = IcsToEvent(
            "https://fal.cnt.es/events/lista/?ical=1"
        )

    @cached_property
    def events(self):
        logger.info("Buscando eventos en MadConvoca")
        ok_events = set(self.__gancio.events).union(self.__fal.events)
        ok_events = set(Event.fusionIfSimilar(
            ok_events,
            ('name', 'place')
        ))

        def _mk_key_mame_place(e: Event):
            name = plain_text(e.name) or ''
            compact = re_sp.sub("", name)
            only_w = re.sub(r"\w", "", compact)
            for s in (only_w, compact, name):
                if len(s) > 10:
                    return (e.place, s)

        for evs in find_duplicates(
            ok_events,
            _mk_key_mame_place
        ):
            for e in evs:
                ok_events.remove(e)
            e = Event.fusion(*evs)
            ok_events.add(e)

        return tuple(sorted(e.merge(id=f"mc{e.id}") for e in ok_events))
