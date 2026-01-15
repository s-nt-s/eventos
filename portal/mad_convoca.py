from portal.gancio import GancioPortal
from core.ics import IcsReader, IcsEventWrapper
from core.event import Event, Place, Category, Session, CategoryUnknown
from functools import cached_property
from core.util import plain_text, find_duplicates, re_or, re_and
import re
import logging

logger = logging.getLogger(__name__)

re_sp = re.compile(r"\s+")


class MadConvoca:
    def __init__(self):
        self.__mad = GancioPortal(
            root="https://mad.convoca.la",
            id_prefix=""
        )
        self.__ext = GancioPortal(
            root="https://calendario.extinctionrebellion.es",
            id_prefix="ex"
        )
        self.__ics = IcsReader(
            "https://fal.cnt.es/events/lista/?ical=1",
            "https://lahorizontal.net/events/lista/?ical=1"
        )

    @cached_property
    def events(self):
        logger.info("Buscando eventos en MadConvoca")
        ok_events = set(self.__mad.events).union(self.__ext.events)
        for e in self.__ics.events:
            event = Event(
                id=e.UID,
                url=e.URL,
                name=e.SUMMARY,
                duration=e.duration or 60,
                img=e.ATTACH,
                price=0,
                publish=e.str_publish,
                category=self.__find_category(e),
                place=Place(
                    name=e.LOCATION,
                    address=e.LOCATION
                ),
                sessions=(
                    Session(
                        date=e.DTSTART.strftime("%Y-%m-%d %H:%M"),
                    ),
                ),
            )
            ok_events.add(event)
        ok_events = set(Event.fusionIfSimilar(
            ok_events,
            ('name', 'place')
        ))

        def _mk_key_mame_place(e: Event):
            name = plain_text(e.name) or ''
            compact = re_sp.sub("", name)
            only_w = re.sub(r"\W", "", compact)
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

        rt = tuple(sorted(e.merge(id=f"mc{e.id}") for e in ok_events))
        logger.info(f"Buscando eventos en MadConvoca = {len(rt)}")
        return rt

    def __find_category(self, e: IcsEventWrapper):
        if re_and(e.SUMMARY, "presentaci[oó]n del?", ("libro", "novela"), flags=re.I, to_log=e.UID):
            return Category.LITERATURE
        if re_or(e.SUMMARY, "exposici[oó]n", flags=re.I, to_log=e.UID):
            return Category.EXPO
        if re_or(e.SUMMARY, "taller", "formaci[óo]n", flags=re.I, to_log=e.UID):
            return Category.WORKSHOP
        logger.critical(str(CategoryUnknown(e.source, f"{e}")))
        return Category.UNKNOWN
