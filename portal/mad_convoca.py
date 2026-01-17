from portal.gancio import GancioPortal
from core.ics import IcsReader, IcsEventWrapper
from core.event import Event, Place, Category, Session, CategoryUnknown, Places
from functools import cached_property
from core.util import plain_text, find_duplicates, re_or, re_and, get_domain, find_euros
import re
import logging
from typing import Callable
from datetime import datetime

logger = logging.getLogger(__name__)

re_sp = re.compile(r"\s+")


class MadConvoca:
    def __init__(
        self,
        isOkDate: Callable[[datetime], bool] = None,
    ):
        self.__mad = GancioPortal(
            root="https://mad.convoca.la",
            id_prefix=""
        )
        self.__ext = GancioPortal(
            root="https://calendario.extinctionrebellion.es",
            id_prefix="ex"
        )
        self.__hack = GancioPortal(
            root="https://hacker.convoca.la",
            id_prefix="hk"
        )
        self.__ics = IcsReader(
            "https://fal.cnt.es/events/lista/?ical=1",
            "https://lahorizontal.net/events/lista/?ical=1",
            "https://madrid.cnt.es/agenda/lista/?ical=1",
            "https://ateneodemadrid.com/eventos/lista/?ical=1",
            "https://ateneodemadrid.com/eventos/lista/p%C3%A1gina/2/?ical=1",
            "https://ateneodemadrid.com/eventos/lista/p%C3%A1gina/3/?ical=1",
            "https://ateneodemadrid.com/eventos/lista/p%C3%A1gina/4/?ical=1",
            isOkDate=isOkDate
        )

    @cached_property
    def events(self):
        logger.info("Buscando eventos en MadConvoca")
        ok_events = set(self.__mad.events).union(self.__ext.events).union(self.__hack.events)
        done: set[str] = set()
        for e in self.__ics.events:
            if e.UID in done:
                continue
            done.add(e.UID)
            if e.SUMMARY is None:
                continue
            if re.match(r"^\s*CANCELADO[\. ].*", e.SUMMARY):
                continue
            place = self.__find_place(e)
            if place is None:
                continue
            place = place.normalize()
            event = Event(
                id=e.UID,
                url=e.URL,
                name=e.SUMMARY,
                duration=e.duration or 60,
                img=e.ATTACH,
                price=self.__find_price(e),
                publish=e.str_publish,
                category=self.__find_category(e),
                place=place,
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

    def __find_price(self, e: IcsEventWrapper):
        prc = find_euros(e.DESCRIPTION)
        if prc is not None:
            return prc
        if re_or(
            e.DESCRIPTION,
            "venta de entradas",
            flags=re.I
        ):
            return 999
        return 0

    def __find_category(self, e: IcsEventWrapper):
        def _has_cat(*args):
            for c in e.CATEGORIES:
                if re_or(c, *args, flags=re.I):
                    return True
            return False

        if re_or(
            e.SUMMARY,
            r"Asesorías? legal(es)?",
            r"Asesorías? laboral(es)?",
            flags=re.I,
            to_log=e.UID
        ):
            return Category.NO_EVENT
        if re_or(
            e.SUMMARY,
            r"Mesa ciudadana del [aá]rbol",
            flags=re.I,
            to_log=e.UID
        ):
            return Category.ACTIVISM
        if re_or(
            e.SUMMARY,
            r"Acto anual de gratitud a las socias y los socios",
            flags=re.I,
            to_log=e.UID
        ):
            return Category.NO_EVENT
        if re_and(
            e.SUMMARY,
            "presentaci[oó]n del?",
            ("libro", "novela"),
            flags=re.I,
            to_log=e.UID
        ):
            return Category.LITERATURE
        if re_or(
            e.SUMMARY,
            "exposici[oó]n(es)?",
            flags=re.I,
            to_log=e.UID
        ):
            return Category.EXPO
        if re_or(
            e.SUMMARY,
            "taller",
            "formaci[óo]n",
            flags=re.I,
            to_log=e.UID
        ):
            return Category.WORKSHOP
        if re_or(
            e.SUMMARY,
            "Ciclo de conferencias",
            flags=re.I
        ):
            return Category.CONFERENCE
        if re_or(
            e.SUMMARY,
            "Club de lectura",
            flags=re.I
        ):
            return Category.READING_CLUB

        if _has_cat(r"Proyecci[óo]n", "cinef[óo]rum"):
            return Category.CINEMA
        if _has_cat(r"Presentaci[óo]n del disco", "concierto"):
            return Category.MUSIC
        if _has_cat(r"mon[oó]logo", r"Lecturas? dramatizadas?"):
            return Category.THEATER
        if _has_cat(r"Presentación del libro", 'Libros'):
            return Category.LITERATURE
        if _has_cat(r"Mesa redonda", "Conferencias", "Charlas?", 'Homenaje'):
            return Category.CONFERENCE

        if re_and(
            e.DESCRIPTION,
            "M[úu]sica",
            ("compositor", "voz", "viol[íi]n"),
            flags=re.I
        ):
            return Category.MUSIC
        if re_and(
            e.DESCRIPTION,
            ("Abre el acto", "Presenta", "modera"),
            ("Intervienen?", "con: "),
            flags=re.I
        ):
            return Category.CONFERENCE
        if re_or(
            e.DESCRIPTION,
            "conversa(re)?mos con",
            flags=re.I
        ):
            return Category.CONFERENCE
        if _has_cat(r"exposiciones"):
            return Category.EXPO
        if re_or(
            e.SUMMARY,
            "concierto",
            flags=re.I
        ):
            return Category.MUSIC
        if get_domain(e.URL) == "ateneodemadrid.com":
            return Category.CONFERENCE
        if e.CATEGORIES:
            logger.critical(str(CategoryUnknown(e.source, f"{e.CATEGORIES} -- {e.SUMMARY}")))
        else:
            logger.critical(str(CategoryUnknown(e.source, f"{e}")))
        return Category.UNKNOWN

    def __find_place(self, e: IcsEventWrapper):
        if e.LOCATION:
            return Place(
                name=e.LOCATION,
                address=e.LOCATION
            )
        if get_domain(e.URL) == "ateneodemadrid.com":
            return Places.ATENEO_MADRID.value


if __name__ == "__main__":
    m = MadConvoca()
    e = m.events
