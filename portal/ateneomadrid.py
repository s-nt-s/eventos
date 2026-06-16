from core.ics import IcsReader, IcsEventWrapper
from core.event import Event, Place, Category, Session, CategoryUnknown, find_book_category
from core.place import Places
from functools import cached_property
from core.util import plain_text, find_duplicates, re_or, re_and, find_euros
from core.util.strng import normalize_quote
import re
import logging
from typing import Callable
from datetime import datetime
from core.web import get_text, buildSoup
from functools import cache
from core.cache import TupleCache
from portal.base import Base

logger = logging.getLogger(__name__)

re_sp = re.compile(r"\s+")


@cache
def html_to_text(html: str):
    soup = buildSoup(None, html)
    for x in soup.select("br, p"):
        x.append("\n")
    return get_text(soup)


def clean_name(name: str):
    name = re.sub(r"\s*\.\s*Ciclo .*", "", name)
    name = re.sub(r"^Ciclo (?:de conferencias )?'(.*?)'$", r"\1", name)
    name = re.sub(r"^Ciclo (?:de conferencias )?'(.*?)'\s*\.?\s*(..+)$", r"\2", name)
    return name


class AteneoMadrid(Base):
    def __init__(
        self,
        isOkDate: Callable[[datetime], bool] = None,
        cache: str | bool = True
    ):
        super().__init__(cache=cache)
        self.__ics = IcsReader(
            "https://ateneodemadrid.com/eventos/lista/?ical=1",
            "https://ateneodemadrid.com/eventos/lista/p%C3%A1gina/2/?ical=1",
            "https://ateneodemadrid.com/eventos/lista/p%C3%A1gina/3/?ical=1",
            "https://ateneodemadrid.com/eventos/lista/p%C3%A1gina/4/?ical=1",
            isOkDate=isOkDate
        )

    def _get_events(self):
        ok_events: set[Event] = set()
        done: set[str] = set()
        for e in self.__ics.events:
            if e.UID in done:
                continue
            done.add(e.UID)
            event = self.__ics_to_event(e)
            if event:
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

        rt = tuple(sorted(ok_events))
        return rt

    def __ics_to_event(self, e: IcsEventWrapper):
        if e.SUMMARY is None:
            return
        if re.match(r"^\s*CANCELADO[\. ].*", e.SUMMARY):
            return
        if e.DESCRIPTION in (
            "El contenido esta protegido.",
            "CANCELADO",
        ):
            return None
        place = self.__find_place(e)
        if place is None:
            return
        place = place.normalize()
        name = normalize_quote(e.SUMMARY)
        category = self.__find_category(e)
        event = Event(
            id=f"am{e.UID}",
            url=e.URL,
            name=clean_name(name),
            duration=e.duration or 60,
            img=e.ATTACH,
            price=self.__find_price(e),
            #publish=e.str_publish,
            category=category,
            place=place,
            sessions=(
                Session(
                    date=e.DTSTART.strftime("%Y-%m-%d %H:%M"),
                ),
            ),
            cycle=self.__find_cycle(name, e, category)
        )
        return event

    def __find_cycle(self, name: str, e: IcsEventWrapper, category: Category):
        m = re.search(r"\. Ciclo '([^'']+)'", name)
        if m:
            return m.group(1).strip()
        m = re.search(r"^Ciclo '([^'']+)'", name)
        if m:
            return m.group(1).strip()
        if category != Category.CINEMA:
            if re_or(
                name,
                r"Valle[\-\s]*Incl[aá]n",
                flags=re.I
            ):
                return "Valle-Inclán"

    def __find_price(self, e: IcsEventWrapper):
        prc = find_euros(e.DESCRIPTION)
        if prc is not None:
            return prc
        if re_or(
            e.DESCRIPTION,
            "venta (de )?entradas",
            "las entradas se pueden (adquirir|comprar)",
            r"Socios c[oó]digo (de )?descuento",
            r"C[oó]digo (de )?descuento (para )?socios",
            r"informaci[oó]n y entradas",
            flags=re.I
        ):
            return 999
        return 0

    def __find_category(self, e: IcsEventWrapper):
        cat = self.__find_category_basic(e)
        if cat in (Category.CONFERENCE, Category.LITERATURE):
            book_cat = find_book_category(e.SUMMARY, e.DESCRIPTION, cat)
            if book_cat in (Category.SPAM, ):
                return book_cat

            if cat == Category.LITERATURE:
                if re_or(
                    e.DESCRIPTION,
                    r"Intervienen los poetas",
                    flags=re.I
                ):
                    return Category.POETRY
                if re_or(
                    e.DESCRIPTION,
                    "Secci[oó]n(es)? de Literatura",
                    "Lectura de fragmentos de la obra por",
                    flags=re.I
                ):
                    return Category.NARRATIVE
                if re_or(
                    e.DESCRIPTION,
                    "Secci[oó]n(es)? de Fotograf[ií]a",
                    flags=re.I
                ):
                    return Category.PHOTO
            if re_or(
                e.SUMMARY,
                "Letras gallegas",
                flags=re.I
            ):
                return Category.NARRATIVE
            if re_or(
                f"{e.SUMMARY or ''} {e.DESCRIPTION or ''}".strip(),
                r"Agrupaci[oó]n(es)? Sabatini",
                "de opinión de El Mundo",
                "92 Liberales",
                "Grupo PPE",
                "diputado PP",
                "Cultura Militar",
                r"Radio Intereconom[ií]a",
                ("Gaceta Sindical", r"CC\.?OO\.?"),
            ):
                return Category.INSTITUTIONAL_POLICY
            if re_or(
                e.DESCRIPTION,
                "Mitos, Religiones y Humanidades",
                flags=re.I
            ):
                return Category.RELIGION
            if re_or(
                e.DESCRIPTION,
                "Estudios pict[oó]ricos y sociales Francisco de Goya",
                flags=re.I
            ):
                return Category.PICTURE
            if re_or(
                e.SUMMARY,
                r"Unamuno",
                r"Agust[ií]n Argüelles",
                r"Del Renacimiento a la Ilustraci[oó]n",
                r"Edificios? emblem[aá]ticos? (en el|del) Camino de Santiago",
                flags=re.I
            ) or re_or(
                e.DESCRIPTION,
                r"Agrupaci[óo]n(es)? Ateneos al Flamenco",
                r"Agrupaci[óo]n(es)? Agust[ií]n Argüelles",
                r"Secci[oó]n(es)? de Arquitectura",
                r"Secci[oó]n(es)? Africanista",
                flags=re.I
            ):
                return Category.OTHERS
            return book_cat
        if cat is not None:
            return cat
        if e.CATEGORIES:
            logger.critical(str(CategoryUnknown(e.source, f"{e.CATEGORIES} -- {e.SUMMARY}")))
        else:
            logger.critical(str(CategoryUnknown(e.source, f"{e}")))
        return Category.UNKNOWN

    def __find_category_basic(self, e: IcsEventWrapper):
        def _has_cat(*args):
            for c in e.CATEGORIES:
                if re_or(c, *args, flags=re.I):
                    return True
            return False
        if re.search(r"^(En )?[rR]ecuerdo de [A-Z]", e.SUMMARY or ''):
            return Category.TRIBUTE
        if re_or(
            e.SUMMARY,
            'Cine y medicina',
            flags=re.I
        ):
            return Category.CINEMA

        if re_or(
            e.SUMMARY,
            r"Acto anual de gratitud a l[oa]s soci[ao]s",
            r"Distinciones Dama de la l[aá]mpara",
            r"Presentaci[oó]n del retrato",
            r"Premios FEDEPE",
            r"Gala de Esgrima",
            flags=re.I,
            to_log=e.UID
        ):
            return Category.NO_EVENT
        if re_or(
            e.SUMMARY,
            "comunicaci[óo]n corporativa",
            r"(Cuarta|4[ºª\*\.]) Revoluci[oó]n Industrial",
            flags=re.I
        ):
            return Category.ENTERPRISE
        if re_or(
            e.SUMMARY,
            r"Los poetas leen a",
            r"poetas leen su",
            r"Voces que habitan el verso",
            flags=re.I
        ):
            return Category.POETRY

        if re_and(
            e.SUMMARY,
            "presentaci[oó]n del?",
            ("libro", "novela"),
            flags=re.I,
            to_log=e.UID
        ):
            return Category.LITERATURE

        if _has_cat(r"Proyecci[óo]n", "cinef[óo]rum"):
            return Category.CINEMA
        if _has_cat(r"Presentaci[óo]n del disco", "conciertos?", "recital de piano"):
            return Category.MUSIC
        if _has_cat("Velada") and re_or(e.DESCRIPTION, "piano", flags=re.I):
            return Category.MUSIC
        if _has_cat(r"mon[oó]logo", r"^Lecturas?$", r"Lecturas? dramatizadas?", "teatro", r"[oó]pera", r"Representaci[oó]n Teatral"):
            return Category.THEATER

        if _has_cat(r"Presentación del libro", 'Libros'):
            return Category.LITERATURE
        if _has_cat(r"Mesa redonda", "Conferencias", "Charlas?", 'Homenaje', 'Congreso'):
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
            "recital de piano",
            flags=re.I
        ):
            return Category.MUSIC
        if re_or(
            e.SUMMARY,
            "Lectura en español y en",
            flags=re.I
        ):
            return Category.THEATER

        if re_or(
            e.DESCRIPTION,
            "Secci[oó]n de Yoga",
            flags=re.I
        ):
            return Category.SPORT
        return Category.CONFERENCE

    def __find_place(self, e: IcsEventWrapper):
        if e.LOCATION:
            return Place(
                name=e.LOCATION,
                address=e.LOCATION
            )
        return Places.ATENEO_MADRID.value


if __name__ == "__main__":
    from core.log import config_log
    config_log("log/ateneomadrid.log", log_level=logging.INFO)
    m = AteneoMadrid()
    evs = m.get_events()
