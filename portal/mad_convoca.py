from core.gancio import GancioPortal, Event as GancioEvent
from core.ics import IcsReader, IcsEventWrapper
from core.event import Event, Place, Category, Session, CategoryUnknown
from functools import cached_property
from core.util import plain_text, find_duplicates, re_or, re_and, get_domain, find_euros
import re
import logging
from typing import Callable
from datetime import datetime
from core.web import get_text, buildSoup
from functools import cache
from core.cache import TupleCache
from core.md import MD

logger = logging.getLogger(__name__)

re_sp = re.compile(r"\s+")


class MadConvoca:
    def __init__(
        self,
        isOkDate: Callable[[datetime], bool] = None,
    ):
        self.__pre = {
            "mad.convoca.la": "mc",
            "calendario.extinctionrebellion.es": "ex",
            "hacker.convoca.la": "hk",
        }
        self.__mad = GancioPortal(
            root="https://mad.convoca.la",
            isOkDate=isOkDate
        )
        self.__ext = GancioPortal(
            root="https://calendario.extinctionrebellion.es",
            isOkDate=isOkDate
        )
        self.__hack = GancioPortal(
            root="https://hacker.convoca.la",
            isOkDate=isOkDate
        )
        self.__ics = IcsReader(
            "https://fal.cnt.es/events/lista/?ical=1",
            "https://lahorizontal.net/events/lista/?ical=1",
            "https://madrid.cnt.es/agenda/lista/?ical=1",
            isOkDate=isOkDate
        )

    @cached_property
    @TupleCache("rec/madconvoca.json", builder=Event.build)
    def events(self):
        logger.info("Buscando eventos en MadConvoca")
        ok_events: set[Event] = set()
        for gc in (self.__mad, self.__ext, self.__hack):
            for e in gc.get_events():
                event = self.__gancio_to_event(e)
                if event:
                    ok_events.add(event)
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

        rt = tuple(sorted(e.merge(id=f"mc{e.id}") for e in ok_events))
        logger.info(f"Buscando eventos en MadConvoca = {len(rt)}")
        return rt

    def __is_ko_place(self, url: str, place: Place):
        if place is None:
            return True
        if re_or(
            f"{place.name} {place.address}",
            "Robledo de Chavela",
            flags=re.I
        ):
            return True
        if get_domain(url) in (
            'calendario.extinctionrebellion.es',
        ):
            if not re_or(
                f"{place.name} {place.address}",
                "Madrid",
                flags=re.I
            ):
                return True
        return False

    def __gancio_to_event(self, e: GancioEvent):
        if len(e.sessions) == 0:
            return
        place = Place(
            name=e.place.name,
            address=e.place.address,
            latlon=e.place.get_latlon()
        ).normalize()
        if self.__is_ko_place(e.url, place):
            return None
        event = Event(
            url=e.url,
            id=self.__pre[get_domain(e.url)] + str(e.id),
            price=self.__find_gancio_price(e),
            name=e.title,
            img=e.media[0] if e.media else None,
            category=self.__find_gancio_category(e),
            duration=e.duration,
            sessions=tuple(Session(date=s) for s in e.sessions),
            place=place,
            more=e.links[0] if e.links else None
        )
        event = self.__fix_gancio(e, event) or event
        return event

    def __fix_gancio(self, e: GancioEvent, ev: Event):
        if e.description and re_or(e.title, r"Cinef[óo]rum de la Rosa", flags=re.I):
            text = MD.convert(e.description)
            m = re.search(
                r"([^\.\(\)]+?) \((\d{4})\),? dir.? ([^\.\(\)]+)",
                text,
                flags=re.I
            )
            if m:
                name, year, dr = map(str.strip, m.groups())
                ev = ev.merge(
                    category=Category.CINEMA
                ).fix_type().merge(
                    name=name,
                    year=int(year),
                    director=(dr, )
                )
                return ev

    def __ics_to_event(self, e: IcsEventWrapper):
        if e.SUMMARY is None:
            return
        if re.match(r"^\s*CANCELADO[\. ].*", e.SUMMARY):
            return
        place = self.__find_ics_place(e)
        if place is None:
            return
        place = place.normalize()
        if self.__is_ko_place(e.URL, place):
            return None
        event = Event(
            id=e.UID,
            url=e.URL,
            name=e.SUMMARY,
            duration=e.duration or 60,
            img=e.ATTACH,
            price=self.__find_ics_price(e),
            #publish=e.str_publish,
            category=self.__find_ics_category(e),
            place=place,
            sessions=(
                Session(
                    date=e.DTSTART.strftime("%Y-%m-%d %H:%M"),
                ),
            ),
        )
        return event

    def __find_ics_price(self, e: IcsEventWrapper):
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

    def __find_ics_category(self, e: IcsEventWrapper):
        def _has_cat(*args):
            for c in e.CATEGORIES:
                if re_or(c, *args, flags=re.I):
                    return True
            return False

        if re_or(
            e.SUMMARY,
            r"Asesorías? legal(es)?",
            r"Asesorías? laboral(es)?",
            ("Redes Libertarias", r"n[úu]mero", "revista"),
            r"Acto anual de gratitud a las socias y los socios",
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
            "Charla Informativa",
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
        if e.CATEGORIES:
            logger.critical(str(CategoryUnknown(e.source, f"{e.CATEGORIES} -- {e.SUMMARY}")))
        else:
            logger.critical(str(CategoryUnknown(e.source, f"{e}")))
        return Category.UNKNOWN

    def __find_ics_place(self, e: IcsEventWrapper):
        if e.LOCATION:
            return Place(
                name=e.LOCATION,
                address=e.LOCATION
            )

    def __find_gancio_category(self, e: GancioEvent) -> Category:
        name = plain_text(e.title)
        txt_desc = MD.convert(e.description)
        tags: set[str] = set(map(plain_text, map(str.strip, e.tags)))
        isLibreria = re_or(e.place.name, "librer[íi]a", flags=re.I)

        def has_tag(*args):
            for a in args:
                if a in tags:
                    logger.debug(f"{e.id} tiene tag {a}")
                    return True
            return False

        def has_tag_or_title(*args):
            for t in tags:
                if re_or(t, *args, flags=re.I, to_log=e.id):
                    return True
            if re_or(name, *args, flags=re.I, to_log=e.id):
                return True
            return False

        if re_or(
            txt_desc,
            "debatiremos sobre la novela",
            flags=re.I
        ):
            return Category.NARRATIVE

        if re_or(
            e.place.name,
            "online y en las calles",
            flags=re.I
        ):
            return Category.ACTIVISM

        if has_tag_or_title("flinta", r"No[\-\s]*mixto"):
            return Category.NON_GENERAL_PUBLIC
        if has_tag_or_title("infantil"):
            return Category.CHILDISH
        if has_tag(
            "asamblea"
        ) or has_tag_or_title(
            'manifestaci[oó]n',
            'concentraci[oó]n',
            'regularizaci[oó]n extraordinaria'
        ):
            return Category.ACTIVISM
        if re_or(
            txt_desc,
            "Ven con tus peques",
            flags=re.I,
            to_log=e.id
        ):
            return Category.CHILDISH
        if re_or(
            name,
            r"Presentaci[óo]n.* Marcha Republicana",
            r"Desayuno en Magdalena",
            "Bienvenida Nuev[oax@e]s? Rebeldes?",
            r"recogida (de )?material",
            ("Bienvenida", r"Rebeli[óo]n", r"Extinci[oó]n"),
            ("Grupo", "masculinidades",),
            ("Convocatoria", "Vivotecnia"),
            flags=re.I,
            to_log=e.id
        ):
            return Category.ACTIVISM

        if has_tag_or_title("kafeta"):
            return Category.PARTY
        if has_tag_or_title(
            "cine",
            "cinef[óo]rum",
            "cinebollum",
            "documental"
        ):
            return Category.CINEMA
        if has_tag("deporte") or has_tag_or_title("yoga", "pilates"):
            return Category.SPORT
        if has_tag_or_title(
            "taller",
            "formaci[oó]n",
            "intercambio de idiomas",
            "hacklab"
        ) or re_or(
            name,
            "^clases de",
            "^curso de",
            ("no", "compres", "cose"),
            flags=re.I,
            to_log=e.id
        ):
            return Category.WORKSHOP
        if re_or(name, "iniciaci[óo]n al",  flags=re.I, to_log=e.id) and has_tag("deporte", "gimnasia"):
            return Category.WORKSHOP
        if has_tag_or_title("presentaci[óo]n de libro"):
            return Category.LITERATURE
        if has_tag_or_title("teatro", "micro abierto", "performance", "mikro abierto"):
            return Category.THEATER
        if has_tag_or_title("club de lectura", "grupo de lectura", "clubdelectura", "grupodelectura", "bookelarre"):
            return Category.READING_CLUB
        if has_tag("concierto") or re_or("^concierto", flags=re.I, to_log=e.id):
            return Category.MUSIC
        if re_or(
            name,
            "fiesta",
            "Social Swing",
            "kabaret",
            "cañeo",
            "Paella Republicana",
            flags=re.I,
            to_log=e.id
        ):
            return Category.PARTY
        if re_or(name, "bicicritica", to_log=e.id):
            return Category.SPORT
        if has_tag_or_title("charlas?", "conversatorio"):
            return Category.CONFERENCE
        if re_or(
            name,
            "Charla-debate",
            "conferencia",
            "Discusi[oó]n cr[ií]tica sobre",
            "Presentaci[oó]n Informe",
            "^Charla:",
            "^Charla",
            "Charla Informativa",
            "Anarkademia",
            flags=re.I,
            to_log=e.id
        ):
            return Category.CONFERENCE
        if re_or(name, "radio comunitaria", flags=re.I, to_log=e.id):
            return Category.WORKSHOP
        if has_tag_or_title("concierto", "swing") or has_tag("musica", "música"):
            return Category.MUSIC
        if has_tag_or_title("exposición", "exposici[óo]n", "miniexpo", "mini-expo"):
            return Category.EXPO
        if has_tag_or_title("mesa ciudadana", "movilizaciones por"):
            return Category.ACTIVISM
        if has_tag_or_title("teknokasa", 'a-k-m-e', 'kawin', 'Repair\s*Caf[eé]'):
            return Category.WORKSHOP
        if re_and(name, "Software", ("Free", "libre"), ("day", "día"), flags=re.I):
            return Category.PARTY
        if re_or(
            name,
            "Ruta",
            ("naturalista", "jar[áa]ma"),
            flags=re.I
        ):
            return Category.SPORT
        if re_or(
            name,
            "Filosof[ií]a PEC",
            flags=re.I
        ):
            return Category.READING_CLUB
        if re_or(
            name,
            "Pelis y Pili",
            flags=re.I
        ):
            return Category.CINEMA

        if re_or(
            txt_desc,
            "Charla cr[ií]tica",
            "vendr[aá]n a conversar sobre",
            "conferencia",
            "conversaremos con",
            ("jornada", "auditorio"),
            "A lo largo de la charla",
            "conservatorio",
            ("Encuentros?", "conversaci[óo]n(es)?"),
            r"en este coloquio",
            r"Habr[aá] charla",
            flags=re.I,
            to_log=e.id
        ):
            return Category.CONFERENCE
        if re_or(txt_desc, "m[uú]sica electr[óo]nica", flags=re.I, to_log=e.id):
            return Category.MUSIC
        if re_or(txt_desc, "hacer arte cutre"):
            return Category.WORKSHOP
        if re_and(txt_desc, "performance", "micr[óo]fono abierto", "DJ Set(lists?)?", to_log=e.id, flags=re.I):
            return Category.PARTY
        if re_and(txt_desc, "Karaoke", r"DJ Set(s|lists?)?", to_log=e.id, flags=re.I):
            return Category.PARTY
        if re_or(txt_desc, "comedia perform[aá]tica", flags=re.I, to_log=e.id):
            return Category.THEATER
        if re_or(txt_desc, "taller", "Curso presencial", flags=re.I, to_log=e.id):
            return Category.WORKSHOP
        if re_and(
            txt_desc,
            "leer un texto",
            "razonar en com[uú]n",
            "club de lectura",
            "leemos juntas",
            flags=re.I
        ):
            return Category.READING_CLUB

        if isLibreria:
            if re_or(name, "poes[íi]aa?", flags=re.I):
                return Category.POETRY
            if re_or(
                name,
                "presentaci[oó]n",
                "El libro analiza",
                flags=re.I
            ):
                return Category.LITERATURE

        if re_or(name, "Presentaci[óo]n del libro", to_log=e.id, flags=re.I):
            return Category.LITERATURE

        if has_tag("poesia"):
            return Category.POETRY
        if has_tag_or_title("asamblea abierta"):
            return Category.ACTIVISM
        if has_tag("marcha", "lavapiesallimite"):
            return Category.ACTIVISM
        if has_tag("excursion") and has_tag("somosierra"):
            return Category.SPORT
        if has_tag_or_title("dramaturgia"):
            return Category.THEATER
        if re_and(e.place.name, "^desde", "hasta", flags=re.I):
            return Category.ACTIVISM
        if re_or(e.title, "Plenario", flags=re.I):
            return Category.ACTIVISM

        if re_or(
            txt_desc,
            "Hablaremos con .*? sobre su libro",
            "presentamos el nuevo libro",
            flags=re.I,
            to_log=e.id
        ):
            return Category.LITERATURE
        if re_or(
            txt_desc,
            "proyectamos el documental",
            "Duraci[oó]n del documental",
            flags=re.I,
            to_log=e.id
        ):
            return Category.CINEMA
        if re_and(
            txt_desc,
            "hablaremos sobre",
            "trae tu libreta",
            flags=re.I,
            to_log=e.id
        ):
            return Category.WORKSHOP
        if re_or(
            txt_desc,
            "Tu nube seca mi río",
            flags=re.I,
            to_log=e.id
        ):
            return Category.CONFERENCE
        if re_or(
            name,
            "Comida bailable",
            r"gymkhana",
            flags=re.I
        ):
            return Category.PARTY
        if has_tag("ecoaldea") and has_tag("encuentro"):
            return Category.NO_EVENT
        if re_or(
            txt_desc,
            ("asamblea", r"c[óo]mo funcionamos", "participar"),
            "MANIFESTACI[óO]N",
            "CONCENTRACI[oÓ]N",
            flags=re.I
        ):
            return Category.ACTIVISM
        if isLibreria:
            return Category.LITERATURE
        logger.critical(str(CategoryUnknown(e.url, f"{e}")))
        return Category.UNKNOWN

    def __find_gancio_price(self, e: GancioEvent):
        prc = find_euros(e.description)
        if prc is not None:
            return prc
        return 0


if __name__ == "__main__":
    m = MadConvoca()
    e = m.events
