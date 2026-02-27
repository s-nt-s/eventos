from core.web import WEB
from bs4 import Tag
import re
from typing import Set, Tuple, Optional, NamedTuple
from core.event import Event, Cinema, Session, Place, Category, CategoryUnknown, FIX_EVENT
from core.util import plain_text, re_or, re_and, get_domain, find_euros, KO_MORE
from arrow import Arrow
import logging
from core.cache import TupleCache
from functools import cached_property, cache
from datetime import datetime, date
import pytz
from zoneinfo import ZoneInfo
from typing import Callable
from core.madrid_es.api import Api, Event as ApiEvent, Place as ApiPlace
from core.ics import IcsEventWrapper
from core.madrid_es.form import get_vgnextoid


logger = logging.getLogger(__name__)
re_sp = re.compile(r"\s+")

TODAY = date.today()

TZ_ZONE = 'Europe/Madrid'
NOW = datetime.now(tz=pytz.timezone(TZ_ZONE))


@cache
def to_place(p: ApiPlace):
    if p is None:
        return None
    latlon = None
    if None not in (p.latitude, p.longitude):
        latlon = f"{p.latitude},{p.longitude}"
    plc = Place(
        name=clean_lugar(p.location),
        address=p.address,
        latlon=latlon
    ).normalize()
    if not plc.zone and p.district:
        plc = plc.merge(zone=p.district)
    return plc


def str_to_datetime(s: str):
    dt = datetime.strptime(s, "%Y-%m-%d %H:%M")
    dt = dt.replace(tzinfo=ZoneInfo(TZ_ZONE))
    return dt


def safe_get_text(n: Tag):
    if isinstance(n, Tag):
        return get_text(n)


def get_text(n: Tag):
    if n is None:
        return None
    t = n.get_text()
    t = re_sp.sub(" ", t)
    t = re.sub(r'[“”]', '"', t)
    t = t.strip()
    if len(t):
        return t


def clean_lugar(s: str):
    if re_and(s, "Auditorio", "Parque Lineal del Manzanares", flags=re.I):
        return "Parque Lineal del Manzanares (Auditorio)"
    if re.search(r"Centro cultural Clara del Rey", s, flags=re.I):
        return "Centro cultural Clara del Rey"
    if re.search(r".*Nave.*\bTerneras\b.*\bCasa del Reloj.*", s, flags=re.I):
        return "Nave Terneras"
    if re.search(r".*La Lonja\b.*\bCasa del Reloj.*", s, flags=re.I):
        return "La Lonja"
    if re.search(r"Casa del Reloj", s, flags=re.I):
        return "Casa del Reloj"
    s = re.sub(r"\bCentro de Información y Educación Ambiental\b", "CIEA", s, flags=re.I)
    s = re.sub(r"^Biblioteca Pública( Municipal)?", "Biblioteca", s)
    s = re.sub(r"\s+\(.*?\)\s*$", "", s)
    s = re.sub(r"^Mercado municipal de ", "Mercado ", s, flags=re.I)
    s = re.sub(
        r"^Espacio de igualdad ([^\.]+)\..*$", r"Espacio de igualdad \1",
        s,
        flags=re.I
    )
    s = re.sub(
        r"^Centro de Información y Educación Ambiental de (.*)$",
        r"Centro de información y educación ambiental de \1",
        s,
        flags=re.I
    )
    s = re.sub(r"^(Matadero) (Medialab|Madrid)$", r"\1", s, flags=re.I)
    s = re.sub(r"^(Cineteca) Madrid$", r"\1", s, flags=re.I)
    s = re.sub(r"^(Imprenta Municipal)\s.*$", r"\1", s, flags=re.I)
    s = re.sub(r"^Centro (cultural|sociocultural)\b", "Centro cultural", s, flags=re.I)
    s = re.sub(r"\s+de\s+Madrid$", "", s, flags=re.I)
    s = re.sub(r"^Centro dotacional integrado", "Centro dotacional integrado", s, flags=re.I)
    s = re.sub(r"\bFaro de la Moncloa\b", "Faro de Moncloa", s, flags=re.I)
    lw = plain_text(s).lower()
    if lw.startswith("museo de san isidro"):
        return "Museo San Isidro"
    for txt in (
        "Biblioteca Eugenio Trías",
        "Centro dotacional integrado Arganzuela",
        "Centro danza Matadero",
        "Auditorio de la Plaza de Chamberí",
    ):
        if lw.startswith(plain_text(txt)):
            return txt
    for txt in (
        "Conde Duque",
        "Matadero"
    ):
        if lw.endswith(" "+plain_text(txt)):
            return txt
    return s


def get_href(n: Tag):
    if n is None:
        return None
    if n.name == "a":
        return n.attrs.get("href")
    return get_href(n.find("a"))


def str_to_arrow_hour(h: str):
    if h is None:
        return None
    if h.isdigit() and int(h) < 25:
        h = f"{int(h):02d}:00"
    while len(h) < 5:
        h = "0"+h
    if re.match(r"^([0-1][0-9]|2[0-4]):([0-5][0-9]|60)$", h):
        return Arrow.strptime(h, "%H:%M")


class ApiInfo(NamedTuple):
    event: ApiEvent
    ics: tuple[IcsEventWrapper, ...] = tuple()


def tp_join(a: tuple | None, b: tuple | None):
    arr = []
    for x in (a or tuple()) + (b or tuple()):
        if x is not None and x not in arr:
            arr.append(x)
    return arr


class MadridEs:
    def __init__(
        self,
        isOkDate: Callable[[datetime], bool] = None,
        places_with_store: tuple[Place, ...] = None,
        max_price: Optional[float] = None,
        avoid_categories: tuple[Category, ...] = tuple(),
        isOkPlace: Callable[[Place | tuple[float, float] | str], bool] = None,
        districts: tuple[str, ...] = tuple()
    ):
        self.__isOkPlace = isOkPlace or (lambda *_: True)
        self.__isOkDate = isOkDate or (lambda *_: True)
        self.__places_with_store = places_with_store or tuple()
        self.__max_price = max_price
        self.__avoid_categories = avoid_categories,
        self.__districts = districts or tuple()
        self.__ban_id = set(map(get_vgnextoid, KO_MORE))
        self.__ban_id.discard(None)

    @cached_property
    def __api(self):
        return Api()

    def __get_api_info(self):
        events: dict[str, ApiEvent] = {}
        for e in self.__api.get_events():
            if e.vgnextoid in self.__ban_id:
                continue
            if e.place is None:
                logger.debug(f"Descartado por place=None {e.url}")
                continue
            if self.__districts and e.place.district and not re_or(e.place.district, *self.__districts, flags=re.I):
                logger.debug(f"Descartado por place.district={e.place.district} {e.url}")
                continue
            if not self.__isOkPlace(to_place(e.place)):
                continue
            prc = FIX_EVENT.get(MadridEs.get_id(e.url), {}).get('price')
            if isinstance(prc, (float, int)):
                e = e._replace(price=prc)
            events[e.vgnextoid] = e

        ids = tuple(sorted(events.keys()))
        e_ics = dict(self.__api.get_ics(*ids))
        for vgnextoid in ids:
            e = events[vgnextoid]
            ics = e_ics[vgnextoid]
            if ics is None or len(ics) == 0:
                logger.debug(f"Descartado por ICS vació {e.url}")
                del events[vgnextoid]
                continue
        ids = tuple(sorted(events.keys()))
        e_page = self.__api.get_page(*(e.url for e in events.values()))
        for vgnextoid in ids:
            e = events[vgnextoid]
            p = e_page[vgnextoid]
            if p is None:
                logger.debug(f"Descartado por página vacía {e.url}")
                del events[vgnextoid]
                continue
        ids = tuple(sorted(events.keys()))

        for vgnextoid in ids:
            e = events[vgnextoid]
            ics = e_ics[vgnextoid]
            page = e_page[vgnextoid]
            if page.free is True:
                e = e._replace(price=0)
            if e.description is None:
                e = e._replace(description=get_text(page.description))
            if e.price is None:
                e = e._replace(price=find_euros(*page.price, e.description))
            ics_desc: set[str] = set()
            ics_price: set[float | int] = set()
            for i in ics:
                ics_desc.add(i.DESCRIPTION)
                ics_price.add(find_euros(i.DESCRIPTION))
            ics_desc.discard(None)
            ics_price.discard(None)
            if e.description is None and ics_desc:
                e = e._replace(description=sorted(ics_desc, key=lambda x: (-len(x), x))[0])
            if e.price is None and ics_price:
                e = e._replace(description=max(ics_price))
            e = e._replace(
                more=tp_join(e.more, page.more),
                img=tp_join(e.more, page.img),
            )
            events[vgnextoid] = e

        for vgnextoid in ids:
            e = events[vgnextoid]
            if e.price is not None or not e.more:
                continue
            more_price: set[int | float] = set()
            for more in e.more:
                vid = get_vgnextoid(more)
                if vid is not None and vid in events:
                    more_e = events[vid]
                    e = e._replace(img=tp_join(e.img, more_e.img))
                    if more_e.price is not None:
                        more_price.add(more_e.price)
            if more_price:
                e = e._replace(price=max(more_price))
            events[vgnextoid] = e

        places_with_price: set[ApiPlace] = set()
        for e in events.values():
            if isinstance(e.price, (int, float)) and e.price > 0:
                places_with_price.add(e.place)
        for vgnextoid in ids:
            e = events[vgnextoid]
            if e.price is not None:
                continue
            if e.place and e.place not in places_with_price:
                if re_or(
                    to_place(e.place).name,
                    "Espacio de igualdad",
                    "Centro dotacional",
                    "Centro cultural",
                    "Biblioteca",
                    flags=re.I
                ):
                    events[vgnextoid] = e._replace(price=0)

        for vgnextoid in ids:
            e = events[vgnextoid]
            p = to_place(e.place)
            ics = e_ics[vgnextoid]
            page = e_page[vgnextoid]
            if e.price is None:
                logger.debug(f"Descartado por price=None {e.url}")
                del events[vgnextoid]
                continue
            price_or_cero = e.price or 0
            if price_or_cero > 0 and p in self.__places_with_store:
                logger.debug(f"Descartado por price={e.price} en lugar {p.name} con store {e.url}")
                del events[vgnextoid]
                continue
            if self.__max_price is not None and price_or_cero > self.__max_price:
                logger.debug(f"Descartado por price={e.price} {e.url}")
                del events[vgnextoid]
                continue
            if self.__avoid_categories:
                cat = self.__find_easy_category(e)
                if cat in self.__avoid_categories:
                    logger.debug(f"Descartado por category={cat} {e.url}")
                    del events[vgnextoid]
                    continue
            ics_events: list[IcsEventWrapper] = []
            for i in ics:
                if i.DTSTART < NOW:
                    logger.debug(f"Fecha  [{i.DTSTART}-{i.DTEND}] < NOW descartada {e.url}")
                    continue
                if not self.__isOkDate(i.DTSTART):
                    logger.debug(f"Fecha {i.DTSTART} en horario laboral descartada {e.url}")
                    continue
                if i.DTEND is not None and i.DTSTART.date() != i.DTEND.date():
                    logger.debug(f"Fecha [{i.DTSTART}-{i.DTEND}] descartada {e.url}")
                    continue
                ics_events.append(i)
            e_ics[vgnextoid] = tuple(ics_events)
            if len(ics_events) == 0:
                logger.debug(f"Descartado por len(sessions)==0 {e.url}")
                del events[vgnextoid]
                continue

        info: list[ApiInfo] = []
        for id, e in events.items():
            info.append(ApiInfo(
                event=e,
                ics=e_ics[id]
            ))

        return tuple(info)

    @property
    @TupleCache("rec/madrid_es.json", builder=Event.build)
    def events(self) -> Tuple[Event, ...]:
        logger.info("Madrid Es: Buscando eventos")

        all_events: Set[Event] = set()
        for i in self.__get_api_info():
            e = self.__info_to_event(i)
            if e is not None:
                all_events.add(e)

        rt = Event.fusionIfSimilar(
            all_events,
            ('name', 'place')
        )
        logger.info(f"Madrid Es: Buscando eventos = {len(rt)}")
        return rt

    def __info_to_event(self, i: ApiInfo):
        duration, sessions = self.__find_duration_session(i)
        if len(sessions) == 0:
            logger.debug(f"Descartado por len(sessions)==0 {i.event.url}")
            return None
        e = Event(
            id=MadridEs.get_id(i.event.url),
            url=i.event.url,
            name=i.event.title,
            price=i.event.price,
            category=self.__find_category(i),
            place=to_place(i.event.place),
            duration=duration,
            sessions=sessions,
            img=i.event.img[0] if i.event.img else None,
            more=i.event.more[0] if i.event.more else None
        ).fix_type()
        if isinstance(e, Cinema):
            e = e.merge(year=self.__find_year(i))
        return e

    def __find_year(self, i: ApiInfo) -> Optional[int]:
        yrs: set[int] = set()
        for y in map(int, re.findall(r"Año:?\s*((?:19|20)\d+)", i.event.description or "")):
            if y >= 1900 and y <= (TODAY.year+1):
                yrs.add(y)
        if len(yrs) == 1:
            return yrs.pop()
        if len(yrs) > 1:
            return None
        for y in map(int, re.findall(r"\([^\(\)\d]*((?:19|20)\d+)\)", i.event.description or "")):
            if y >= 1900 and y <= (TODAY.year+1):
                yrs.add(y)
        if len(yrs) == 1:
            return yrs.pop()

    def __find_duration_session(self, i: ApiInfo) -> tuple[int, tuple[Session, ...]]:
        if len(i.ics) == 0:
            raise ValueError(i)

        durations: Set[int] = set()
        sessions: Set[Session] = set()
        for e in i.ics:
            start = self.__get_start(e.DTSTART, i)
            if not self.__isOkDate(start):
                logger.debug(f"Fecha {start} descartada por horario laboral {i.event.url}")
                continue
            if e.DTEND and e.DTEND.strftime("%H:%M") != "23:59":
                durations.add(int((e.DTEND - start).seconds / 60))
            sessions.add(Session(
                date=start.strftime("%Y-%m-%d %H:%M")
            ))
        if len(sessions) == 0:
            return 0, tuple()
        duration = self.__get_duration(durations, i)
        return duration, tuple(sorted(sessions))

    def __find_easy_category(self, i: ApiEvent):
        cat = FIX_EVENT.get(MadridEs.get_id(i.url), {}).get('category')
        if isinstance(cat, str):
            return Category[cat]
        if re_or(
            i.title,
            r"concierto infantil",
            r"en familia",
            r"elaboraci[óo]n de comederos de aves",
            r"los [\d\. ]+ primeros d[íi]as no se repiten",
            r"photocall hinchable",
            r"^re vuelta al patio",
            r"Visita familiar",
            r"taller familiar",
            r"huerto familiar",
            r"Taller infantil",
            r"Pedag[óo]gico Infantil",
            (r"dia", r"internacional", r"familias?"),
            (r"taller", r"pequeños"),
            flags=re.I
        ):
            return Category.CHILDISH
        if re_or(
            i.description,
            r"Espect[aá]culo infantil",
            r"musical? infantil",
            r"teatro infantil",
            r"relatos en familia",
            r"concierto familiar",
            r"bienestar de niños y niñas",
            (r"cuentacuentos", r"en familia"),
            r"donde los niños y niñas pueden",
            flags=re.I
        ):
            return Category.CHILDISH
        if re_or(
            i.title,
            "taller juvenil",
            "Teenage Party",
            flags=re.I
        ):
            return Category.YOUTH
        if re_or(
            i.title,
            "para mayores$",
            flags=re.I
        ):
            return Category.SENIORS
        if re_or(
            i.title,
            r"Grupo de crianza",
            "La Liga de la Leche",
            flags=re.I
        ):
            return Category.MATERNITY

        if i.has_audience(
            'colectivos necesitados',
            'discapacidad',
            r'necesidad socioecon[oó]mica',
            'emergencia social',
            r'situaci[óo]n de dependencia',
            'sin hogar',
            r'v[íi]ctimas',
            'violencia genero',
            r'(in|e)?migrantes',
            'drogodependientes'
        ):
            return Category.MARGINALIZED

        if i.has_audience(
            'conductores',
            'vehiculos',
            'empresarios',
            'comerciantes',
            r'ongs?',
            'animales',
        ):
            return Category.NON_GENERAL_PUBLIC

        if i.has_category(
            'en linea',
            'online',
        ):
            return Category.ONLINE
        if re_or(
            i.title,
            "Voluntarios? por Madrid",
            flags=re.I
        ):
            return Category.NO_EVENT
        if re_or(
            i.title,
            r"d[íi]a mundial de la poes[íi]a",
            r"encuentro po[ée]tico",
            r"Recital de poes[íi]a",
            r"Versos entrevistados",
            r"Presentaci[óo]n del poemario",
            r"^T[eé] y poes[ií]a",
            r"encuentro de poetas",
            flags=re.I
        ):
            return Category.POETRY
        if re_or(
            i.title,
            r"Muestra de proyectos \d+",
            flags=re.I
        ):
            return Category.EXPO
        if re_or(
            i.title,
            "Grupo de hombres por la Igualdad",
            "^C[ií]rculo de Mujeres$",
            flags=re.I
        ):
            return Category.ACTIVISM
        if re_or(
            i.title,
            r"Primeros pasos con Gmail",
            r"Quiero usar mi m[oó]vil",
            flags=re.I
        ):
            return Category.SPAM

        if i.has_audience(
            'mayores'
        ):
            return Category.SENIORS

        if i.has_category(
            'escolares',
            'campamentos',
        ):
            return Category.CHILDISH

        place = to_place(i.place).name
        if re_or(
            place,
            "titeres"
        ):
            return Category.PUPPETRY

        if i.has_category(
            "documental",
            "cine experimental",
            ("ficci[óo]n", "cine"),
            r"^cine$",
        ):
            return Category.CINEMA
        if re_or(
            i.description,
            r"Una proyecci[oó]n de la pel[ií]cula",
            flags=re.I
        ):
            return Category.CINEMA

        if re_or(
            i.title,
            "Mejora tu ingl[eé]s con charlas",
            "Reconocimiento de [aá]rboles",
            "taller de escritura",
            "Aprende Chotis",
            flags=re.I
        ):
            return Category.WORKSHOP
        if re_or(
            i.title,
            "Salida medioambiental",
            flags=re.I
        ):
            return Category.HIKING
        if re_or(
            i.title,
            r"^exposici[oó]n(es)$",
            flags=re.I
        ):
            return Category.EXPO
        if re_or(
            i.title,
            r"^conciertos?$",
            r"Composici[oó]n musical para",
            flags=re.I
        ):
            return Category.MUSIC
        if re_or(
            i.title,
            r"^teatros?$",
            r"^Microteatros?",
            "Audio-?drama",
            flags=re.I
        ):
            return Category.THEATER
        if re_or(
            i.title,
            r"^danzas?$",
            r"Baile sin cuartel",
            flags=re.I
        ):
            return Category.DANCE
        if re_or(
            i.title,
            r"^visitas? guiadas?",
            r"^visita el",
            r"^descubre el vivero",
            r"En este itinerario se",
            r"^Visitas? al",
            flags=re.I
        ):
            return Category.VISIT
        if re_or(
            i.title,
            r"Presentaci[óo]n del? libro",
            flags=re.I
        ):
            return Category.LITERATURE

        if re_or(
            i.description,
            r"Ciclo de conferencias",
            r"En esta charla vamos",
            flags=re.I
        ):
            return Category.CONFERENCE
        if re_or(
            i.description,
            r"Presentaci[óo]n del? libro",
            flags=re.I
        ):
            return Category.LITERATURE
        if re_or(
            i.description,
            r"Este proyecto musical",
            flags=re.I
        ):
            return Category.MUSIC

        if i.has_category(
            r"cine actividades audiovisuales",
        ):
            return Category.CINEMA
        if re_or(
            i.title,
            r"^cine$",
            "cine",
            "^proyecci[oó]n(es)? de",
            "cortometrajes?",
            "^Pel[íi]cula:",
            "Cinef[oó]rum",
            "documental",
            flags=re.I
        ):
            return Category.CINEMA

        if i.has_audience(
            'familias',
            'niñas',
            'niños',
        ):
            return Category.CHILDISH
        if i.has_audience(r'j[óo]venes'):
            return Category.YOUTH

        if re_or(
            i.title,
            r"^deportes?$",
            flags=re.I
        ):
            return Category.SPORT

    def __find_category(self, i: ApiInfo):
        cat = self.__find_easy_category(i.event)
        if cat is not None:
            return cat

        maybeSPAM = any([
            re_or(i.event.title, "el mundo de los toros", "el mundo del toro", "federacion taurina", "tertulia de toros"),
            re_and(i.event.title, "actos? religios(os)?", ("santo rosario", "eucaristia", "procesion")),
        ])

        if i.event.has_category(
            r'club(es)? de lectura',
        ):
            return Category.READING_CLUB
        if i.event.has_category(
            r'cursos?',
            r'taller(es)?',
        ):
            return Category.WORKSHOP
        if i.event.has_category(
            r'concursos?',
            r'certamen(es)?',
        ):
            return Category.CONTEST
        if i.event.has_category(
            r"(clasico|drama)\b.*teatro",
            r"(zarzuela).*\bm[úu]sica",
            r"teatro perfomance",
        ):
            return Category.THEATER
        if i.event.has_category(
            r"(opera)\b.*teatro",
            r"flamenco\b.*danza",
            r"(rap|jazz|soul|funky|swing|reagge|flamenco|clasica|batucada|latina|española|electronica|rock|pop|folk|country).*\bm[úu]sica",
            r"^m[úu]sica$",
        ):
            return Category.MUSIC
        if i.event.has_category(
            r"danza y baile",
            r"(cl[áa]sica|tango|breakdance|contempor[áa]ne(a|o))\b.*danza",
        ):
            return Category.DANCE
        if i.event.has_category(
            r'deportivas',
        ):
            return Category.SPORT
        if i.event.has_category(
            r"^exposici[óo]n(es)?$",
        ):
            return Category.EXPO
        if i.event.has_category(
            r'recital(es)?',
            r'presentaci[óo]n(es)?',
            r'actos? literarios?',
        ):
            return Category.LITERATURE

        if i.event.has_category(
            r'congresos?',
            r'jornadas?',
            r'conferencias?',
            r'coloquios?s'
        ):
            if maybeSPAM:
                return Category.SPAM
            return Category.CONFERENCE

        if maybeSPAM:
            return Category.SPAM

        if re_or(
            i.event.title,
            "recital de piano",
            r"Cuartero de C[áa]mara",
            r"Arias de [Óo]pera",
            "No cesar[áa]n mis cantos",
            flags=re.I
        ):
            return Category.MUSIC
        if re_and(
            i.event.title,
            "ballet",
            ("repertorio", "clasico"),
        ):
            return Category.DANCE
        if re_or(
            i.event.title,
            r"certamen( de)? (pintura|decoraci[oó]n|ilustraci[oó]n)",
            "festival by olavide"
        ):
            return Category.EXPO
        if re_or(
            i.event.title,
            "belen viviente",
            r"Representaci[óo]n(es)? teatral(es)?",
            r"Mon[oó]logos? de humor",
            flags=re.I
        ):
            return Category.THEATER
        if re_or(
            i.event.title,
            r"belen (popular )?(angelicum|tradicional|monumental|napolitano)",
            r"belen (de )?navidad en",
            "belenes del mundo",
            r"apertura al publico (de el|del) belen",
            r"dioramas? de navidad",
            flags=re.I
        ):
            return Category.EXPO
        if re_or(
            i.event.title,
            r"^conferencias?$",
            r"^pregon$",
            r'[Mm]ocrofestival, tableros y pantallas',
        ):
            return Category.CONFERENCE
        if re_or(
            i.event.title,
            "cañon del rio",
            "ruta a caballo",
            "cerro de",
            r"actividad(es)? acuaticas? pantano",
            flags=re.I
        ):
            return Category.SPORT
        if re_or(
            i.event.title,
            "Voguing",
            flags=re.I
        ):
            return Category.DANCE
        if re_or(
            i.event.title,
            r"^exposicion y (charla|coloquio)",
            r"europa ilustra",
            flags=re.I
        ):
            return Category.EXPO
        if re_or(
            i.event.title,
            r"^conferencia y (charla|coloquio)",
            flags=re.I
        ):
            return Category.CONFERENCE
        if re_or(
            i.event.title,
            r"^taller",
            "tertulias en latin",
            r"taller(es)? de calidad del aire",
            "compostagram",
            "esquejodromo",
            r"^Iniciaci[oó]n a",
            flags=re.I
        ):
            return Category.WORKSHOP
        if re_or(
            i.event.title,
            "visitas guiadas",
            "Recorrido por la Iluminaci[óo]n",
            flags=re.I
        ):
            return Category.VISIT
        if re_or(
            i.event.title,
            "^concierto de",
            flags=re.I
        ):
            return Category.MUSIC
        if re_or(
            i.event.title,
            ("espectaculo", "magia"),
            r"la magia de",
            r"^Magia:",
            r"Magia o plomo",
            flags=re.I
        ):
            return Category.MAGIC
        if re_or(
            i.event.title,
            "m[úu]sica",
            "musicales",
            "conciertos?",
            "hip-hob",
            "jazz",
            "reagge",
            "flamenco",
            "batucada",
            "rock",
            flags=re.I
        ):
            return Category.MUSIC
        if re_or(
            i.event.title,
            "teatro",
            "zarzuela",
            "lectura dramatizada",
            flags=re.I
        ):
            return Category.THEATER
        if re_or(
            i.event.title,
            "exposicion(es)?",
            "noche de los museos",
            flags=re.I
        ):
            return Category.EXPO
        if re_or(
            i.event.title,
            "conferencias?",
            "coloquios?",
            "presentacion(es)?",
            flags=re.I
        ):
            return Category.CONFERENCE
        if re_or(
            i.event.title,
            "charlemos sobre",
            flags=re.I
        ):
            return Category.CONFERENCE
        if re_or(
            i.event.title,
            "club(es)? de lectura",
            flags=re.I
        ):
            return Category.READING_CLUB
        if re_or(
            i.event.title,
            ("elaboracion", "artesanal"),
            flags=re.I
        ):
            return Category.WORKSHOP
        if re_or(
            i.event.title,
            "^senderismo",
            r"^senda",
            "senda botanica",
            "excursion medioambiental",
            r"^del? .* a casa de campo$",
            "^salida multiaventura",
            r"(paseo|itinerario) ornitologico",
            r"^entreparques",
            ("deportes?", "torneo"),
            flags=re.I
        ):
            return Category.SPORT

        place = to_place(i.event.place).name
        if re_or(
            place,
            "educacion ambiental",
            flags=re.I
        ) and re_or(
            i.event.title,
            "^arroyo",
            flags=re.I
        ):
            return Category.SPORT
        if re_or(
            place,
            "imprenta",
            flags=re.I
        ) and re_or(
            i.event.title,
            "demostracion(es)?",
            "museos?",
            flags=re.I
        ):
            return Category.EXPO
        if re_or(
            i.event.title,
            "^(danza|chotis)",
            flags=re.I
        ):
            return Category.DANCE
        if re_or(
            i.event.title,
            "^(charlas?|ensayos?)",
            flags=re.I
        ):
            return Category.CONFERENCE
        if re_or(
            i.event.title,
            "^(acompañamiento digital)",
            flags=re.I
        ):
            return Category.WORKSHOP
        if re_or(
            i.event.title,
            "^(webinario)",
            flags=re.I
        ):
            return Category.ONLINE
        if re_or(
            i.event.title,
            "^(paseo|esculturas)",
            "de el retiro$",
            flags=re.I
        ):
            return Category.VISIT
        if re_or(
            i.event.title,
            "^mercadea en el mercado",
            "^mercadea en los mercadillos",
            flags=re.I
        ):
            return Category.CONFERENCE
        if re_or(
            i.event.title,
            "poemario",
            "^poesia rapidita",
            r"^\d+ poemas",
            "poesia o barbarie",
            flags=re.I
        ):
            return Category.POETRY
        if re_or(
            i.event.title,
            "^hacer actuar",
            flags=re.I
        ):
            return Category.WORKSHOP
        if re_or(
            i.event.title,
            r"visita a",
            flags=re.I
        ):
            return Category.VISIT
        if re_or(
            i.event.title,
            "actuacion coral",
            "recital coral",
            "taller de sevillanas",
            flags=re.I
        ):
            return Category.MUSIC
        if re_or(
            i.event.title,
            "encuentro artistico",
            flags=re.I
        ):
            return Category.EXPO
        if re_or(
            i.event.title,
            "^(cantando|banda municipal)",
            flags=re.I
        ):
            return Category.MUSIC
        if re_and(
            i.event.title,
            "dialogos?",
            "mac",
            flags=re.I
        ):
            return Category.CONFERENCE
        if re_or(
            i.event.title,
            "lengua de signos",
            r"^talleres",
            flags=re.I
        ):
            return Category.WORKSHOP
        if re_or(
            i.event.title,
            "^El mago",
            flags=re.I
        ):
            return Category.MAGIC
        if re_and(
            i.event.title,
            "fiesta",
            "aniversario",
            flags=re.I
        ):
            return Category.PARTY

        if re_or(
            i.event.description,
            "zarzuela",
            "teatro",
            "radionovela",
            "espect[áa]culo (circense y )?teatral",
            flags=re.I
        ):
            return Category.THEATER
        if re_or(
            i.event.description,
            "itinerario .* kil[ó]metros",
            flags=re.I
        ):
            return Category.SPORT
        if re_or(
            i.event.title,
            "actuacion",
            "verbena"
        ) and re_or(
            i.event.description,
            "música",
            "concierto",
            "canciones",
            "pop",
            "rock",
            "baila",
            "bailable",
            "cantante",
            "d[ée]cada prodigiosa",
            flags=re.I
        ):
            return Category.MUSIC
        if re_or(
            i.event.description,
            "concierto",
            r"\bun concierto de",
            r"\bg[oó]spel",
            ("canciones", "Boleros", "Baladas"),
            ("hip-hop", "MCs?"),
            flags=re.I
        ):
            return Category.MUSIC
        if re_or(
            i.event.description,
            r"intervienen l[oa]s",
            "una mesa redonda con",
            "encuentro del ciclo Escritores",
            r"esta conferencia",
            ("encuentro", "compartiremos"),
            r"conferencia dedicada",
            r"Charla basada en",
            flags=re.I
        ):
            return Category.CONFERENCE
        if (i.event.description or '').count("poesía") > 2 or re_or(
            i.event.description,
            "presentación del poemario",
            r"recital de poes[íi]a",
            "presenta su poemario",
            r"presentan? este poemario de",
            r"poemas in[eé]ditos",
            flags=re.I
        ):
            return Category.POETRY
        if re_or(
            i.event.description,
            "propuesta creativa y participativa que combina lectura, escritura y expresión",
            r"Se organizará un '?escape room'?",
            "taller creativo",
            "pensado para ejercitar la memoria",
            "m[óo]dulo pr[aá]ctico",
            ("Drupal", "introducci[óo]n"),
            flags=re.I
        ):
            return Category.WORKSHOP
        if re_and(
            i.event.description,
            r"presentaci[oó]n",
            (r"libros?", r"novelas?"),
            (r"autore(es)?", r"autoras?"),
            flags=re.I
        ):
            return Category.LITERATURE
        if re_and(
            i.event.description,
            "ilusionista",
            "mentalismo"
        ):
            return Category.MAGIC
        if re_or(
            i.event.description,
            r"visitas? guiadas?",
            flags=re.I
        ):
            return Category.VISIT
        if re_and(
            place,
            "ambiental",
            ("casa de campo", "retiro"),
            flags=re.I
        ):
            return Category.VISIT

        if re_or(
            i.event.description,
            r"Concurso de disfraces",
            flags=re.I
        ):
            return Category.CONTEST
        if re_or(
            i.event.description,
            r"Mercado al aire libre",
            flags=re.I
        ):
            return Category.NO_EVENT

        logger.critical(str(CategoryUnknown(
            i.event.url,
            f"name={i.event.title} category={i.event.category}"
        )))
        return Category.UNKNOWN

    def __get_duration(self, durations: Set[int], i: ApiInfo):
        limit = (24*60)-1
        ok = set((d for d in durations if d < limit))
        duration = max(ok) if ok else limit
        if duration < limit:
            return duration
        duration = self.__get_duration_from_info(i)
        if duration is not None:
            return duration
        for more_url in (i.event.more or tuple()):
            dom = get_domain(more_url)
            if dom == "centrodanzamatadero.es":
                soup = WEB.get_cached_soup(more_url)
                for txt in map(get_text, soup.select(".inner-wrapper.card .field__item")):
                    if txt is None:
                        continue
                    m = re.match(r"^(\d+) hora\D+(\d+) minutos*", txt)
                    if m:
                        return (int(m.group(1))*60)+int(m.group(2))
                    m = re.match(r"^(\d+) hora\b.*", txt)
                    if m:
                        return (int(m.group(1))*60)

    def __get_duration_from_info(self, i: ApiInfo):
        desc = i.event.description
        if not desc:
            return None
        for r in (
            r"\bDuraci[óo]n[:\s]+(\d+) min",
        ):
            m = re.search(r, desc, flags=re.I)
            if m is None:
                continue
            duration = int(m.group(1))
            logger.debug(f"FIX duration={duration} <- {i.event.url}")
            return duration
        for r in (
            r"\bcelebraci[oó]n[:\s]+de (\d+(?::\d+)?) a (\d+(?::\d+)?) h",
            r"\bhorario[:\s]+de (\d+(?::\d+)?) a (\d+(?::\d+)?) h"
        ):
            m = re.search(r, desc, flags=re.I)
            if m is None:
                continue
            h1 = str_to_arrow_hour(m.group(1))
            h2 = str_to_arrow_hour(m.group(2))
            if None in (h1, h2):
                continue
            if h1 > h2:
                h2 = h2.shift(days=1)
            duration = int((h2 - h1).seconds / 60)
            logger.debug(f"FIX duration={duration} <- {i.event.url}")
            return duration

    def __get_start(self, start: datetime, i: ApiInfo):
        ko_hour = ("00:00", None)
        if start.strftime("%H:%M") not in ko_hour:
            return start
        s_hour = self.__get_start_from_info(i)
        if s_hour not in ko_hour:
            h, m = map(int, s_hour.split(":"))
            start = start.replace(hour=h, minute=m)
            return start
        for more_url in (i.event.more or tuple()):
            dom = get_domain(more_url)
            if dom == "centrodanzamatadero.es":
                soup = WEB.get_cached_soup(more_url)
                for txt in map(get_text, soup.select(".inner-wrapper.card .field__item")):
                    if txt is None:
                        continue
                    m = re.match(r"^(\d\d:\d\d)(\s*h)?$", txt)
                    if m:
                        h, m = map(int, m.group(1).split(":"))
                        start = start.replace(hour=h, minute=m)
                        return start
        return start

    def __get_start_from_info(self, i: ApiInfo):
        desc = i.event.description
        if not desc:
            return None
        for r in (
            r"\bcelebraci[óo]n[:\s]+de (\d+(?::\d+)?) a (\d+(?::\d+)?) h",
            r"\bhorario[:\s]+de (\d+(?::\d+)?) a (\d+(?::\d+)?) h",
            r"\bdar[áa]n? comienzo a las (\d+(?::\d+)?) h",
        ):
            m = re.search(r, desc, flags=re.I)
            if m is None:
                continue
            h = str_to_arrow_hour(m.group(1))
            if h:
                hm = h.strftime("%H:%M")
                logger.debug(f"FIX hour={hm} <- {i.event.url}")
                return hm
        return None

    @staticmethod
    def get_id(lk: str):
        vgnextoid = get_vgnextoid(lk)
        if vgnextoid is None:
            return None
        return "ms"+vgnextoid


if __name__ == "__main__":
    from core.log import config_log
    config_log("log/madrides.log", log_level=(logging.INFO))
    evs = MadridEs(
    ).events
    print(len(evs))
