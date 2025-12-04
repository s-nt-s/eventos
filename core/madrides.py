from .web import Web, WebException, WEB, Driver
from bs4 import Tag, BeautifulSoup
import re
from typing import Set, Dict, List, Tuple, Union
from urllib.parse import urlencode
from .event import Event, Session, Place, Category, CategoryUnknown, isWorkingHours
from .util import plain_text, re_or, re_and, my_filter, get_domain
from ics import Calendar
from arrow import Arrow
import logging
from .cache import TupleCache
from urllib.parse import urlparse, parse_qs
from functools import cached_property, cache
from collections import defaultdict
from core.util.madrides import find_more_url
from html import unescape
from tatsu.exceptions import FailedParse
from core.zone import Circles


logger = logging.getLogger(__name__)
re_sp = re.compile(r"\s+")


def get_query(url: str):
    purl = urlparse(url)
    qr = parse_qs(purl.query)
    return {k: v[0] for k, v in qr.items()}


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


@cache
def isOkPlace(p: Place):
    if re.search(r"\bcentro juvenil\b", p.name, flags=re.I):
        return False
    if p.latlon is None:
        return True
    kms: list[float] = []
    lat, lon = map(float, p.latlon.split(","))
    for c in Circles:
        kms.append(c.value.get_km(lat, lon))
        if kms[-1] <= c.value.kms:
            return True
    k = round(min(kms))
    logger.debug(f"Lugar descartado {k}km {p.name} {p.url}")
    return False


class MadridEs:
    AGENDA = "https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/?vgnextfmt=default&vgnextchannel=ca9671ee4a9eb410VgnVCM100000171f5a0aRCRD"
    TAXONOMIA = "https://www.madrid.es/ContentPublisher/jsp/apl/includes/XMLAutocompletarTaxonomias.jsp?taxonomy=/contenido/actividades&idioma=es&onlyFirstLevel=true"

    def __init__(self, remove_working_sessions: bool = False):
        self.__remove_working_sessions = remove_working_sessions
        self.w = Web()
        self.w.s = Driver.to_session(
            "firefox",
            "https://www.madrid.es",
            session=self.w.s,
        )

    def get_safe_events(self):
        try:
            return self.events
        except Exception as e:
            logger.critical(str(e), stack_info=True)
        return tuple()

    @cached_property
    def _free(self):
        action, data = self.prepare_search()
        data['gratuita'] = "1"
        ids = self.__get_ids(action, data)
        logger.debug(f"{len(ids)} ids en gratuita=1")
        return ids

    @cached_property
    def _category(self):
        action, data_form = self.prepare_search()
        category: Dict[Category, Set[str]] = defaultdict(set)
        tipos = {plain_text(unescape(v)): k for k, v in self.tipos.items()}
        usuarios = {plain_text(unescape(v)): k for k, v in self.usuarios.items()}

        def _set_cats(key: str, data_key: Dict[str, str], data_cat: Dict[Category, Tuple[str, ...]]):
            data_val: Set[str] = set()
            for k, v in data_key.items():
                if re_or(k, *data_cat):
                    data_val.add(v)
            data = dict(data_form)
            for cat, key_vals in data_cat.items():
                data_val: Set[str] = set()
                data_txt: Set[str] = set()
                for k, v in data_key.items():
                    if re_or(k, *key_vals):
                        data_val.add(v)
                        data_txt.add(k)
                if len(data_val) == 0:
                    logger.warning(f"No encontrado {key} que cumpla {key_vals}, disponible = {tuple(data_key.keys())}")
                    continue
                logger.debug(f"{cat} = {key} in {tuple(sorted(data_txt))}")
                for v in sorted(data_val):
                    data[key] = v
                    ids = self.__get_ids(action, data)
                    logger.debug(f"{len(ids)} ids en {key}={v}")
                    category[cat] = category[cat].union(ids)

        _set_cats('usuario', usuarios, {
            Category.CHILDISH: (
                'familias',
                'jovenes',
                'niñas',
                'niños',
            ),
            Category.SENIORS: ('mayores', ),
            Category.MARGINNALIZED: (
                'colectivos necesitados',
                'discapacidad',
                'necesidad socioeconómica',
                'emergencia social',
                'situacion de dependencia',
                'sin hogar',
                'víctimas',
                'violencia genero',
                r'(in|e)?migrantes',
                'drogodependientes',
            ),
            Category.NON_GENERAL_PUBLIC: (
                'conductores',
                'vehiculos',
                'empresarios',
                'comerciantes',
                r'ongs?',
                'animales',
            ),
        })
        #cine: Set[str] = set()
        #for k, v in tipos.items():
        #    if re_or(k, "cine"):
        #        cine = cine.union(self.__get_ids(action, {**data_form, **{'tipo': v}}))
        #category[Category.CHILDISH] = category[Category.CHILDISH].difference(cine)
        _set_cats('tipo', tipos, {
            Category.CHILDISH: (
                'escolares',
                'campamentos',
            ),
            Category.ONLINE: (
                'en linea',
                'online',
            ),
            Category.READING_CLUB: (
                r'club(es)? de lectura',
            ),
            Category.WORKSHOP: (
                r'cursos?',
                r'taller(es)?',
            ),
            Category.CONTEST: (
                r'concursos?',
                r'certamen(es)?',
            ),
            Category.THEATER: (
                r"(clasico|drama)\b.*teatro",
                r"(zarzuela)\bmusica",
                r"teatro perfomance",
            ),
            Category.MUSIC: (
                r"(opera)\b.*teatro",
                r"flamenco\b.*danza",
                r"(rap|jazz|soul|funky|swing|reagge|flamenco|clasica|batucada|latina|española|electronica|rock|pop|folk|country)\bmusica",
                r"^musica$",
            ),
            Category.DANCE: (
                r"danza y baile",
                r"(clasica|tango|breakdance|contemporane(a|o))\b.*danza",
            ),
            Category.SPORT: (
                r'deportivas',
            ),
            Category.EXPO : (
                r"^exposicion(es)?$",
            ),
            Category.LITERATURA: (
                r'recital(es)?',
                r'presentacion(es)?',
                r'actos? literarios?',
            ),
            Category.CINEMA: (
                r'\b(documental|cine experimental)\b',
                r"\b(ficcion)\b.*cine",
                r"cine\b.*(ficcion)\b",
                r"^cine$"
            ),
            Category.CONFERENCE: (
                r'congresos?',
                r'jornadas?',
                r'conferencias?',
                r'coloquios?s'
            )
        })

        done: Set[str] = set()
        rt_dict: Dict[Tuple[str, ...], Category] = {}
        for k, v in category.items():
            v = v.difference(done)
            rt_dict[tuple(sorted(v))] = k
            done = done.union(v)
        return rt_dict

    def get(self, url, *args, **kwargs) -> BeautifulSoup:
        if self.w.url != url:
            logger.debug(url)
            self.w.get(url, *args, **kwargs)
        title = get_text(self.w.soup.select_one("title"))
        if title == "Access Denied":
            body = get_text(self.w.soup.select_one("body"))
            body = re.sub(r"^Access Denied\s+", "", body or "")
            raise ValueError(f"{url} {title} {body}".strip())
        return self.w.soup

    def __get_description(self, url: str):
        soup = WEB.get_cached_soup(url)
        txt = get_text(soup.select_one("div.tramites-content div.tiny-text"))
        return txt

    def __get_price(self, id: str, url_event: str):
        if id in self._free:
            return 0
        soup_event = WEB.get_cached_soup(url_event)
        if soup_event.select_one("ul li p.gratuita"):
            return 0
        prices: set[str] = set()
        for n in soup_event.select("div.tramites-content div.tiny-text, #importeVenta p"):
            txt = get_text(n)
            if txt is None:
                continue
            if re_or(
                txt,
                "Entrada libre hasta completar aforo",
                "Entrada gratuita",
                flags=re.I
            ):
                return 0
            for p in re.findall(r"(\d[\d\.,]*)\s*(?:€|euros?)", txt):
                p = p.replace(",", '.').strip()
                try:
                    prices.add(float(p))
                except Exception:
                    pass
        if len(prices):
            return max(prices)

    @property
    @TupleCache("rec/madrides.json", builder=Event.build)
    def events(self) -> Tuple[Event, ...]:
        logger.info("Madrid Es: Buscando eventos")
        all_events: Set[Event] = set()
        for action, data in self.iter_submit():
            all_events = all_events.union(self.__get_events(action, data))
        if len(all_events) == 0:
            return tuple()

        empty = {k: None for k in list(all_events)[0]._asdict().keys()}

        mrg_events: Set[Event] = set()
        ko_events: List[Event] = sorted(all_events)

        while ko_events:
            e = ko_events[0]
            k: Event = Event.build({
                **empty,
                **{
                    'name': e.name,
                    'place': e.place,
                }
            })
            ok, ko_events = my_filter(ko_events, lambda x: x.isSimilar(k))
            mrg_events.add(Event.fusion(*ok))
        return tuple(sorted(mrg_events))

    def __get_ids(self, action: str, data: Dict = None):
        ids: Set[str] = set()
        for id, a, div in self.__get_soup_events(action, data):
            ids.add(id)
        return tuple(sorted(ids))

    def __get_events(self, action: str, data: Dict = None):
        evts: Set[Event] = set()
        for id, a, div in self.__get_soup_events(action, data):
            lg = div.select_one("a.event-location")
            if lg is None:
                continue
            place = Place(
                name=clean_lugar(lg.attrs["data-name"]),
                address=lg.attrs["data-direction"],
                latlon=lg.attrs["data-latitude"]+","+lg.attrs["data-longitude"]
            )
            if not isOkPlace(place):
                continue
            url_event = a.attrs["href"]
            duration, sessions = self.__get_sessions(url_event, div)
            if len(sessions) == 0:
                continue
            cat = self.__find_category(id, div, url_event)
            if cat is None:
                continue
            price = self.__get_price(id, url_event)
            if price is None:
                continue
            ev = Event(
                id=id,
                url=url_event,
                name=get_text(a),
                img=None,
                price=price,
                category=cat,
                place=place,
                duration=duration,
                sessions=sessions
            )
            evts.add(ev)
        return evts

    def __get_sessions(self, url_event: str, div: Tag) -> Tuple[Union[int, None], Tuple[Session, ...]]:
        cal = self.__get_cal(div)
        if cal is None:
            return 0, tuple()
        durations: Set[int] = set()
        sessions: Set[Session] = set()
        for event in cal.events:
            if event.begin.strftime("%Y-%m-%d") != event.end.strftime("%Y-%m-%d"):
                continue
            s_date = self.__get_start(event.begin, url_event)
            s = Session(
                date=s_date
            )
            if not self.__remove_working_sessions or not isWorkingHours(s.get_date()):
                sessions.add(s)
            if event.end.strftime("%H:%M") != "23:59":
                durations.add(int((event.end - event.begin).seconds / 60))
        if len(sessions) == 0:
            return 0, tuple()
        duration = self.__get_duration(durations, url_event)
        return duration, tuple(sorted(sessions))

    def __get_duration(self, durations: Set[int], url_event: str):
        limit = (24*60)-1
        ok = set((d for d in durations if d < limit))
        duration = max(ok) if ok else limit
        if duration < limit:
            return duration
        duration = self.__get_duration_from_madrides(url_event)
        if duration is not None:
            return duration
        more_url = find_more_url(url_event)
        dom = get_domain(more_url)
        if dom == "madrid.es":
            duration = self.__get_duration_from_madrides(more_url)
            if duration is not None:
                return duration
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

    def __get_duration_from_madrides(self, url: str):
        desc = self.__get_description(url)
        if not desc:
            return None
        for r in (
            r"\bDuraci[óo]n[:\s]+(\d+) min",
        ):
            m = re.search(r, desc, flags=re.I)
            if m is None:
                continue
            duration = int(m.group(1))
            logger.debug(f"FIX duration={duration} <- {url}")
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
            logger.debug(f"FIX duration={duration} <- {url}")
            return duration

    def __get_start(self, start: Arrow, url_event: str):
        ko_hour = ("00:00", None)
        s_date = start.strftime("%Y-%m-%d %H:%M")
        s_day, s_hour = s_date.split()
        if s_hour not in ko_hour:
            return s_date
        s_hour = self.__get_start_from_madrides(url_event)
        if s_hour not in ko_hour:
            return f"{s_day} {s_hour}"
        more_url = find_more_url(url_event)
        dom = get_domain(more_url)
        if dom == "madrid.es":
            s_hour = self.__get_start_from_madrides(more_url)
            if s_hour not in ko_hour: 
                return f"{s_day} {s_hour}"
        if dom == "centrodanzamatadero.es":
            soup = WEB.get_cached_soup(more_url)
            for txt in map(get_text, soup.select(".inner-wrapper.card .field__item")):
                if txt is None:
                    continue
                m = re.match(r"^(\d\d:\d\d)(\s*h)?$", txt)
                if m:
                    return f"{s_day} {m.group(1)}"
        return s_date

    def __get_start_from_madrides(self, url: str):
        desc = self.__get_description(url)
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
                logger.debug(f"FIX hour={hm} <- {url}")
                return hm
        return None

    def __get_cal(self, div: Tag):
        cal = div.select_one("p.event-date a")
        if cal is None:
            return None
        url = cal.attrs["href"]
        logger.debug(url)
        r = self.w._get(url)
        valid_lines: list[str] = []
        for line in r.text.splitlines():
            if not line.strip():
                continue
            if line.startswith(("BEGIN", "END", " ")):
                valid_lines.append(line)
                continue
            if ":" not in line:
                continue
            field = re.split(r"[;:]", line)[0].strip()
            if len(field) == 0 or field != field.upper():
                continue
            m = re.match(r"^\s*(DTSTAMP|DTSTART|DTEND)\s*:\s*(\d+T[\d:Z]+)\s*$", line)
            if m:
                k, v = m.groups()
                if re.match(r"^\d{8}T\d\d:\d\d:\d\dZ$", v):
                    v = v.replace(":", "")[:-1]
                    line = f"{k}:{v}"
            valid_lines.append(line)
        try:
            return Calendar("\n".join(valid_lines))
        except (NotImplementedError, FailedParse, KeyError) as e:
            logger.error(str(e)+" "+url)
            return None

    def __find_category(self, id: str, div: Tag, url_event: str):
        plain_type = plain_text(safe_get_text(div.select_one("p.event-type")))
        name = (get_text(div.select_one("a.event-link")) or "").lower()
        plain_name = plain_text(name)
        if re_or(plain_name, r"d[íi]a mundial de la poes[íi]a", r"encuentro po[ée]tico", r"Recital de poes[íi]a", r"Versos entrevistados", to_log=id, flags=re.I):
            return Category.POETRY

        note_place = div.select_one("a.event-location")
        plain_place = plain_text(note_place.attrs["data-name"]) if note_place else None
        if re_or(plain_place, "titeres", to_log=id):
            return Category.PUPPETRY

        name_tp = re.split(r"\s*[:'\"\-]", name)[0].lower()
        tp_name = plain_text(((plain_type or "")+" "+plain_name).strip())
        maybeSPAM = any([
            re_or(plain_name, "el mundo de los toros", "el mundo del toro", "federacion taurina", "tertulia de toros", to_log=id),
            re_and(plain_name, "actos? religios(os)?", ("santo rosario", "eucaristia", "procesion"), to_log=id),
        ])

        for ids, cat in self._category.items():
            if id in ids:
                if maybeSPAM and cat == Category.CONFERENCE:
                    return Category.SPAM
                logger.debug(f"{id} en {cat}")
                return cat

        if re_and(tp_name, "taller", ("animales", "pequeños"), to_log=id):
            return Category.CHILDISH
        if re_and(tp_name, "dia", "internacional", "familias?", to_log=id):
            return Category.CHILDISH
        if re_or(tp_name, "concierto infantil", "en familia", r"[Ee]laboraci[óo]n de comederos de aves", r"[Ll]os [\d\. ]+ primeros d[íi]as no se repiten", "[pP]hotocall hinchable", to_log=id):
            return Category.CHILDISH
        if re_or(plain_name, "^re vuelta al patio", to_log=id):
            return Category.CHILDISH
        if re_or(plain_name, "para mayores$", to_log=id):
            return Category.SENIORS
        if maybeSPAM:
            return Category.SPAM
        if re_or(plain_name, "Mejora tu ingl[eé]s con charlas", "POM Condeduque", to_log=id, flags=re.I):
            return Category.WORKSHOP
        if re_or(plain_name, "Salida medioambiental", to_log=id, flags=re.I):
            return Category.HIKING
        if re_or(plain_name, 
                 "recital de piano",
                 r"Cuartero de C[áa]mara",
                 r"Arias de [Óo]pera",
                 "No cesar[áa]n mis cantos",
                 to_log=id,
                 flags=re.I
            ):
            return Category.MUSIC
        if re_and(plain_name, "ballet", ("repertorio", "clasico"), to_log=id):
            return Category.DANCE
        if re_or(plain_name, "certamen( de)? (pintura|decoracion)", "festival by olavide", to_log=id):
            return Category.EXPO
        if re_or(plain_name, r"Representaci[óo]n(es)? teatral(es)?", to_log=id, flags=re.I):
            return Category.THEATER
        if re_or(name_tp, r"^exposici[oó]n(es)$", to_log=id):
            return Category.EXPO
        if re_or(name_tp,
                 r"^conferencias?$",
                 r"^pregon$",
                 r'[Mm]ocrofestival, tableros y pantallas',
                 to_log=id
            ):
            return Category.CONFERENCE
        if re_or(name_tp, r"^conciertos?$", to_log=id):
            return Category.MUSIC
        if re_or(plain_name, "cañon del rio", "ruta a caballo", "cerro de", r"actividad(es)? acuaticas? pantano", to_log=id):
            return Category.SPORT
        if re_or(name_tp, r"^teatros?$", to_log=id):
            return Category.THEATER
        if re_or(name_tp, r"^danzas?$", to_log=id):
            return Category.DANCE
        if re_or(name_tp, r"^cine$", to_log=id):
            return Category.CINEMA
        if re_or(name_tp, r"^visitas? guiadas?$", to_log=id):
            return Category.VISIT
        if re_or(plain_name, r"^exposicion y (charla|coloquio)", r"europa ilustra", to_log=id):
            return Category.EXPO
        if re_or(plain_name, r"^conferencia y (charla|coloquio)", to_log=id):
            return Category.CONFERENCE
        if re_or(
            plain_name,
            r"^taller",
            "tertulias en latin",
            r"taller(es)? de calidad del aire",
            "compostagram",
            "esquejodromo",
            to_log=id
        ):
            return Category.WORKSHOP
        if re_or(plain_name, "visitas guiadas para", "Recorrido por la Iluminaci[óo]n", to_log=id, flags=re.I):
            return Category.VISIT
        if re_or(plain_name, "^concierto de", to_log=id):
            return Category.MUSIC
        if re_or(tp_name, ("espectaculo", "magia"), r"\b[Ll]a magia de", to_log=id):
            return Category.MAGIC
        if re_or(tp_name, "cine", "proyeccion(es)?", "cortometrajes?", to_log=id):
            return Category.CINEMA
        if re_or(tp_name, "musica", "musicales", "conciertos?", "hip-hob", "jazz", "reagge", "flamenco", "batucada", "rock", to_log=id):
            return Category.MUSIC
        if re_or(tp_name, "teatro", "zarzuela", "lectura dramatizada", to_log=id):
            return Category.THEATER
        if re_or(tp_name, "exposicion(es)?", "noche de los museos", to_log=id):
            return Category.EXPO
        if re_or(plain_type, "danza", "baile", to_log=id):
            return Category.DANCE
        if re_or(tp_name, "conferencias?", "coloquios?", "presentacion(es)?", to_log=id):
            return Category.CONFERENCE
        if re_or(tp_name, "charlemos sobre", to_log=id):
            return Category.CONFERENCE
        if re_or(tp_name, "club(es)? de lectura", to_log=id):
            return Category.READING_CLUB
        if re_or(tp_name, ("elaboracion", "artesanal"), to_log=id):
            return Category.WORKSHOP
        if re_or(plain_type, "cursos?", "taler(es)?", "capacitacion", to_log=id):
            return Category.WORKSHOP
        if re_or(plain_type, "concursos?", "certamen(es)?", to_log=id):
            return Category.CONTEST
        if re_or(plain_type, "actividades deportivas", to_log=id):
            return Category.SPORT
        if re_or(
            plain_name,
            "^senderismo",
            r"^senda",
            "senda botanica",
            "excursion medioambiental",
            r"^del? .* a casa de campo$",
            "^salida multiaventura",
            r"(paseo|itinerario) ornitologico",
            r"^entreparques",
            ("deportes?", "torneo"),
            to_log=id
        ):
            return Category.SPORT
        if re_or(plain_place, "educacion ambiental") and re_or(plain_name, "^arroyo", to_log=id):
            return Category.SPORT
        if re_or(plain_place, "imprenta") and re_or(tp_name, "demostracion(es)?", "museos?", to_log=id):
            return Category.EXPO
        if re_or(plain_name, "^(danza|chotis)", to_log=id):
            return Category.DANCE
        if re_or(plain_name, "^(charlas?|ensayos?)", to_log=id):
            return Category.CONFERENCE
        if re_or(plain_name, "^(acompañamiento digital)", to_log=id):
            return Category.WORKSHOP
        if re_or(plain_name, "^(webinario)", to_log=id):
            return Category.ONLINE
        if re_or(plain_name, "^(paseo|esculturas)", "de el retiro$", to_log=id):
            return Category.VISIT
        if re_or(plain_name, "^mercadea en el mercado", "^mercadea en los mercadillos", to_log=id):
            return Category.CONFERENCE
        if re_or(plain_name, "^poesia rapidita", r"^\d+ poemas", "poesia o barbarie", to_log=id):
            return Category.POETRY
        if re_or(plain_name, "^hacer actuar", to_log=id):
            return Category.WORKSHOP
        if re_or(plain_name, "^concentracion", to_log=id):
            return Category.ACTIVISM
        if re_or(plain_type, r"visitas?", to_log=id):
            return Category.VISIT
        if re_or(plain_name, r"visita a", to_log=id):
            return Category.VISIT
        if re_or(plain_type, "jornadas?", "congresos?", to_log=id):
            return Category.CONFERENCE
        if re_or(plain_name, "actuacion coral", "recital coral", "taller de sevillanas", to_log=id):
            return Category.MUSIC
        if re_or(plain_name, "encuentro artistico", to_log=id):
            return Category.EXPO
        if re_or(plain_name, "^(cantando|banda municipal)", to_log=id):
            return Category.MUSIC
        if re_and(plain_name, "dialogos?", "mac"):
            return Category.CONFERENCE
        if re_or(plain_name, "lengua de signos", r"^[Tt]alleres"):
            return Category.WORKSHOP
        if re_or(plain_name, "^El mago", flags=re.I, to_log=id):
            return Category.MAGIC
        if re_and(plain_name, "fiesta", "aniversario", flags=re.I, to_log=id):
            return Category.PARTY

        desc = self.__get_description(url_event)
        if re_or(desc, "[mM]usical? infantil", r"[Tt]eatro infantil", "relatos en familia", "concierto familiar", "bienestar de niños y niñas", ("cuentacuentos", "en familia"), to_log=id, flags=re.I):
            return Category.CHILDISH
        if re_or(desc, "zarzuela", "teatro", "espect[áa]culo (circense y )?teatral", to_log=id, flags=re.I):
            return Category.THEATER
        if re_or(desc, "itinerario .* kil[ó]metros", to_log=id, flags=re.I):
            return Category.SPORT
        if re_or(plain_name, "actuacion", "verbena") and re_or(desc, "música", "concierto", "canciones", "pop", "rock", "baila", "bailable", "cantante", " d[ée]cada prodigiosa", to_log=id, flags=re.I):
            return Category.MUSIC
        if re_or(desc, "Concierto", r"\b[Uu]n concierto de", r"\b[Gg][oó]spel", to_log=id):
            return Category.MUSIC
        if re_or(desc, r"intervienen l[oa]s", "una mesa redonda con", " encuentro del ciclo Escritores", to_log=id, flags=re.I):
            return Category.CONFERENCE
        if desc and desc.count("poesía") > 2 or re_or(desc, "presentación del poemario", "recital de poesía", "presenta su poemario", flags=re.I):
            return Category.POETRY
        if re_or(desc,
                 "propuesta creativa y participativa que combina lectura, escritura y expresión",
                 r"Se organizará un '?escape room'?",
                 "taller creativo",
                 "pensado para ejercitar la memoria",
                 "m[óo]dulo pr[aá]ctico",
                 to_log=id,
                 flags=re.I
            ):
            return Category.WORKSHOP
        if re_and(desc, r"presentaci[oó]n", (r"libros?", r"novelas?"), (r"autore(es)?", r"autoras?"), to_log=id):
            return Category.CONFERENCE

        if re_and(plain_place, "ambiental", ("casa de campo", "retiro"), to_log=id):
            return Category.VISIT

        logger.critical(str(CategoryUnknown(url_event, f"{id}: type={plain_type}, name={plain_name}")))
        return Category.UNKNOWN

    @staticmethod
    def get_id(lk: str):
        if lk is None or get_domain(lk) != "madrid.es":
            return None
        qr = get_query(lk)
        id = qr.get("vgnextoid")
        if id is None:
            return None
        return "ms"+id

    def __get_soup_events(self, action: str, data=None):
        def _get(url: str):
            soup = self.get(url)
            arr = soup.select("#listSearchResults ul.events-results li div.event-info")
            a_next = soup.select_one("li.next a.pagination-text")
            logger.debug(f"{len(arr)} en {url}")
            if a_next is None:
                return None, arr
            return a_next.attrs["href"], arr

        if data:
            action = action + '?' + urlencode(data)
        url = str(action)
        rt_arr: Dict[str, Tuple[Tag, Tag]] = {}
        while url:
            url, arr = _get(url)
            for div in arr:
                a = div.select_one("a.event-link")
                if a is None:
                    continue
                lk = a.attrs.get("href")
                id = MadridEs.get_id(lk)
                if id is None:
                    continue
                rt_arr[id] = (a, div)
        logger.debug(f"{len(rt_arr)} TOTAL en {action}")
        return tuple((id, a, div) for id, (a, div) in rt_arr.items())

    def prepare_search(self):
        self.get(MadridEs.AGENDA)
        action, data = self.w.prepare_submit("#generico1", enviar="buscar")
        if action is None:
            raise WebException(f"#generico1 NOT FOUND in {self.w.url}")
        for k in ("gratuita", "movilidad"):
            if k in data:
                del data[k]
        #data['gratuita'] = "1"
        data["tipo"] = "-1"
        data["distrito"] = "-1"
        data["usuario"] = "-1"
        return action, data

    def iter_submit(self):
        action, data = self.prepare_search()

        data = {k: v for k, v in data.items() if v is not None}
        aux = dict(data)

        def do_filter(**kwargs):
            return bool(len(self.__get_soup_events(action, {**aux, **kwargs})))

        def my_filter(k, arr, **kwargs):
            return tuple(filter(lambda v: do_filter(**{**kwargs, **{k: v}}), arr))

        for dis in my_filter("distrito", self.zona.keys()):
            data["distrito"] = dis
            yield action, data

    @cached_property
    def zona(self):
        data: Dict[str, str] = {}
        for k, v in self.distritos.items():
            if re.search(r"arganzuela|centro|moncloa|chamberi|retiro|salamaca|villaverde|carabanchel", plain_text(v)):
                data[k] = v
        return data

    @cached_property
    def distritos(self):
        return self.__get_options("#distrito")

    @cached_property
    def usuarios(self):
        return self.__get_options("#usuario")

    @cached_property
    def gente(self):
        gente: Dict[str, str] = {}
        for k, v in self.usuarios.items():
            if re_or(
                plain_text(v),
                "familias",
                "jovenes",
                "mayores",
                "mujeres",
                "niñas",
                "niños",
                "poblacion general"
            ):
                gente[k] = v
        return gente

    def __get_options(self, slc):
        data: Dict[str, str] = {}
        soup = self.get(MadridEs.AGENDA)
        for o in soup.select(slc+" option"):
            k = o.attrs["value"]
            v = re_sp.sub(" ", o.get_text()).strip()
            if k != "-1":
                data[k] = v
        return data

    @cached_property
    def tipos(self):
        data: Dict[str, str] = {}
        soup = self.get(MadridEs.TAXONOMIA, parser="xml")
        for n in soup.find_all('item'):
            value = n.find('value').string.strip()
            text = re_sp.sub(" ", n.find('text').string).strip()
            data[value] = text
        return data


if __name__ == "__main__":
    from .log import config_log
    config_log("log/madrides.log", log_level=(logging.DEBUG))
    print(MadridEs().events)
    #m.get_events()
