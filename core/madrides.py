from .web import Web, WebException
from bs4 import Tag, BeautifulSoup
import re
from typing import Set, Dict, List, Tuple
from urllib.parse import urlencode
from .event import Event, Session, Place, Category, CategoryUnknown
from .util import plain_text, re_or, re_and, getKm, my_filter, get_main_value
from ics import Calendar
from arrow import Arrow
import logging
from .cache import TupleCache
from urllib.parse import urlparse, parse_qs
from functools import cached_property, cache
from collections import defaultdict

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
    t = n.get_text()
    t = re_sp.sub(" ", t)
    t = re.sub(r'[“”]', '"', t)
    return t.strip()


def clean_lugar(s: str):
    if re.search(r"Centro Cultural Casa del Reloj", s, flags=re.IGNORECASE):
        return "Centro cultural Casa del Reloj"
    s = re.sub(r"^Biblioteca Pública( Municipal)?", "Biblioteca", s)
    s = re.sub(r"\s+\(.*?\)\s*$", "", s)
    s = re.sub(r"^Mercado municipal de ", "Mercado ", s, flags=re.IGNORECASE)
    s = re.sub(
        r"^Espacio de igualdad ([^\.]+)\..*$", r"Espacio de igualdad \1",
        s,
        flags=re.IGNORECASE
    )
    s = re.sub(
        r"^Centro de Información y Educación Ambiental de (.*)$",
        r"Centro de información y educación ambiental de \1",
        s,
        flags=re.IGNORECASE
    )
    s = re.sub(r"^(Matadero) (Medialab|Madrid)$", r"\1", s, flags=re.IGNORECASE)
    s = re.sub(r"^(Cineteca) Madrid$", r"\1", s, flags=re.IGNORECASE)
    s = re.sub(r"^(Imprenta Municipal)\s.*$", r"\1", s, flags=re.IGNORECASE)
    s = re.sub(r"^Centro (cultural|sociocultural)\b", "Centro cultural", s, flags=re.IGNORECASE)
    lw = plain_text(s).lower()
    if lw.startswith("museo de san isidro"):
        return "Museo San Isidro"
    for txt in (
        "Biblioteca Eugenio Trías",
        "Centro Dotacional Integrado Arganzuela",
        "Centro Danza Matadero",
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


OK_ZONE = {
    # Villaverde Bajo
    (40.352672, -3.684576): 1,
    # Legazpi
    (40.391225, -3.695124): 2,
    # Delicias
    (40.400400, -3.692774): 2,
    # Banco de España
    (40.419529, -3.693949): 3,
    # Moncloa
    (40.434616, -3.719097): 1,
    # Pacifico
    (40.401874, -3.674703): 1,
    # Sainz de Baranda
    (40.414912, -3.669639): 1,
    # Oporto
    (40.388966, -3.731448): 1
}


@cache
def isOkPlace(p: Place):
    if re.search(r"\bcentro juvenil\b", p.name, flags=re.IGNORECASE):
        return False
    if p.latlon is None:
        return True
    kms: list[float] = []
    lat, lon = map(float, p.latlon.split(","))
    for (lt, ln), km in OK_ZONE.items():
        kms.append(getKm(lat, lon, lt, ln))
        if kms[-1] <= km:
            return True
    k = round(min(kms))
    logger.debug(f"Lugar descartado {k}km {p.name} {p.url}")
    return False


class MadridEs:
    AGENDA = "https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/?vgnextfmt=default&vgnextchannel=ca9671ee4a9eb410VgnVCM100000171f5a0aRCRD"
    TAXONOMIA = "https://www.madrid.es/ContentPublisher/jsp/apl/includes/XMLAutocompletarTaxonomias.jsp?taxonomy=/contenido/actividades&idioma=es&onlyFirstLevel=true"

    def __init__(self):
        self.w = Web()

    @cached_property
    def __category(self):
        action, data_form = self.prepare_search()
        category: Dict[Category, Set[str]] = defaultdict(set)
        tipos = {plain_text(v): k for k, v in self.tipos.items()}
        usuarios = {plain_text(v): k for k, v in self.usuarios.items()}

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
                    return
                logger.debug(f"{cat} = {key} in {tuple(sorted(data_txt))}")
                for v in sorted(data_val):
                    data[key] = v
                    category[cat] = category[cat].union(
                        self.__get_ids(action, data)
                    )

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
            ),
            Category.MUSIC: (
                r"(opera)\b.*teatro",
                r"flamenco\b.*danza",
                r"(rap|jazz|soul|funky|swing|reagge|flamenco|clasica|batucada|latina|española|electronica|rock|pop|folk|country)\bmusica",
            ),
            Category.DANCE: (
                r"(clasica|tango|breakdance|contemporane(a|o))\b.*danza",
            ),
            Category.SPORT: (
                r'deportivas',
            ),
            Category.CINEMA: (
                r'(documental|ficcion|cine experimental)\b.*cine',
            ),
            Category.CONFERENCE: (
                r'congresos?',
                r'jornadas?',
                r'presentacion(es)?',
                r'actos? literarios?',
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
            return self.w.get(url, *args, **kwargs)
        return self.w.soup

    @cache
    def __get_description(self, url: str):
        w = Web()
        w.get(url)
        return plain_text(get_text(w.select_one("div.tramites-content div.tiny-text")))

    @property
    @TupleCache("rec/madrides.json", builder=Event.build)
    def events(self) -> Tuple[Event, ...]:
        all_events: Set[Event] = set()
        for action, data in self.iter_submit():
            all_events = all_events.union(self.__get_events(action, data))
        if len(all_events) == 0:
            return tuple()

        def _merge(events: List[Event]):
            if len(events) == 1:
                return events[0]
            logger.debug("Fusión: " + " + ".join(map(lambda e: f"{e.id} {e.duration}", events)))
            sessions: Set[Session] = set()
            categories: List[Category] = []
            durations: List[float] = []
            imgs: List[str] = []
            for e in events:
                if e.category not in (None, Category.UNKNOWN):
                    categories.append(e.category)
                if e.duration is not None:
                    durations.append(e.duration)
                if e.img is not None:
                    imgs.append(e.img)
                for s in e.sessions:
                    sessions.add(s._replace(
                        url=s.url or e.url
                    ))
            return events[0].merge(
                url=None,
                duration=get_main_value(durations),
                img=get_main_value(imgs),
                category=get_main_value(categories, default=Category.UNKNOWN),
                sessions=tuple(sorted(sessions, key=lambda s: (s.date, s.url))),
            )

        mrg_events: Set[Event] = set()
        ko_events = sorted(all_events)
        empty = {k: None for k in ko_events[0]._asdict().keys()}

        while ko_events:
            e = ko_events[0]
            k: Event = Event.build({
                **empty,
                **{
                    'name': e.name,
                    'place': e.place,
                    'category': None
                }
            })
            ok, ko_events = my_filter(ko_events, lambda x: x.isSimilar(k))
            mrg_events.add(_merge(ok))
        arr_events: List[Event] = []
        for e in sorted(mrg_events):
            urls: List[str] = []
            if e.url:
                urls.append(e.url)
            for s in e.sessions:
                if s.url and s.url not in urls:
                    urls.append(s.url)
            while e.img is None and urls:
                img = self.get(urls.pop(0)).select_one("div.image-content img")
                if img:
                    e = e.merge(img=img.attrs["src"])
            arr_events.append(e)

        return tuple(sorted(arr_events))

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
            cat = self.__find_category(id, div, url_event)
            if cat is None:
                continue
            duration, sessions = self.__get_sessions(url_event, div)
            if len(sessions) == 0:
                continue
            ev = Event(
                id=id,
                url=url_event,
                name=get_text(a),
                img=None,
                price=0,
                category=cat,
                place=place,
                duration=duration,
                sessions=sessions
            )
            evts.add(ev)
        return evts

    def __get_sessions(self, url_event: str, div: Tag) -> Tuple[int, Tuple[Session, ...]]:
        cal = self.__get_cal(div)
        if cal is None:
            return 0, tuple()
        durations: Set[int] = set()
        sessions: Set[Session] = set()
        for event in cal.events:
            start = event.begin
            end = event.end
            durations.add(int((end - start).seconds / 60))
            s_date = self.__get_start(start, url_event)
            sessions.add(Session(
                date=s_date
            ))
        if len(durations) == 0:
            return 0, tuple()
        duration = max(durations) # self.__get_duration(durations, url_event)
        return duration, tuple(sorted(sessions))

    def __get_duration(self, durations: Set[int], url_event: str):
        limit = 1439
        ok = set((d for d in durations if d < limit))
        duration = max(ok) if durations else limit
        if duration < limit:
            return duration
        desc = self.__get_description(url_event)
        if not desc:
            return 60
        # TODO
        pass

    def __get_start(self, start: Arrow, url_event: str):
        s_date = start.strftime("%Y-%m-%d %H:%M")
        return s_date
        # TODO
        s_day, s_hour = s_date.split()
        if s_hour != "00:00":
            return s_date
        desc = self.__get_description(url_event)
        if not desc:
            return s_date
        for r in (
            r"\bsalida desde *?, a las (\d+(:\d+)?) h",
            r"\bHorario (\d+(:\d+)?) h",
            r"\ba las (\d+(:\d+)?) h",
            r"\bde (\d+(:\d+)?) a (\d+(:\d+)?) h",
        ):
            m = re.search(r, desc, flags=re.IGNORECASE)
            if m is None:
                continue
            h: str = m.group(1)
            if h.isdigit() and int(h) < 25:
                h = f"{int(h):02d}:00"
            while len(h) < 5:
                h = "0"+h
            if re.match(r"^([0-1][0-9]|2[0-4]):([0-5][0-9]|60)$", h):
                logger.debug(f"FIX {h} <- {url_event}")
                return f"{s_day} {h}"
        return s_date

    def __get_cal(self, div: Tag):
        cal = div.select_one("p.event-date a")
        if cal is None:
            return None
        url = cal.attrs["href"]
        logger.debug(url)
        r = self.w._get(url)
        try:
            return Calendar(r.text)
        except NotImplementedError as e:
            logger.error(str(e)+" "+url)
            return None

    def __find_category(self, id: str, div: Tag, url_event: str):
        for ids, cat in self.__category.items():
            if id in ids:
                return cat
        lg = div.select_one("a.event-location")
        lg = plain_text(lg.attrs["data-name"]) if lg else None
        if re_or(lg, "titeres"):
            return Category.PUPPETRY
        tp = plain_text(safe_get_text(div.select_one("p.event-type")))
        name = plain_text(get_text(div.select_one("a.event-link")))
        tp_name = plain_text(((tp or "")+" "+name).strip())
        if re_and(tp_name, "taller", ("animales", "pequeños"), to_log=id):
            return Category.CHILDISH
        if re_and(tp_name, "dia", "internacional", "familias?", to_log=id):
            return Category.CHILDISH
        if re_or(tp_name, "concierto infantil", "en familia", to_log=id):
            return Category.CHILDISH
        if re_or(name, "^re vuelta al patio"):
            return Category.CHILDISH
        if re_or(name, "el mundo de los toros", "el mundo del toro"):
            return Category.SPAM
        if re_and(name, "ballet", ("repertorio", "clasico")):
            return Category.DANCE
        if re_or(name, r"^exposicion\s[:'\"\-].*$", "^exposicion y (charla|coloquio)", "europa ilustra", to_log=id):
            return Category.EXPO
        if re_or(name, r"^conferencia\s[:'\"\-].*$", "^conferencia y (charla|coloquio)", to_log=id):
            return Category.CONFERENCE
        if re_or(
            name,
            r"^taller",
            "tertulias en latin",
            "taller(es)? de calidad del aire",
            "compostagram",
            "esquejodromo",
            to_log=id
        ):
            return Category.WORKSHOP
        if re_or(name, r"^visita guiada\s[:'\"\-].*$", "visitas guiadas para", to_log=id):
            return Category.VISIT
        if re_or(name, r"^concierto:", to_log=id):
            return Category.MUSIC
        if re_or(tp_name, ("espectaculo", "magia")):
            return Category.MAGIC
        if re_or(tp_name, "cine", "proyeccion(es)?", "cortometrajes?", to_log=id):
            return Category.CINEMA
        if re_or(tp_name, "musica", "musicales", "conciertos?", "hip-hob", "jazz", "reagge", "flamenco", "batucada", "rock", to_log=id):
            return Category.MUSIC
        if re_or(tp_name, "teatro", "zarzuela", "lectura dramatizada", to_log=id):
            return Category.THEATER
        if re_or(tp_name, "exposicion(es)?", "noche de los museos", to_log=id):
            return Category.EXPO
        if re_or(tp, "danza", "baile", to_log=id):
            return Category.DANCE
        if re_or(tp_name, "conferencias?", "coloquios?", "presentacion(es)?", to_log=id):
            return Category.CONFERENCE
        if re_or(tp_name, "charlemos sobre", to_log=id):
            return Category.CONFERENCE
        if re_or(tp_name, "club(es)? de lectura", to_log=id):
            return Category.READING_CLUB
        if re_or(tp_name, ("elaboracion", "artesanal"), to_log=id):
            return Category.WORKSHOP
        if re_or(tp, "cursos?", "taler(es)?", "capacitacion", to_log=id):
            return Category.WORKSHOP
        if re_or(tp, "concursos?", "certamen(es)?", to_log=id):
            return Category.CONTEST
        if re_or(tp, "actividades deportivas", to_log=id):
            return Category.SPORT
        if re_or(
            name,
            "^senderismo",
            r"^senda",
            "senda botanica",
            "excursion medioambiental",
            r"^del? .* a casa de campo$",
            "^salida multiaventura",
            r"(paseo|itinerario) ornitologico",
            r"^entreparques",
            to_log=id
        ):
            return Category.SPORT
        if re_or(lg, "educacion ambiental") and re_or(name, "^arroyo", to_log=id):
            return Category.SPORT
        if re_or(lg, "imprenta") and re_or(tp_name, "demostracion(es)?", "museos?", to_log=id):
            return Category.EXPO
        if re_or(name, "^(danza|chotis)", to_log=id):
            return Category.DANCE
        if re_or(name, "^(charlas?|ensayos?)", to_log=id):
            return Category.CONFERENCE
        if re_or(name, "^(acompañamiento digital)", to_log=id):
            return Category.WORKSHOP
        if re_or(name, "^(webinario)", to_log=id):
            return Category.ONLINE
        if re_or(name, "^(paseo|esculturas)", "de el retiro$", to_log=id):
            return Category.VISIT
        if re_or(name, "^mercadea en el mercado", "^mercadea en los mercadillos", to_log=id):
            return Category.CONFERENCE
        if re_or(name, "^poesia rapidita", r"^\d+ poemas", to_log=id):
            return Category.POETRY
        if re_or(name, "^hacer actuar", to_log=id):
            return Category.WORKSHOP
        if re_or(name, "^concentracion", to_log=id):
            return Category.ACTIVISM
        if re_or(tp, r"visitas?", to_log=id):
            return Category.VISIT
        if re_or(name, r"visita a", to_log=id):
            return Category.VISIT
        if re_or(tp, "jornadas?", "congresos?"):
            return Category.CONFERENCE

        desc = self.__get_description(url_event)
        if re_or(desc, "zarzuela", "teatro", to_log=id):
            return Category.THEATER
        if re_or(desc, "itinerario .* kilometros", to_log=id):
            return Category.SPORT
        if desc.count("poesia") > 2:
            return Category.CONFERENCE

        if re_and(lg, "ambiental", ("casa de campo", "retiro"), to_log=id):
            return Category.VISIT

        logger.critical(str(CategoryUnknown(url_event, f"{id}: type={tp}, name={name}")))
        return Category.UNKNOWN

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
                if lk is None:
                    continue
                qr = get_query(lk)
                id = qr.get("vgnextoid")
                if id is None:
                    continue
                rt_arr["ms"+id] = (a, div)
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
        data['gratuita'] = "1"
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
        for n in soup.findAll('item'):
            value = n.find('value').string.strip()
            text = re_sp.sub(" ", n.find('text').string).strip()
            data[value] = text
        return data


if __name__ == "__main__":
    from .log import config_log
    config_log("log/madrides.log", log_level=(logging.DEBUG))
    print(MadridEs().events)
    #m.get_events()
