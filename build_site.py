#!/usr/bin/env python3

from core.event import Event, Category, Session, Cinema
from core.ics import IcsEvent
from core.casaencendida import CasaEncendida
from core.dore import Dore
from core.madriddestino import MadridDestino
from core.salaberlanga import SalaBerlanga
from core.salaequis import SalaEquis
from core.casaamerica import CasaAmerica
from core.academiacine import AcademiaCine
from core.caixaforum import CaixaForum
from core.madrides import MadridEs
from core.telefonica import Telefonica
from core.j2 import Jnj2, toTag
from datetime import datetime, timedelta
from core.log import config_log
from core.img import MyImage
from core.util import dict_add, get_domain, to_datetime, uniq, to_uuid, find_duplicates
import logging
from os import environ
from os.path import isfile
from typing import Tuple, Dict, Set, List
from core.filemanager import FM
import math
import bs4
import re
import pytz
from core.rss import EventosRss
from collections import defaultdict
from core.wiki import WIKI
from core.filmaffinity import FilmAffinityApi
from core.dblite import DB

import argparse

parser = argparse.ArgumentParser(description='Lista eventos')
parser.add_argument('--precio', type=int, help="Precio máximo", default=5)

args = parser.parse_args()
PAGE_URL = environ['PAGE_URL']
OUT = "out/"

config_log("log/build_site.log")
logger = logging.getLogger(__name__)
white = (255, 255, 255)
NOW = datetime.now(tz=pytz.timezone('Europe/Madrid'))
PUBLISH: dict[str, str] = FM.load(OUT+"publish.json")


def distance_to_white(*color) -> Tuple[int]:
    arr = []
    for c in color:
        d = math.sqrt(sum([(c1 - c2) ** 2 for c1, c2 in zip(c, white)]))
        arr.append(d)
    return tuple(arr)


def get_trim_image(im: MyImage):
    tr = im.trim()
    if tr is None or tr.isKO:
        return None
    if (im.isLandscape and tr.isPortrait):
        return tr
    if len(set(im.im.size).intersection(tr.im.size)) == 1:
        return tr
    diff_height = abs(im.im.height-tr.im.height)
    diff_width = abs(im.im.width-tr.im.width)
    if diff_height < (im.im.height*0.10) and diff_width > (im.im.width*0.20):
        return tr
    if diff_width < (im.im.width*0.10) and diff_height > (im.im.height*0.20):
        return tr
    dist = distance_to_white(im.get_corner_colors().get_most_common())
    if max(dist) < 260:
        return tr
    return None


def add_image(e: Event):
    if e.img is None:
        return (None, e)
    local = f"img/{e.id}.jpg"
    file = OUT+local
    im = MyImage.get(e.img)
    if isfile(file):
        lc = MyImage(file, parent=im, background=im.background)
    else:
        if im.isKO:
            return (im, e)
        width = 500
        height = [im.im.height, 300, width*(9/16)]
        im = get_trim_image(im) or im
        tb = im.thumbnail(width=width, height=min(height))
        if tb is None or tb.isKO:
            return (im, e)
        lc = tb.save(file, quality=80)
        if lc is None or lc.isKO:
            return (im, e)
    lc.url = PAGE_URL+'/'+local
    return (lc, e)


OK_CAT = (
    Category.CINEMA,
    Category.MUSIC,
    Category.THEATER,
    Category.DANCE,
    Category.CONFERENCE,
    Category.VISIT,
    Category.MAGIC,
    Category.UNKNOWN,
)


def myfilter(e: Event):
    if e.place.name in (
        "Espacio Abierto Quinta de los Molinos",
        #"Centro Sociocultural Ágata"
    ):
        return False
    if e.price > args.precio:
        return False
    if e.category not in OK_CAT:
        return False

    #if "madrid.es" in map(get_domain, e.iter_urls()):
    #    if e.place.name in (
    #        "Faro de Moncloa"
    #    ):
    #        # Ya registrado en madrid-destino
    #        return False

    e.remove_old_sessions(NOW)
    e.remove_working_sessions()

    count_session = len(e.sessions)
    if count_session == 0:
        return False
    if count_session > 20:
        logger.warning(f"{e.id} tiene {count_session} sesiones {e.url}")
        return False
    return True


def find_filmaffinity_if_needed(imdb_film: dict[str, int], e: Cinema):
    if not isinstance(e, Cinema):
        return None
    if isinstance(e.filmaffinity, int):
        return None
    _id_ = imdb_film.get(e.imdb)
    if isinstance(_id_, int):
        return _id_
    if isinstance(e.cycle, str):
        return None
    if isinstance(e.imdb, str):
        db_year = e.year or DB.one("select year from MOVIE where id = ?", e.imdb)
        db_title = DB.to_tuple("select title from TITLE where movie = ?", e.imdb)
        _id_ = FilmAffinityApi.search(
            db_year,
            *db_title
        )
        if isinstance(_id_, int):
            return _id_
    _id_ = FilmAffinityApi.search(e.year, *e.get_full_aka())
    if isinstance(_id_, int):
        return _id_


def sorted_and_fix(eventos: List[Event]):
    def _iter_fix(eventos: List[Event]):
        done: set[Event] = set()
        for e in eventos:
            e = e.fix_type()
            e = e.fix(publish=PUBLISH.get(e.id))
            if e not in done:
                done.add(e)
                if myfilter(e):
                    PUBLISH[e.id] = e.publish
                    yield e

    ok_events = set(_iter_fix(eventos))

    def _mk_key_madrid_music(e: Event):
        if e.category != Category.MUSIC:
            return None
        if "madrid.es" not in (get_domain(e.url),  get_domain(e.more)):
            return None
        return (e.more, e.place, e.price)

    for evs in find_duplicates(
        ok_events,
        _mk_key_madrid_music
    ):
        for e in evs:
            ok_events.remove(e)
        more = evs[0].more
        _id_ = MadridEs.get_id(more)
        e = Event.fusion(*evs, firstEventUrl=False).merge(
            name=None,
            id=_id_,
            url=more,
        ).fix(publish=PUBLISH.get(_id_))
        ok_events.add(e)

    def _mk_key_cycle(e: Event | Cinema):
        if len(e.sessions) == 1 and e.cycle:
            return (e.cycle, e.category, e.place, e.price)

    for evs in find_duplicates(
        ok_events,
        _mk_key_cycle
    ):
        for e in evs:
            ok_events.remove(e)
        cycle = evs[0].cycle
        _id_ = to_uuid("".join(e.id for e in evs))
        e = Event.fusion(*evs, firstEventUrl=True).merge(
            name=cycle,
            cycle=cycle,
            id=_id_,
            url=None,
            more=None
        ).fix(publish=PUBLISH.get(_id_))
        ok_events.add(e)

    def _mk_place_name_session(e: Event | Category):
        return (e.place, e.category, e.name, e.price, tuple((s.date for s in e.sessions)))

    for evs in find_duplicates(
        ok_events,
        _mk_place_name_session
    ):
        for e in evs:
            ok_events.remove(e)
        _id_ = to_uuid("".join(e.id for e in evs))
        e = Event.fusion(*evs).merge(
            id=_id_,
        ).fix(publish=PUBLISH.get(_id_))
        ok_events.add(e)

    arr1 = sorted(
        (e.fix_type().fix() for e in ok_events),
        key=lambda e: (min(s.date for s in e.sessions), e.name, e.url or '')
    )
    imdb: set[str] = set()
    for e in arr1:
        if isinstance(e, Cinema) and e.imdb and e.filmaffinity is None:
            imdb.add(e.imdb)
    imdb_film = WIKI.get_filmaffinity(*imdb)
    for i, e in enumerate(arr1):
        filmaffinity = find_filmaffinity_if_needed(imdb_film, e)
        if filmaffinity:
            logger.debug(f"FIND FilmAffinity: {filmaffinity}")
            arr1[i] = e.merge(filmaffinity=filmaffinity).fix()

    return tuple(arr1)


logger.info("Recuperar eventos")
eventos = \
    MadridEs().get_safe_events() + \
    Dore().events + \
    MadridDestino().events + \
    CasaEncendida().events + \
    SalaBerlanga().events + \
    SalaEquis().events + \
    CasaAmerica().events + \
    AcademiaCine().events + \
    CaixaForum().events + \
    Telefonica().events
logger.info(f"{len(eventos)} recuperados")

eventos = tuple(filter(myfilter, eventos))
eventos = sorted_and_fix(eventos)

logger.info(f"{len(eventos)} filtrados")


sesiones: Dict[str, Set[int]] = {}
sin_sesiones: Set[int] = set()
categorias: Dict[Category, int] = {}
lugares: Dict[str, int] = {}

for e in eventos:
    categorias[e.category] = categorias.get(e.category, 0) + 1
    lugares[e.place.get_alias()] = lugares.get(e.place.get_alias(), 0) + 1
    if len(e.sessions) == 0:
        sin_sesiones.add(e.id)
        continue
    for f in e.sessions:
        f = f.date.split()[0]
        dict_add(sesiones, f, e.id)


def event_to_ics(e: Event, s: Session):
    price = str(int(e.price)) if int(e.price) == e.price else f"{e.price:.2f}"
    description = (f'{price} €\n\n' + "\n\n".join(
        uniq(e.url, *e.also_in, s.url, e.more)
    )).strip()
    dtstart = to_datetime(s.date)
    dtend = dtstart + timedelta(minutes=(e.duration or 120))
    return IcsEvent(
        uid=f"{e.id}_{s.id}",
        dtstamp=NOW,
        url=(s.url or e.url),
        categories=str(e.category),
        summary=e.title,
        description=description,
        location=e.place.address,
        organizer=e.place.name,
        dtstart=dtstart,
        dtend=dtend
    )


logger.info("Añadiendo ics")
session_ics: Dict[str, str] = dict()
icsevents = []
for e in eventos:
    for s in e.sessions:
        ics = event_to_ics(e, s)
        uid = ics.uid.lower()
        session_ics[e.id+s.id] = uid
        ics.dumpme(f"out/cal/{uid}.ics")
        icsevents.append(ics)
IcsEvent.dump("out/eventos.ics", *icsevents)

logger.info("Añadiendo imágenes")
img_eventos = tuple(map(add_image, eventos))

logger.info("Creando web")


def set_icons(html: str, **kwargs):
    a: bs4.Tag
    soup = bs4.BeautifulSoup(html, 'html.parser')
    for a in soup.find_all("a", string=re.compile(r"\s*🔗\s*")):
        txt = a.get_text().strip()
        href = a.attrs["href"]
        dom = get_domain(href)
        dom = dom.rsplit(".", 1)[0]
        ico = {
            "autocines": "https://autocines.com/wp-content/uploads/2021/01/cropped-favicon-32x32-1-32x32.png",
            "filmaffinity": "https://www.filmaffinity.com/favicon.png",
            "atrapalo": "https://www.atrapalo.com/favicon.ico",
            "google": "https://www.google.es/favicon.ico",
            "cinesa": "https://www.cinesa.es/scripts/dist/favicon/es/favicon.ico",
            "yelmocines": "https://eu-static.yelmocines.es/img/favicon.ico",
            "lavaguadacines": "https://lavaguadacines.es/assets/images/favicon.jpg",
            "madrid": "https://www.madrid.es/favicon.ico",
            "21distritos": "https://21distritos.es/CD_Favicon_generico.jpg",
            "centrodanzamatadero": "https://www.centrodanzamatadero.es/themes/custom/centro_danza/favicon.ico",
            "salaberlanga": "https://salaberlanga.com/wp-content/uploads/2023/09/cropped-cropped-favicon-berlanga-bn-300x300-1-32x32.png",
            "cinetecamadrid": "https://www.cinetecamadrid.com/themes/custom/cineteca_theme/favicon.ico",
            "imdb": "https://m.media-amazon.com/images/G/01/imdb/images-ANDW73HA/favicon_desktop_32x32._CB1582158068_.png",
            "teatroreal": "https://www.teatroreal.es/themes/custom/teatro_real/favicon.ico",
            "semanacienciamadrid": "https://www.semanacienciamadrid.org/themes/custom/bs5fmmd/favicon.ico",
            "condeduquemadrid": "https://www.condeduquemadrid.es/themes/custom/condebase_theme/icon_app/favicon-16x16.png",
            "docs.google": "https://ssl.gstatic.com/docs/spreadsheets/forms/favicon_qp2.png",
            "forms.office": "https://cdn.forms.office.net/images/favicon.ico"
        }.get(dom)
        if ico is None:
            continue
        a.string = ""
        a.append(toTag(f'<img src="{ico}" class="ico" alt="{txt}"/>'))
        tit = {
            "filmaffinity": "Ver en Filmaffinity",
            "atrapalo": "Buscar en Atrapalo",
            "google": "Buscar en Google",
            "21distritos": "Ver en 21distritos.es",
        }.get(dom)
        if tit and not a.attrs.get("title"):
            a.attrs["title"] = tit
    return str(soup)


PBLSH = sorted(set((e.publish for e in eventos if e.publish)), reverse=True)
NEWS = PBLSH[0 if len(PBLSH) < 3 else 1]

CLSS = defaultdict(list)
CLSS_COUNT = defaultdict(int)
for e in eventos:
    if NEWS <= e.publish:
        CLSS[e.id].append("novedad")
for arr in CLSS.values():
    for a in arr:
        CLSS_COUNT[a] = CLSS_COUNT[a] + 1


j = Jnj2("template/", OUT, favicon="🗓", post=set_icons)
j.create_script(
    "rec/info.js",
    SESIONES=sesiones,
    SIN_SESIONES=sin_sesiones,
    replace=True,
)
j.save(
    "index.html",
    now=NOW,
    eventos=img_eventos,
    clss=CLSS,
    clss_count=CLSS_COUNT,
    categorias=categorias,
    session_ics=session_ics,
    lugares=lugares,
    count=len(eventos),
    precio=max(e.price for e in eventos),
    fecha=dict(
        ini=min(sesiones.keys()),
        fin=max(sesiones.keys())
    )
)
logger.info("Creando rss")
EventosRss(
    destino=OUT,
    root=PAGE_URL,
    eventos=eventos
).save("eventos.rss")

FM.dump(OUT+"eventos.json", eventos, compact=True)
FM.dump(OUT+"publish.json", PUBLISH)
logger.info("Fin")
