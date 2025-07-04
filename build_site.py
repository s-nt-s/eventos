#!/usr/bin/env python3

from core.event import Event, Category, Session, Place
from core.ics import IcsEvent
from core.casaencendida import CasaEncendida
from core.dore import Dore
from core.madriddestino import MadridDestino
from core.cineentradas import CineEntradas
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
from core.util import dict_add, get_domain, to_datetime, uniq
import logging
from os import environ
from os.path import isfile
from typing import Dict, Set, Tuple, List
from core.filemanager import FM
import math
import bs4
import re
import pytz
from core.rss import EventosRss
from collections import defaultdict

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
    ):
        return False
    if e.price > args.precio:
        return False
    if e.category not in OK_CAT:
        return False

    e.remove_old_sessions(NOW)
    e.remove_working_sessions()

    if len(e.sessions) == 0:
        return False
    return True


def isMadridMusic(e: Event):
    if get_domain(e.url) != "madrid.es" or e.category not in (Category.MUSIC, ):
        return False
    if get_domain(e.more) != "madrid.es":
        return False
    return True


def sorted_and_fix(eventos: List[Event]):
    def _iter_fix(eventos: List[Event]):
        done: set[Event] = set()
        for e in eventos:
            e = e.fix(publish=PUBLISH.get(e.id))
            if e not in done:
                done.add(e)
                if myfilter(e):
                    PUBLISH[e.id] = e.publish
                    yield e
    ok_events: Set[Event] = set()
    data: Dict[Tuple[str, Place]] = defaultdict(set)
    for e in _iter_fix(eventos):
        if not isMadridMusic(e):
            ok_events.add(e)
            continue
        data[(e.more, e.place)].add(e)

    for (more, place), evs in data.items():
        if len(evs) == 1:
            ok_events = ok_events.union(evs)
            continue
        _id_ = MadridEs.get_id(more)
        e = Event.fusion(*evs).merge(
            name=None,
            id=_id_,
            url=more,
        ).fix(publish=PUBLISH.get(_id_))
        ok_events.add(e)
    arr1 = sorted(
        ok_events,
        key=lambda e: (min(s.date for s in e.sessions), e.name, e.url)
    )
    return tuple(arr1)


logger.info("Recuperar eventos")
eventos = \
    MadridDestino().events + \
    Dore().events + \
    CasaEncendida().events + \
    CineEntradas(CineEntradas.SALA_BERLANGA, price=4.40).events + \
    SalaEquis().events + \
    CasaAmerica().events + \
    AcademiaCine().events + \
    CaixaForum().events + \
    MadridEs().events + \
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
    dtend = dtstart + timedelta(minutes=e.duration)
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
    for a in soup.findAll("a", string=re.compile(r"\s*🔗\s*")):
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
            "teatroreal": "https://www.teatroreal.es/themes/custom/teatro_real/favicon.ico"
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

FM.dump(OUT+"eventos.json", eventos)
FM.dump(OUT+"publish.json", PUBLISH)
logger.info("Fin")
