#!/usr/bin/env python3

from core.event import Event, Category, Session
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
from core.j2 import Jnj2, toTag
from datetime import datetime, timedelta
from core.log import config_log
from core.img import MyImage
from core.util import dict_add, get_domain, to_datetime
import logging
from os import environ
from os.path import isfile
from typing import Dict, Set, Tuple, List, Union
from core.filemanager import FM
import math
import bs4
import re
from textwrap import dedent
import uuid
import pytz
from core.rss import EventosRss

import argparse

parser = argparse.ArgumentParser(description='Lista eventos')
parser.add_argument('--precio', type=int, help="Precio máximo", default=5)

args = parser.parse_args()
PAGE_URL = environ['PAGE_URL']
OUT = "out/"

config_log("log/build_site.log")
logger = logging.getLogger(__name__)
now = datetime.now(tz=pytz.timezone('Europe/Madrid'))
white = (255, 255, 255)

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


def myfilter(e: Event):
    if e.price > args.precio:
        return False
    if e.category not in (Category.CINEMA, Category.MUSIC, Category.THEATER, Category.DANCE):
        return False
    if e.place.name in ('Espacio Abierto Quinta de los Molinos', 'Faro de Moncloa', 'Centro Cultural Las Californias', 'Plaza de Daoíz y Velarde'):
        return False
    e.remove_old_sessions(now)
    if len(e.sessions) == 0:
        return False
    return True


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
    MadridEs().events
logger.info(f"{len(eventos)} recuperados")
eventos = tuple(filter(myfilter, eventos))
logger.info(f"{len(eventos)} filtrados")


def mysorted(eventos: List[Event]):
    arr1 = sorted(
        eventos,
        key=lambda e: (min(s.date for s in e.sessions), e.name, e.url)
    )
    return tuple(arr1)


eventos = mysorted(eventos)

sesiones: Dict[str, Set[int]] = {}
sin_sesiones: Set[int] = set()
categorias = {}
lugares = {}

for e in eventos:
    categorias[e.category] = categorias.get(e.category, 0) + 1
    lugares[e.place.name] = lugares.get(e.place.name, 0) + 1
    if len(e.sessions) == 0:
        sin_sesiones.add(e.id)
        continue
    for f in e.sessions:
        f = f.date.split()[0]
        dict_add(sesiones, f, e.id)


def event_to_ics(e: Event, s: Session):
    description = "\n".join(filter(lambda x: x is not None, [
        f'{e.price}€',
        e.url,
        s.url,
        e.more if (e.more and not e.more.startswith("https://www.google.es/search")) else None
    ])).strip()
    dtstart = to_datetime(s.date)
    dtend = dtstart + timedelta(minutes=e.duration)
    return IcsEvent(
        uid=f"{e.id}_{s.id}",
        dtstamp=now,
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
icsevents = []
for e in eventos:
    for s in e.sessions:
        ics = event_to_ics(e, s)
        ics.dumpme(f"out/cal/{e.id}_{s.id}.ics")
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
            "lavaguadacines": "https://lavaguadacines.es/assets/images/favicon.jpg"
        }.get(dom)
        if ico is None:
            continue
        a.string = ""
        a.append(toTag(f'<img src="{ico}" class="ico" alt="{txt}"/>'))
        tit = {
            "filmaffinity": "Ver en Filmaffinity",
            "atrapalo": "Buscar en Atrapalo",
            "google": "Buscar en Google",
        }.get(dom)
        if tit and not a.attrs.get("title"):
            a.attrs["title"] = tit
    return str(soup)


j = Jnj2("template/", OUT, favicon="🗓", post=set_icons)
j.create_script(
    "rec/info.js",
    SESIONES=sesiones,
    SIN_SESIONES=sin_sesiones,
    replace=True,
)
j.save(
    "index.html",
    eventos=img_eventos,
    now=now,
    categorias=categorias,
    lugares=lugares,
    count=len(eventos),
    precio=max(e.price for e in eventos),
    fecha=dict(
        ini=min(sesiones.keys()),
        fin=max(sesiones.keys())
    )
)
logger.info(f"Creando rss")
EventosRss(
    destino=OUT,
    root=PAGE_URL,
    eventos=eventos
).save("eventos.rss")

FM.dump(OUT+"eventos.json", eventos)
logger.info("Fin")
