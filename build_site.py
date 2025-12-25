#!/usr/bin/env python3

from core.event import Event, Category, Session
from core.ics import SimpleIcsEvent
from core.j2 import Jnj2, toTag
from datetime import datetime, timedelta
from core.log import config_log
from core.img import MyImage
from core.util import dict_add, get_domain, to_datetime, uniq
import logging
from os import environ
from os.path import isfile
from typing import Tuple, Dict, Set
from core.filemanager import FM
import math
import bs4
import re
import pytz
from core.rss import EventosRss
from collections import defaultdict
from portal.event_collector import EventCollector


config_log("log/build_site.log")
logger = logging.getLogger(__name__)

PAGE_URL = environ['PAGE_URL']
OUT = "out/"
WHITE = (255, 255, 255)

EC = EventCollector(
    max_price={
        Category.CINEMA: 5,
        Category.OTHERS: 10,
    },
    max_sessions=30,
    avoid_working_sessions=True,
    publish=FM.load(OUT+"publish.json"),
    ko_places=(
        "Espacio Abierto Quinta de los Molinos",
        "Parroquia Nuestra Señora de Guadalupe",
    ),
    categories=(
        Category.CINEMA,
        Category.MUSIC,
        Category.THEATER,
        Category.DANCE,
        Category.CONFERENCE,
        Category.VISIT,
        Category.MAGIC,
        Category.UNKNOWN,
        Category.LITERATURE,
        Category.WORKSHOP,
        Category.PARTY,
        Category.READING_CLUB
    )
)


def distance_to_white(*color) -> Tuple[int]:
    arr = []
    for c in color:
        d = math.sqrt(sum([(c1 - c2) ** 2 for c1, c2 in zip(c, WHITE)]))
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


eventos = EC.get_events()

null_zone = "Otra"
sesiones: Dict[str, Set[int]] = {}
sin_sesiones: Set[int] = set()
categorias: Dict[Category, int] = {}
zones: Dict[str, int] = {}
places: Dict[str, int] = {}

for e in eventos:
    categorias[e.category] = categorias.get(e.category, 0) + 1
    zones[e.place.zone or null_zone] = zones.get(e.place.zone or null_zone, 0) + 1
    places[e.place.name] = places.get(e.place.name, 0) + 1
    if len(e.sessions) == 0:
        sin_sesiones.add(e.id)
        continue
    for f in e.sessions:
        f = f.date.split()[0]
        dict_add(sesiones, f, e.id)

zones = dict(sorted(zones.items(), key=lambda kv: (int(kv[0] == null_zone), kv)))


def event_to_ics(now: datetime, e: Event, s: Session):
    price = str(int(e.price)) if int(e.price) == e.price else f"{e.price:.2f}"
    description = (f'{price} €\n\n' + "\n\n".join(
        uniq(e.url, *e.also_in, s.url, e.more)
    )).strip()
    dtstart = to_datetime(s.date)
    dtend = dtstart + timedelta(minutes=(e.duration or 120))
    return SimpleIcsEvent(
        uid=f"{e.id}_{s.id}",
        dtstamp=now,
        url=(s.url or e.url),
        categories=str(e.category),
        summary=s.title or e.title,
        description=description,
        location=e.place.address,
        organizer=e.place.name,
        dtstart=dtstart,
        dtend=dtend
    )


NOW = datetime.now(tz=pytz.timezone('Europe/Madrid'))
logger.info("Añadiendo ics")
session_ics: Dict[str, str] = dict()
icsevents = []
for e in eventos:
    for s in e.sessions:
        ics = event_to_ics(NOW, e, s)
        uid = ics.uid.lower()
        session_ics[e.id+s.id] = uid
        ics.dumpme(f"out/cal/{uid}.ics")
        icsevents.append(ics)
SimpleIcsEvent.dump("out/eventos.ics", *icsevents)

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
            "forms.office": "https://cdn.forms.office.net/images/favicon.ico",
            "goodreads": "https://www.goodreads.com/favicon.ico",
            "teatroespanol": "https://www.teatroespanol.es/themes/custom/teatroespanol_v2/favicon.ico",
            "es.wikipedia": "https://es.wikipedia.org/static/favicon/wikipedia.ico",
            "mataderomadrid": "https://www.mataderomadrid.org/themes/custom/new_matadero/favicon.ico",
            "centrocentro": "https://www.centrocentro.org/sites/default/files/favicon_1.ico"
        }.get(dom)
        if ico is None:
            continue
        cls = dom.replace(".", "_")
        a.string = ""
        a.append(toTag(f'<img src="{ico}" class="ico {cls}" alt="{txt}"/>'))
        tit = {
            "filmaffinity": "Ver en Filmaffinity",
            "atrapalo": "Buscar en Atrapalo",
            "google": "Buscar en Google",
            "21distritos": "Ver en 21distritos.es",
            "goodreads": "Ver en Goodreads",
            "wikipedia": "Ver en Wikipedia"
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
    places=places,
    zones=zones,
    null_zone=null_zone,
    count=len(eventos),
    precio=round(max(e.price for e in eventos)),
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
FM.dump(OUT+"publish.json", EC.publish)
logger.info("Fin")
