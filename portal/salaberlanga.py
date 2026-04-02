from core.web import Driver, get_text, buildSoup, get_query
from functools import cached_property
from core.event import Cinema, Event, Category, Session, FIX_EVENT
from core.place import Places
from portal.cineentradas import CineEntradas
from bs4 import Tag
import re
from core.cache import TupleCache
import json
from typing import NamedTuple
from collections import defaultdict
from core.util import re_or, MONTH, re_and
from datetime import date
import logging

logger = logging.getLogger(__name__)

url = "https://salaberlanga.com/wp-json/wp/v2/actividad/?per_page=1"

re_sp = re.compile(r"\s+")
TODAY = date.today()


def get_attr(n: Tag, attr: str):
    if n is None:
        return None
    href = n.attrs.get(attr)
    if not isinstance(href, str):
        return None
    href = href.strip()
    if re.match(r"https?://.*", href, flags=re.I):
        return href


class Item(NamedTuple):
    url: str
    tag: Tag
    inf: dict


class SalaBerlanga:
    PRICE = 4.40
    CINE_ENTRADAS = 2369
    HOME = "https://salaberlanga.com/programacion-de-actividades/"
    ACTIVIDADES = "https://salaberlanga.com/wp-json/wp/v2/actividad/?per_page=100"

    def __init__(self):
        self.__cine_entradas = CineEntradas(
            SalaBerlanga.CINE_ENTRADAS,
            price=SalaBerlanga.PRICE
        ).events

    @cached_property
    def items(self):
        urls: dict[str, set[str]] = defaultdict(set)
        with Driver(browser="firefox", wait=15) as f:
            f.get(SalaBerlanga.HOME)
            f.wait_ready()
            f.click("check-sin-entradas")
            f.wait_ready()
            while True:
                count = len(urls)
                soup = f.get_soup()
                slc = "h5.card-title a"
                for a in soup.select(slc):
                    href = a.attrs.get("href")
                    if href:
                        node = a.parent
                        while node and node.parent and len(node.parent.select(slc)) == 1:
                            node = node.parent
                        urls[href].add(str(node) if node else None)
                if count == len(urls):
                    break
                f.safe_click("mas-actividades-portada", "mas-actividades")
                f.wait_ready()
            f.get(SalaBerlanga.ACTIVIDADES)
            f.wait_ready()
            f.click("rawdata-tab")
            f.wait_ready()
            actividades = get_text(f.get_soup().select_one("pre"))
        act = json.loads(actividades)
        items: list[Item] = []
        for url, tags in urls.items():
            tags = tuple(t for t in tags if t is not None)
            if len(tags) == 0:
                raise ValueError(f"No se encuentra casilla para {url} en {SalaBerlanga.HOME}")
            tag: str = '<div>' + "\n".join(tags) + '</div>'
            inf = self.__get_ficha(act, url)
            if inf is None:
                continue
            items.append(Item(
                url=url,
                tag=buildSoup(SalaBerlanga.HOME, tag),
                inf=inf
            ))
        #FM.dump("rec/salaberlanga/fichas.json", [i.inf for i in items])
        return tuple(items)

    def __get_ficha(self, act: dict, url: str):
        for a in act:
            if a['link'] == url:
                return a
        logger.warning(f"{url} not found in {SalaBerlanga.ACTIVIDADES}")

    def __get_cine_entrada(self, url: str, name: str):
        def _showGroups(u: str):
            sg = (get_query(u).get("showGroups") or "").strip()
            if len(sg) == 0:
                return None
            return sg.lower()

        ok_name: set[Event] = set()
        ok_showGroups: set[Event] = set()
        like_showGroups: set[Event] = set()
        showGroups = _showGroups(url)
        for e in self.__cine_entradas:
            if e.name == name:
                ok_name.add(e)
            if e.url == url:
                return e
            sg = _showGroups(e.url)
            if showGroups and sg:
                if showGroups == sg:
                    ok_showGroups.add(e)
                if (showGroups in sg) or (sg in showGroups):
                    like_showGroups.add(e)
            for s in e.sessions:
                if s.url == url:
                    return e
                sg = _showGroups(s.url)
                if showGroups and sg:
                    if showGroups == sg:
                        ok_showGroups.add(e)
                    if (showGroups in sg) or (sg in showGroups):
                        like_showGroups.add(e)
        if len(ok_name) == 1:
            return ok_name.pop()
        if len(ok_showGroups) == 1:
            return ok_showGroups.pop()
        if len(like_showGroups) == 1:
            return like_showGroups.pop()

    def _to_event(self, item: Item):
        if get_text(item.tag.select_one("p.card-text-dispo")) == "Entradas agotadas":
            return None
        a = item.tag.select_one("p.card-text-comprar a")
        isGratis = get_text(a) == "Entrada gratuita"
        url_compra = get_attr(a, "href")
        if url_compra in (
            'https://cine.entradas.com/cine/madrid/sala-berlanga/sesiones?ref=770&showAllDates=true',
        ):
            url_compra = None
        card_text = re.match(
            r"^(.*?)\s+\|\s+(\d+)\s+\|\s+(\d+)[´']$",
            get_text(item.tag.select_one("p.card-text-time")) or ''
        )
        _id_ = "sb"+str(item.inf['id'])
        html_sessions: set[Session] = set()
        dates = "\n".join(map(get_text, item.tag.select("p.card-text-date")))
        for d, m, hm in re.findall(r"(\d+) de (" + "|".join(MONTH) + r")\w+ - (\d+:\d+)", dates, flags=re.I):
            d = int(d)
            m = MONTH.index(m.lower()) + 1
            dt = date(TODAY.year, m, d)
            if dt.month < TODAY.month:
                dt.replace(year=dt.year+1)
            html_sessions.add(Session(date=f"{dt.isoformat()} {hm}"))
        html_name = get_text(item.tag.select_one("h5"))
        tup_html_sessions = tuple(sorted(html_sessions, key=lambda s: (s.date, s.url)))
        cine_entradas = self.__get_cine_entrada(url_compra, html_name)
        if cine_entradas:
            old_url = cine_entradas.url
            sessions: set[Session] = set()
            for s in cine_entradas.sessions:
                if s.url is None:
                    s = s.merge(url=old_url)
                sessions.add(s)
            ev = cine_entradas.merge(
                id=_id_,
                url=item.url,
                sessions=tuple(sorted(sessions, key=lambda s: (s.date, s.url)))
            )
        else:
            if url_compra and len(tup_html_sessions) == 1 and tup_html_sessions[0].url is None:
                tup_html_sessions = (tup_html_sessions[0]._replace(url=url_compra), )
            ev = Event(
                id=_id_,
                url=item.url,
                name=html_name,
                category=Category.UNKNOWN,
                place=Places.SALA_BERLANGA.value,
                price=SalaBerlanga.PRICE,
                duration=int(card_text.group(3)) if card_text else None,
                sessions=tup_html_sessions,
                img=None, #item.inf.get('yoast_head_json', {}).get('og_image', [{}])[0].get('url'),
            )
        if isGratis:
            ev = ev.merge(price=0)
        ev = self.__complete(ev, item)
        ev = ev.fix_type()
        if isinstance(ev, Cinema):
            content = buildSoup(item.inf['link'], item.inf['content']['rendered'])
            aka = self.__find_p_strong(content, "Título original")
            if aka:
                ev = ev.merge(aka=(ev.name, aka))
            if card_text:
                ev = ev.merge(
                    director=tuple(d for d in map(str.strip, re.split(r",| y ", card_text.group(1))) if d),
                    year=int(card_text.group(2))
                )
        return ev

    def __complete(self, ev: Event, item: Item):
        if ev.img is None:
            ev = ev.merge(img=get_attr(item.tag.select_one("img"), "src"))
        category = self.__find_category(ev, item)
        if category is not None:
            ev = ev.merge(category=category)
        if ev.category == Category.DANCE:
            return ev.merge(
                cycle="Bailar en la Berlanga",
            )
        if ev.category == Category.THEATER:
            return ev.merge(
                cycle="Teatro en la Berlanga",
            )
        if ev.cycle is None and ev.img:
            cycle = {
                "https://salaberlanga.com/wp-content/uploads/2026/03/Redes_Feed_NT-Bergia2-240x300.jpg": "Nuevos territorios",
                "https://salaberlanga.com/wp-content/uploads/2026/03/0.-Cartel-Ciclo-C54-211x300.png": "Cinco cuartos",
            }.get(ev.img)
            if cycle is None and re.search(r"[\-_]nuevos[\-_]territorios[\-_]", ev.img, flags=re.I):
                cycle = "Nuevos territorios"
            if cycle:
                return ev.merge(cycle=cycle)
        if ev.category == Category.CINEMA and re_or(
            ev.name,
            ("LA MIRADA TAB[ÚU]", "EDICI[OÓ]N"),
            flags=re.I
        ):
            return ev.merge(
                cycle="La mirada tabú: Cortometrajes"
            )
        return ev

    def __find_category(self, ev: Event, item: Item):
        cat = get_text(item.tag.select_one("div.categoria-sala-berlanga")) or ''
        if re_and(
            ev.name,
            "Charlas?",
            "sesi[oó]n(es)? de firmas?",
            flags=re.I
        ):
            return Category.CONFERENCE
        if re_and(
            ev.name,
            "Presentación del libro",
            flags=re.I
        ):
            return Category.LITERATURE
        if re_or(cat, "cine", flags=re.I):
            return Category.CINEMA
        if re_or(cat, "M[uú]sica", flags=re.I):
            return Category.MUSIC
        if re_or(cat, "Artes? esc[eé]nicass?", flags=re.I):
            if re.search(r"[\-_]Bailar[\-_]", ev.img or '', flags=re.I):
                return Category.DANCE
            return Category.THEATER

    def __find_p_strong(self, soup: Tag, txt: str):
        line = r"\s+".join(map(re.escape, txt.split()))
        regex = re.compile(r"^\s*"+line+r"\s*:?\s*$", flags=re.I)
        strong = soup.find("strong", string=regex)
        if strong is None:
            return None
        p = strong.find_parent("p")
        if p is None:
            return None
        s_txt = get_text(strong)
        p_txt = get_text(p)
        if None in (s_txt, p_txt):
            return None
        if not p_txt.startswith(s_txt):
            return None
        return p_txt[len(s_txt)+1:].strip()

    @property
    @TupleCache("rec/salaberlanga.json", builder=Event.build)
    def events(self):
        logger.info("Sala Berlanga: Buscando eventos")
        events: set[Event] = set()
        for item in self.items:
            ev = self._to_event(item)
            if ev:
                events.add(ev)
        logger.info(f"Sala Berlanga: Buscando eventos = {len(events)}")
        return tuple(sorted(events))


if __name__ == "__main__":
    s = SalaBerlanga()
    list(s.events or [])
