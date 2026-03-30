from core.web import get_text, WebException, MyTag, Web
from core.cache import TupleCache, HashCache
from typing import Set, Dict, Union, List, Tuple
import logging
from core.event import Event, Session, Category, FieldNotFound, CategoryUnknown
from core.place import Places
import re
from bs4 import Tag
from datetime import datetime
from core.util import plain_text, re_or, find_duplicates, get_main_value, round_to_even
from functools import cached_property
from core.md import MD

logger = logging.getLogger(__name__)
NOW = datetime.now()


class MyIdTag(MyTag):
    def __init__(self, id: int, url: str, node: Tag):
        super().__init__(url, node)
        self.id = id


class CaixaForum:
    TIT_SELECTOR = "a > h2, a > .activity-title, a > h3"
    URLS = (
        "https://caixaforum.org/es/madrid/actividades?p=999",
        #"https://caixaforum.org/es/madrid/actividades?p=999&c=874798",
        #"https://caixaforum.org/es/madrid/actividades?p=999&c=874804",
        #"https://caixaforum.org/es/madrid/actividades?p=999&c=314579",
        #"https://caixaforum.org/es/madrid/actividades?p=999&c=161124943"
    )

    def __init__(self):
        self.__w = Web()
        self.__w.s.headers.update({'Accept-Encoding': 'gzip, deflate'})

    @cached_property
    def __category(self):
        done: Set[int] = set()
        category: Dict[Tuple[int, ...], Category] = {}
        for cat, url in {
            Category.CHILDISH: "https://caixaforum.org/es/madrid/familia?p=999",
        }.items():
            ids = set(d.id for d in self.__get_div_events(url))
            category[tuple(sorted(ids.difference(done)))] = cat
            done = done.union(ids)
        return category

    @HashCache("rec/caixaforum/{}_ld.json")
    def __get_ld_json(self, url: str) -> Dict:
        return self.get_soup(url).select_one_json(
            'script[type="application/ld+json"]',
            if_none="silent"
        )

    def get_soup(self, url: str):
        soup = self.__w.get(url)
        return MyTag(url, soup)

    @HashCache("rec/caixaforum/{}_js.json")
    def get_json(self, url: str) -> Union[Dict, List]:
        node = self.get_soup(url)
        return node.select_one_json("body", none=("Desinstalado", ))

    def get_ld_json(self, url: str) -> Dict:
        js = self.__get_ld_json(url)
        if js is None:
            return {}
        for k in ("startDate", "endDate"):
            v = js.get(k)
            if v:
                js[k] = datetime.fromisoformat(v)
        return js

    @property
    @TupleCache("rec/caixaforum.json", builder=Event.build)
    def events(self):
        logger.info("Caixa Forum: Buscando eventos")
        events: Set[Event] = set()
        for url in CaixaForum.URLS:
            divs = self.__get_div_events(url)
            for div in divs:
                ev = self.__div_to_event(div)
                if ev:
                    events.add(ev)

        def _mk_key_cycle(e: Event):
            if not e.cycle:
                return None
            if e.category == Category.CINEMA:
                return None
            return (e.cycle, e.category, e.place, round_to_even(e.price))

        for evs in find_duplicates(
            events,
            _mk_key_cycle
        ):
            for e in evs:
                events.remove(e)
            url = get_main_value(list(e.more for e in evs if e.more))
            e = Event.fusion(
                *evs,
                name=evs[0].cycle,
                url=url,
                also_in=tuple() if url else None
            )
            events.add(e)
        logger.info(f"Caixa Forum: Buscando eventos {len(events)}")
        return tuple(sorted(events))

    def __get_div_events(self, url: str) -> Tuple[MyIdTag, ...]:
        events: List[MyIdTag] = []
        soup = self.get_soup(url).node
        warn = get_text(soup.select_one("div.portlet-body div.title-warrings h2"))
        if warn is not None:
            logger.warning(warn+" "+url)
            return tuple()
        slc = f"div.card-item:has({CaixaForum.TIT_SELECTOR})"
        divs = soup.select(slc)
        if len(divs) == 0:
            raise WebException(f"{slc} NOT FOUND in {url}")
        for div in divs:
            h2 = div.select_one(CaixaForum.TIT_SELECTOR)
            url = h2.find_parent("a").attrs["href"]
            eid = self.__get_id_from_url(url)
            if eid is None:
                logger.warning(f"ID not found in {url}")
                continue
            events.append(MyIdTag(id=eid, url=url, node=div))
        logger.debug(f"{len(events)} {slc}")
        return tuple(events)

    def __get_id_from_url(self, url: str):
        m = re.match(r".*_a(\d+)$", url)
        if m:
            return int(m.group(1))
        soup = self.get_soup(url)
        ids: set[int] = set()
        for script in map(get_text, soup.select("script")):
            for m in re.findall(r"loadOneBoxTicketsDetailActivity\s*\(\s*['\"](\d+)['\"]", script or ''):
                ids.add(int(m))
        if len(ids):
            return ids.pop()

    def __div_to_event(self, div: MyIdTag):
        h2 = div.select_one(CaixaForum.TIT_SELECTOR)
        url = h2.find_parent("a").attrs["href"]
        info = self.get_ld_json(url)
        ficha = self.__get_ficha(div.id)
        sessions = self.__find_session(url, ficha, info)
        if len(sessions) == 0:
            return None
        event_soup = self.get_soup(url)
        price = self.__find_price(div.id, event_soup)
        if price < 0:
            return None
        category = self.__find_category(div, event_soup)
        name = get_text(h2)
        more, cycle = self.__find_more_cycle(name, url, category)
        ev = Event(
            id=f"cf{div.id}",
            url=url,
            name=name,
            img=self.__find_img(event_soup),
            price=price,
            category=category,
            duration=self.__find_duration(
                url,
                event_soup.select_one("#description-read"),
                category,
                info
            ),
            sessions=sessions,
            place=Places.CAIXA_FORUM.value,
            more=more,
            cycle=cycle
        )
        return ev

    def __find_more_cycle(self, name: str, url: str, category: Category):
        if category == Category.CINEMA:
            return None, None
        if re_or(
            name,
            r"Vermut y tertulia",
            flags=re.I
        ):
            return None, "Vermut y tertulia"
        if re_or(
            name,
            r"Encuentros con\s*(\.\.\.|…)",
            flags=re.I
        ):
            return None, "Encuentros con…"
        soup = self.get_soup(url)
        a = soup.node.select_one("div.filters-form-container li:last-child a")
        if a is None:
            return None, None
        txt = get_text(a)
        cycle = re.match(r"^Ciclo: (.+)$", txt or "", flags=re.I)
        if not cycle:
            return None, None
        return a.attrs["href"], cycle.group(1)

    def __find_img(self, div: MyTag):
        try:
            nmg = div.select_one('figure img, div.card-detail img')
        except WebException as e:
            logger.warning("Imagen no encontrada: " + str(e))
            return None
        img = nmg.attrs.get("data-src") or nmg.attrs.get("src")
        if img is None:
            logger.warning("Imagen sin data-src o src: " + str(nmg))
            return None
        img = re.sub(r"(\.jpe?g)/.*$", r"\1", img)
        return img

    def __get_ficha(self, eid: int) -> Dict | None:
        for url in (
            "https://caixaforum.org/es/web/madrid/fichaactividad?p_p_id=FlcHeaderSearchPortlet&p_p_lifecycle=2&p_p_state=normal&p_p_mode=view&p_p_resource_id=%2Fsearch%2FoneBox&p_p_cacheability=cacheLevelPage&_FlcHeaderSearchPortlet_cpDefinitionId="+str(eid),
            "https://caixaforum.org/es/web/madrid/actividades?p_p_id=headersearch_INSTANCE_HeaderSearch&p_p_lifecycle=2&p_p_resource_id=%2Fsearch%2FoneBox&_headersearch_INSTANCE_HeaderSearch_cpDefinitionId="+str(eid),
        ):
            js = self.get_json(url)
            if js is not None:
                return js
        return None

    def __find_session(self, event_url: str, ficha: Dict | None, info: Dict):
        sessions: Set[Session] = set()
        ss: List[Dict] = (ficha or {}).get('sessions', [])
        for s in ss:
            if s.get('availableCapacity') == 0:
                continue
            h, mm = map(int, s["date"].split(":"))
            d, m, y = map(int, s["time"].split("/"))
            sessions.add(Session(
                url=s['url'],
                date=f"{y}-{m:02d}-{d:02d} {h:02d}:{mm:02d}"
            ))
        if sessions:
            return tuple(sorted(sessions))
        st: datetime = info.get("startDate")
        nd: datetime = info.get("endDate")
        if None in (st, nd) or st.date() != nd.date():
            logger.warning(str(FieldNotFound("session", event_url)))
            return tuple()
        return (Session(
                url=event_url,
                date=st.strftime("%Y-%m-%d %H:%M")
                ),)

    def __find_price(self, id: int, div: MyTag):
        for txt in div.select_txt("script"):
            m = re.search(r"loadOneBoxTicketsDetailActivity\(['\"]" + str(id) + r"['\"], ['\"](\d+)", txt)
            if m:
                return int(m.group(1))
        price = (div.select_one_txt(
            "div.card-detail div.card-block-btn span, div.card-detail div.reserva-ficha",
            if_none="warn"
        ) or "").lower()
        if "gratuita" in price:
            return 0
        if "reserva de entradas disponible próximamente" in price:
            return -1
        prcs = tuple(map(int, re.findall(r"(\d+)\s*€", price)))
        if len(prcs) == 0:
            n = div.select_one("#description-read")
            prcs = tuple(map(int, re.findall(r"(\d+)\s*€", get_text(n))))
        if len(prcs) == 0:
            logger.warning(str(FieldNotFound("price", div.url)))
            return 0
        return max(prcs)

    def __find_duration(self, url_event: str, soup: Tag, category: Category, info: Dict):
        duration = []
        for p in map(get_text, soup.find_all("p", string=re.compile(r".*\d+\s+minutos.*"))):
            duration.extend(map(int, re.findall(r"\d+", p)))
        if duration:
            return sum(duration)
        if category in (Category.EXPO, Category.WORKSHOP, Category.MUSIC, Category.CONFERENCE, Category.UNKNOWN):
            return 60
        div = soup.select_one("div.secondary-text")
        if div is not None:
            for re_dur in (
                r".* (una|1) hora.*",
                r".*duraci[oó]n:?\s*1\s*h.*"
            ):
                if div.find("p", string=re.compile(re_dur, re.I)):
                    return 60
        st: datetime = info.get("startDate")
        nd: datetime = info.get("endDate")
        if None not in (st, nd) and st.date() == nd.date():
            return int((nd-st).total_seconds() / 60)
        logger.warning(f"duración no encontrada en {url_event}")
        return 60

    def __find_category(self, div: MyIdTag, event_soup: MyTag):
        for ids, cat in self.__category.items():
            if div.id in ids:
                return cat
        for txt in event_soup.select_txt("div.activity-detail-block"):
            if re_or(plain_text(txt), "los niños y niñas tienen que ir siempre acompañados de un adulto"):
                return Category.CHILDISH
        txt = div.select_one_txt("p.on-title, div.pre-title", if_none="silent")
        cat = plain_text(txt.lower())
        if re_or(cat, "concierto"):
            return Category.MUSIC
        if re_or(cat, "exposicion"):
            return Category.EXPO
        if re_or(cat, "proyecciones", "proyección", "proyeccion", "cinefórum", "cineforum"):
            return Category.CINEMA
        if re_or(cat, "taller", "curso", "espacio educativo"):
            return Category.WORKSHOP
        if re_or(cat, "encuentro", "conferencia", "debate", "tertulia", "jornada", r"di[áa]logo"):
            return Category.CONFERENCE
        #if re_or(cat, "otros formatos"):  # , "espacio de mediacion", "espectaculo"):
        #    return Category.OTHERS
        if re_or(cat, "visita"):
            return Category.VISIT
        if re_or(cat, "performance"):
            return Category.THEATER
        txt_tit = div.select_one_txt("#title-read, div.card-txt-item h2", if_none="silent")
        tit = plain_text(txt_tit.lower())
        if re_or(tit, "magia"):
            return Category.MAGIC
        if re_or(tit, "muestra de moda"):
            return Category.EXPO
        txt_des = MD.convert(div.select_one_txt("div.primary-text p", if_none="silent"))
        if re_or(txt_des, "encuentro coreografico", "danza tradicional", flags=re.I):
            return Category.DANCE
        if re_or(txt_des, "cine", "cine-?f[óo]rum", "cine-?club", flags=re.I):
            return Category.CINEMA
        href = div.select_one_attr("div.card-viewmore a", "href", if_none="silent")
        plain_href = plain_text(href).lower()
        if re_or(plain_href, "circo"):
            return Category.CIRCUS
        if re_or(plain_href, "danza"):
            return Category.DANCE
        if re_or(plain_href, "cine", "cineforum", "cineclub"):
            return Category.CINEMA
        logger.critical(str(CategoryUnknown(div.url, f"{txt} {txt_tit} {txt_des} {href}")))
        return Category.UNKNOWN


if __name__ == "__main__":
    from core.log import config_log
    config_log("log/caixaforum.log", log_level=(logging.DEBUG))
    print(len(CaixaForum().events))
