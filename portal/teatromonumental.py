from core.web import WEB, get_text, get_query, Tag
from functools import cached_property
from core.event import Event, Session, Category, Places
from typing import NamedTuple
from types import MappingProxyType
from core.util import to_uuid
import re
import logging
from core.cache import TupleCache

logger = logging.getLogger(__name__)


class Info(NamedTuple):
    id: int
    name: str
    date: str
    location: str
    full: bool


def get_nums(s: str | Tag | None) -> tuple[int, ...]:
    if s is None:
        return tuple()
    if isinstance(s, Tag):
        s = get_text(s)
    return tuple(map(int, re.findall(r"\d+", s)))


class TeatroMonumental:

    @cached_property
    def info(self):
        rows: set[Info] = set()
        index = -1
        while True:
            index = index + 1
            soup = WEB.get(f"https://teatromonumental.entradas.com/webshop/webticket/include/eventlistdelta?&weekdaysstring=NNNNNNN&index={index}")
            lnks = list(soup.select("a[href]"))
            if len(lnks) == 0:
                break
            for a in lnks:
                a_txt = get_text(a)
                url = a.attrs.get("href")
                if url is None:
                    continue
                event_id = get_query(url).get("eventId")
                if event_id is None or not event_id.isdecimal():
                    logger.warning(f"event_id no esperado = {event_id} en {url}")
                    continue
                div = next(reversed(a.find_parents("div", class_="row")))
                dmy = get_nums(div.select_one("div.dateTime-date"))
                hm = get_nums(div.select_one("span.evt-event-detail__time"))
                rows.add(Info(
                    id=int(event_id),
                    name=get_text(div.select_one("h2")),
                    date="{2:04}-{1:02}-{0:02} {3:02}:{4:02}".format(*dmy, *hm),
                    location=get_text(div.select_one("div.event-address-item")),
                    full=(a_txt == "Entradas agotadas")
                ))
        return MappingProxyType({r.id: r for r in rows})

    @cached_property
    def urls(self):
        urls: set[str] = set()
        soup = WEB.get("https://www.teatromonumental.es/")
        for a in soup.select("a[href]"):
            href = a.attrs.get("href")
            txt = get_text(a)
            if href and txt == "+ Info":
                urls.add(href)
        return tuple(sorted(urls))

    @property
    @TupleCache("rec/teatromonumental.json", builder=Event.build)
    def events(self):
        logger.info("TeatroMonumental: Buscando eventos")
        all_events: set[Event] = set()
        for url in self.urls:
            e = self.__get_event(url)
            if e is not None:
                all_events.add(e)

        evs = Event.fusionIfSimilar(
            all_events,
            ('name', 'place'),
            firstEventUrl=True
        )
        logger.info(f"TeatroMonumental: Buscando eventos = {len(evs)}")
        return evs

    def __get_event(self, url: str) -> Event | None:
        soup = WEB.get(url)
        price, sessions = self.__get_price_and_sessions(url, soup)
        if len(sessions) == 0:
            return None
        full_name = self.__get_name(soup)
        img = soup.select_one("img.imagen_post_single").attrs.get("src")
        name = re.sub(r"\s+[A-Z]\s*/\s*\d+\s*$", "", full_name)
        if name != full_name:
            sessions = tuple(s.merge(title=full_name) for s in sessions)
        ev = Event(
            id="tm"+to_uuid(url),
            url=url,
            name=name,
            img=img,
            price=price,
            category=Category.MUSIC,
            duration=None,
            sessions=sessions,
            place=Places.TEATRO_MONUMENTAL.value
        )
        return ev

    def __get_price_and_sessions(self, url: str, soup: Tag):
        prices: set[int] = set()
        sessions: set[Session] = set()
        sct = soup.select("section.box-info")
        lnk = soup.select("a.comprar_boton")
        if len(sct) != len(lnk):
            raise ValueError(f"Número de sesiones distinto en {url}")
        for section, a in zip(sct, lnk):
            info: dict[str, str] = {}
            for p in section.select("p"):
                txt = get_text(p)
                if p and ":" in txt:
                    key, value = map(str.strip, txt.split(":", 1))
                    if key and value:
                        info[key.lower()] = value
            dmy = get_nums(info.get("fecha"))
            hm = get_nums(info.get("hora"))
            url_session = a.attrs.get("href")
            eventId = int(get_query(url_session)["event"])
            i = self.info.get(eventId)
            if i is None:
                logger.critical(f"ID no encontrado en info: {eventId} en {url}")
            else:
                if i.location != "Teatro Monumental":
                    logger.critical(f"Lugar inesperado: {i.location} en {url}")
                if i.full:
                    continue
            prices.update(get_nums(info.get("precio desde")))
            sessions.add(Session(
                date="{2:04}-{1:02}-{0:02} {3:02}:{4:02}".format(*dmy, *hm),
                url=a.attrs.get("href"),
            ))
        price = max(prices) if len(prices) > 0 else None
        return price, tuple(sorted(sessions))

    def __get_name(self, soup: Tag):
        name = get_text(soup.select_one("header.box-title"))
        for k, v in {
            "ENSAYO GENERAL": "Ensayo general",
            "CONCIERTO SINFÓNICO": "Concierto sinfónico",
            "JÓVENES MÚSICOS": "Jóvenes músicos",
            "CICLO DE MÚSICA DE CÁMARA DE LA ORQUESTA Y CORO RTVE": "Ciclo de música de cámara de la Orquesta y Coro RTVE",
        }.items():
            name = re.sub(r"\b" + re.escape(k) + r"\b", v, name, flags=re.I)
        return name


if __name__ == "__main__":
    tm = TeatroMonumental()
    for x in tm.events:
        print(x.price, x.name, len(x.sessions), x.url)
