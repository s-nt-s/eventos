from core.web import Web, get_text, get_attr
from portal.base import Base
import re
from datetime import datetime, date, timedelta
from core.util import get_festivos, to_uuid, get_domain
import logging
from core.event import Cinema, Category, Session
from core.place import Places
import pytz
from bs4 import Tag
from collections import defaultdict


logger = logging.getLogger(__name__)

TZ_ZONE = 'Europe/Madrid'

re_date = re.compile(
    r"^\s*Mi[eé]rcoles\s+(?P<day>\d{1,2})/(?P<month>\d{1,2})\s+(?P<times>\d{1,2}:\d{1,2}(?:[,\d\s:]*)?)\s*$",
    re.IGNORECASE
)
re_null = re.compile(r"^[\s\-\_]+$")

# https://cinescallao.es/dia-del-espectador/
def is_dia_del_espectador(d: date) -> bool:
    if d.weekday() != 2:
        return False
    festivos = get_festivos(d.year)
    if d in festivos:
        return False
    if (d+timedelta(days=1)) in festivos:
        return False
    return True


def _get_str(soup: Tag, rgx: str):
    r = re.compile(
        r"^\s*" + r"\s*".join(rgx.split()) + r"\s*$",
        re.IGNORECASE
    )
    for p in map(get_text, soup.select("p")):
        m = r.match(p or "")
        if m is None:
            continue
        v = m.group(1).strip()
        if len(v) == 0:
            return None
        return v


def _get_int(soup: Tag, rgx: str):
    v = _get_str(soup, rgx)
    if v:
        return int(v)


def _get_tup(soup: Tag, rgx: str):
    v = _get_str(soup, rgx)
    if not v:
        return tuple()
    return tuple(re.split(r", ", v))


class CinesCallao(Base):
    def __init__(self, cache: str | bool = True, cache_ttl: int = 3):
        super().__init__(cache=cache, cache_ttl=cache_ttl)
        self.__w = Web()
        self.__now = datetime.now(tz=pytz.timezone(TZ_ZONE))

    def _get_events(self):
        url = "https://cinescallao.es/cartelera-de-cine/"
        soup = self.__w.get(url)
        events: list[Cinema] = set()
        for p in soup.select("p"):
            txt = get_text(p)
            m = re_date.match(txt or "")
            if m is None:
                continue
            d = datetime(self.__now.year, int(m.group("month")), int(m.group("day")))
            if d.weekday() != 2:
                d = d.replace(year=self.__now.year + 1)
                if d.weekday() != 2:
                    continue
            if not is_dia_del_espectador(d.date()):
                continue
            div = p.find_parent("div", class_="et_pb_column")
            if div is None:
                logger.warning("No div found for event")
                continue
            shop = get_attr(
                div.find("a", string=re.compile(r"^\s*Comprar\s+entradas?\s*", re.IGNORECASE)),
                "href"
            )
            if shop is None:
                logger.warning("No shop found for event")
                continue
            shop = re.sub(r"[\?&]+$", "", shop)
            ficha = get_attr(
                div.select_one("a[href^='https://cinescallao.es/']"),
                "href"
            )
            soup_ficha = self.__w.get_cached_soup(ficha)
            sessions: set[Session] = set()
            for h, mm in re.findall(r"(\d{2}):(\d{2})", m.group("times")):
                d = d.replace(hour=int(h), minute=int(mm))
                sessions.add(Session(
                    date=d.strftime("%Y-%m-%d %H:%M"),
                    url=shop
                ))
            for tag in div.select("strong"):
                if re_null.match(tag.get_text()):
                    tag.extract()
            strong = div.select_one("div > p[style='text-align: center;'] > strong")
            c = Cinema(
                id=to_uuid(ficha),
                price=4.5,
                duration=_get_int(soup_ficha, r"Duración: (\d+) min") or 60,
                name=get_text(strong),
                url=ficha,
                img=get_attr(div.select_one("img"), "src"),
                category=Category.CINEMA,
                place=Places.CINES_CALLAO.value,
                sessions=tuple(sorted(sessions)),
                year=_get_int(soup_ficha, r"Estreno: \d+/\d+/(\d{4})"),
                director=_get_tup(soup_ficha, r"Director(?:es|as?)?: (.+)"),
            )
            events.add(c)
        events = set(Cinema.fusionIfSimilar(
            events,
            ('name', 'price')
        ))
        for e in list(events):
            if any(s.url for s in e.sessions):
                continue
            dom_url: dict[str, set[str]] = defaultdict(set)
            for u in (e.url, *e.also_in):
                d = get_domain(u)
                if d is not None:
                    dom_url[d].add(u)
            if tuple(sorted(dom_url.keys())) != ('cinescallao.es', 'reservaentradas.com'):
                continue
            c_urls = dom_url.get('cinescallao.es')
            r_urls = dom_url.get('reservaentradas.com')
            if len(c_urls) != 1 or len(r_urls) != 1:
                continue
            c_url = c_urls.pop()
            r_url = r_urls.pop()
            events.remove(e)
            events.add(e.merge(
                url=c_url,
                also_in=tuple(),
                sessions=tuple((s._replace(url=r_url) for s in e.sessions))
            ))
        return tuple(sorted(events))


if __name__ == "__main__":
    c = CinesCallao()
    ev = c.get_events()
    #print(*ev, sep="\n")
