import re
from typing import List, Dict, Union, Set, Tuple, Optional, Callable, TypeVar, Iterable
from bs4 import Tag, BeautifulSoup
import logging
from unidecode import unidecode
from urllib.parse import urlparse, ParseResult
import pytz
from datetime import datetime
from math import radians, sin, cos, sqrt, atan2
from collections import Counter, defaultdict
from os import environ

import uuid

UUID_NAMESPACE = uuid.UUID('00000000-0000-0000-0000-000000000000')

T = TypeVar('T')
KeyTuple = TypeVar("KeyTuple", bound=tuple)
ValObject = TypeVar("ValObject", bound=object)

logger = logging.getLogger(__name__)

MONTH = ('ene', 'feb', 'mar', 'abr', 'may', 'jun', 'jul', 'ago', 'sep', 'oct', 'nov', 'dic')

re_sp = re.compile(r"\s+")

tag_concat = ('u', 'ul', 'ol', 'i', 'em', 'strong')
tag_round = ('u', 'i', 'em', 'span', 'strong', 'a')
tag_trim = ('li', 'th', 'td', 'div', 'caption', 'h[1-6]')
tag_right = ('p',)
heads = ("h1", "h2", "h3", "h4", "h5", "h6")
block = heads + ("p", "div", "table", "article")
inline = ("span", "strong", "i", "em", "u", "b", "del")


def get_domain(url: str):
    if url is None or len(url) == 0:
        return None
    parsed_url: ParseResult = urlparse(url)
    domain: str = parsed_url.netloc.lower()
    if domain.startswith("www."):
        domain = domain[4:]
    return domain


def get_img_src(n: Tag):
    if n is None:
        return None
    src = n.attrs.get('src')
    if not isinstance(src, str):
        return None
    src = src.rstrip(" /").strip()
    if len(src) == 0:
        return None
    sch = src.split("://")[0].lower()
    if sch not in ("https", "http"):
        return None
    name = src.rsplit("/", 1)[-1]
    if "." not in name:
        return None
    return src


def get_a_href(n: Tag):
    if n is None:
        return None
    href = n.attrs.get('href')
    if not isinstance(href, str):
        return None
    href = href.strip()
    if len(href) == 0:
        return None
    sch = href.split("://")[0].lower()
    if sch not in ("https", "http"):
        return None
    return href


def get_obj(*args, **kwargs) -> dict:
    if len(args) != 0 and len(kwargs) != 0:
        raise ValueError()
    if len(args) > 1:
        raise ValueError()
    if len(args) == 0:
        return kwargs
    obj = args[0]
    if obj is not None and not isinstance(obj, (dict, list)):
        raise ValueError()
    return obj


def trim(s: str):
    if s is None:
        return None
    s = s.strip()
    if len(s) == 0:
        return None
    return s


def get_text(n: Tag):
    if n is None:
        return None
    txt = n.get_text()
    txt = re_sp.sub(r" ", txt)
    txt = txt.strip()
    if len(txt) == 0:
        return None
    return txt



def dict_add(obj: Dict[str, Set], a: str, b: Union[str, int, List[str], Set[str], Tuple[str]]):
    if a not in obj:
        obj[a] = set()
    if isinstance(b, (str, int)):
        obj[a].add(b)
    else:
        obj[a] = obj[a].union(b)


def plain_text(s: Union[str, Tag], is_html=False):
    if isinstance(s, str) and is_html:
        s = BeautifulSoup(s, "html.parser")
    if isinstance(s, Tag):
        for n in s.findAll(["p", "br"]):
            n.insert_after(" ")
        s = get_text(s)
    if s is None:
        return None
    faken = "&%%%#%%%#%%#%%%%%%&"
    s = re.sub(r"[,\.:\(\)\[\]¡!¿\?\"']", " ", s).lower()
    s = s.replace("ñ", faken)
    s = unidecode(s)
    s = s.replace(faken, "ñ")
    s = re_sp.sub(" ", s).strip()
    if len(s) == 0:
        return None
    return s


def re_or(s: str, *args: Union[str, Tuple[str]], to_log: str = None, flags=0):
    if s is None or len(s) == 0 or len(args) == 0:
        return None
    for r in args:
        if isinstance(r, tuple):
            b = re_and(s, *r, flags=flags)
            if b is not None:
                if to_log:
                    logger.debug(f"{to_log} cumple {b}")
                return b
        else:
            reg = str(r)
            if reg[0] not in ("^", " "):
                reg = r"\b" + reg
            if reg[-1] not in ("$", " "):
                reg = reg + r"\b"
            if re.search(reg, s, flags=flags):
                if to_log:
                    logger.debug(f"{to_log} cumple {r}")
                return r
    return None


def re_and(s: str, *args: Union[str, Tuple[str]], to_log: str = None, flags=0):
    if s is None or len(s) == 0 or len(args) == 0:
        return None
    arr = []
    for r in args:
        if isinstance(r, tuple):
            b = re_or(s, *r, flags=flags)
            if b is None:
                return None
            arr.append(b)
        elif re.search(r"\b" + r + r"\b", s, flags=flags):
            arr.append(r)
        else:
            return None
    txt = " AND ".join(arr)
    if to_log:
        logger.debug(f"{to_log} cumple {txt}")
    return txt


def to_datetime(s: str):
    if s is None:
        return None
    tz = pytz.timezone('Europe/Madrid')
    dt = datetime.strptime(s, "%Y-%m-%d %H:%M")
    return tz.localize(dt)


def getKm(lat1: float, lon1: float, lat2: float, lon2: float):
    # Radio de la Tierra en kilómetros
    R = 6371.0

    lat1_rad = radians(lat1)
    lon1_rad = radians(lon1)
    lat2_rad = radians(lat2)
    lon2_rad = radians(lon2)

    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    # Fórmula de Haversine
    a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    # Distancia
    distance = R * c
    return abs(distance)


def my_filter(iterable: Iterable[T], func: Callable[[T], bool]) -> Tuple[List[T], List[T]]:
    ok: List[T] = []
    ko: List[T] = []
    for i in iterable:
        if func(i) is True:
            ok.append(i)
        else:
            ko.append(i)
    return ok, ko


def get_main_value(arr: List[T], default: Optional[T] = None) -> Union[T, None]:
    if arr is None:
        return default
    if len(arr) == 0:
        return default
    contador = Counter(arr)
    max_rep = max(contador.values())
    for e in arr:
        if contador[e] == max_rep:
            return e


def to_uuid(s: str):
    try:
        _ = uuid.UUID(s)
        return s.upper()
    except ValueError:
        return str(uuid.uuid5(UUID_NAMESPACE, s)).upper()


def uniq(*args: Union[str, None]):
    arr: List[str] = []
    for a in args:
        if a not in (None, '') and a not in arr:
            arr.append(a)
    return arr


def iter_chunk(size: int, args: list):
    arr = []
    for a in args:
        arr.append(a)
        if len(arr) == size:
            yield arr
            arr = []
    if arr:
        yield arr


def get_env(*args: str, default: str = None) -> str | None:
    for a in args:
        v = environ.get(a)
        if isinstance(v, str):
            v = v.strip()
            if len(v):
                return v
    return default


def find_duplicates(
    evs: Iterable[ValObject],
    mk_key: Callable[[ValObject], Optional[KeyTuple]],
) -> Tuple[Tuple[ValObject, ...], ...]:
    data_set: Dict[KeyTuple, List[ValObject]] = defaultdict(list)
    for e in evs:
        k = mk_key(e)
        if k is not None and e not in data_set[k]:
            data_set[k].append(e)
    data: list[Tuple[ValObject, ...]] = []
    for k, v in data_set.items():
        if len(v) > 1:
            data.append(tuple(v))
    return tuple(data)
