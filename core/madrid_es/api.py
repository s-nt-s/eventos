from core.madrid_es.datos import DatosMadridEs, Item as DatosItem
from core.madrid_es.form import FormSearch, get_vgnextoid, Item as FormItem
from typing import NamedTuple, Optional
from core.madrid_es.tp import Place
from core.util import find_euros, get_obj
import logging
from core.cache import TupleCache
import re

logger = logging.getLogger(__name__)
re_sp = re.compile(r"\s+")


class Event(NamedTuple):
    title: str
    url: str
    price: Optional[float] = None
    place: Optional[Place] = None
    audience: tuple[str, ...] = tuple()
    category: tuple[str, ...] = tuple()
    description: Optional[str] = None

    @staticmethod
    def build(*args, **kwargs):
        obj = get_obj(*args, **kwargs)
        if obj is None:
            return None
        for k, v in list(obj.items()):
            if isinstance(v, list):
                obj[k] = tuple(v)
            if k == "place" and isinstance(v, dict):
                obj[k] = Place(**v)
        return Event(**obj)


def _join(a: str | tuple[str, ...] | None, b: str | tuple[str, ...] | None):
    def _to_list(i: str | tuple[str, ...] | None):
        if i is None:
            return []
        if isinstance(i, tuple):
            return list(i)
        if isinstance(i, str):
            return [i]
        raise ValueError(i)

    arr: list[str] = []
    for i in _to_list(a)+_to_list(b):
        if i is None:
            continue
        i = re_sp.sub(" ", i.lower()).strip()
        if len(i) == 0:
            continue
        i = {
            "inmigrantesy emigrantes": "inmigrantes y emigrantes",
            "jovenes": "jóvenes",
            "niños": "niños y niñas",
            "poblacion general": "población general",
        }.get(i, i)
        if i not in arr:
            arr.append(i)

    return tuple(arr)


class Api:
    def __init__(self):
        self.__datos = DatosMadridEs()
        self.__form = FormSearch()

    @TupleCache("rec/apimadrides/events.json", builder=Event.build)
    def get_events(self):
        events: set[Event] = set()
        data = {get_vgnextoid(i.url): i for i in self.__datos.get_events()}
        for e in self.__form.get_events():
            _id_ = get_vgnextoid(e.url)
            if _id_ is None:
                raise ValueError(e)
            d = data.get(_id_)
            i = Event(
                title=e.title,
                url=e.url,
                place=e.place or (d.place if d else None),
                price=self.__find_price(e, d),
                audience=_join(e.audience, d.audience if d else None),
                category=_join(e.category, d.category if d else None),
                description=d.description if d else None
            )
            events.add(i)
        dupes: dict[str, int] = {}
        for e in events:
            _id_ = get_vgnextoid(e.url)
            dupes[_id_] = dupes.get(_id_, 0) + 1
        dupes = sorted(k for k, v in dupes.items() if v > 1)
        if dupes:
            raise ValueError(f"ids duplicates: {', '.join(dupes)}")
        return tuple(sorted(events))

    def __find_price(self, a: FormItem, b: DatosItem | None):
        if a.free is True:
            return 0
        if b:
            if b.free is True:
                return 0
            for txt in (
                b.price,
                b.description
            ):
                prc = find_euros(txt)
                if prc is not None:
                    return prc
            if b.price not in (
                None,
                "Entradas disponibles próximamente en entradas.com y en la taquilla del recinto",
                "Consultar descuentos especiales",
            ):
                logger.critical(f"Campo price inexperado: {prc}")


if __name__ == "__main__":
    from core.log import config_log
    config_log("log/madrid_es_api.log")
    a = Api()
    e = a.get_events()
    print(len(e))
    print(len(set(get_vgnextoid(i.url) for i in e)))
