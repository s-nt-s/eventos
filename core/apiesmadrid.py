import xmltodict
from core.filemanager import FileManager
from core.cache import Cache, TupleCache
from core.web import Driver, buildSoup, get_text
import re
from typing import NamedTuple, Optional
from core.util import get_obj
from core.dictwraper import DictWraper
import logging
from datetime import timedelta, date
from core.util import re_or

logger = logging.getLogger(__name__)
TODAY = date.today()


class EsMadridEvent(NamedTuple):
    id: int
    fechaActualizacion: str
    name: str
    body: str
    web: str
    address: str
    latitude: float
    longitude: float
    dates: tuple[str, ...]
    imgs: tuple[str, ...]
    category: tuple[str, ...]
    price: Optional[float | int] = None
    zipcode: Optional[int] = None

    @staticmethod
    def build(*args, **kwargs):
        obj = get_obj(*args, **kwargs)
        if obj is None:
            return None
        for k, v in list(obj.items()):
            if isinstance(v, list):
                obj[k] = tuple(v)
        return EsMadridEvent(**obj)


def is_dict(obj, *keys: str):
    if not isinstance(obj, dict):
        return False
    ks = tuple(sorted(obj.keys()))
    if ks != tuple(sorted(keys)):
        return False
    return True


def get_path(obj: dict, *path, default=None):
    obj = {None: obj}
    path = list(reversed(path))
    path.append(None)
    while path and obj and isinstance(obj, dict):
        obj = obj.get(path.pop())
        if len(path) == 0:
            if obj is None:
                return default
            return obj
    return default


def flatten(lst):
    if isinstance(lst, list):
        for x in lst:
            if isinstance(x, list):
                yield from flatten(x)
            else:
                yield x


def list_item_to_obj(arr: list):
    if not isinstance(arr, list):
        return None
    if len(arr) == 0:
        return None
    obj = {}
    for a in arr:
        if not is_dict(a, '#text', '@name') and not is_dict(a, '@name'):
            return None
        k = a['@name']
        v = a.get('#text', '')
        if not isinstance(k, str) or not isinstance(v, str):
            return None
        k = k.strip()
        v = v.strip()
        if len(k) == 0 and len(v) == 0:
            continue
        if len(k) == 0:
            return None
        if k in obj and obj[k] != v:
            return None
        obj[k] = v
    if len(obj):
        obj = {k: v for k, v in obj.items() if v != ''}
        return obj


class ApiEsMadrid:
    @Cache("rec/esmadrid/pre_dataset.json")
    def __get_data(self) -> list[dict]:
        session = Driver.to_session(
            "firefox",
            "https://www.esmadrid.com/"
        )
        url = "https://www.esmadrid.com/opendata/agenda_v1_es.xml"
        r = session.get(url)
        r.raise_for_status()
        obj = xmltodict.parse(
            r.text,
        )
        obj = FileManager.parse_obj(
            obj,
            compact=True,
            re_parse=list_item_to_obj

        )
        if not isinstance(obj, dict):
            raise ValueError("No es un {...} "+url)
        if tuple(obj.keys()) != ('serviceList', ):
            raise ValueError("No es un {'serviceList': ...} "+url)
        obj = obj['serviceList']
        if not isinstance(obj, dict):
            raise ValueError("No es un {'serviceList': {...}} "+url)
        if tuple(obj.keys()) != ('service', ):
            raise ValueError("No es un {{'serviceList': {'service': ...}} "+url)
        lst = obj['service']
        if not isinstance(lst, list):
            raise ValueError("No es un {{'serviceList': {'service': [...]}} "+url)

        def _flatten(obj: dict, *path):
            prt = get_path(obj, *path[:-1])
            val = get_path(obj, *path)
            if isinstance(val, (dict, str)):
                prt[path[-1]] = [val]
            elif isinstance(val, list):
                prt[path[-1]] = list(flatten(val))

        for val in lst:
            if not isinstance(val, dict):
                raise ValueError("No es un {{'serviceList': {'service': [dict]}} "+url)
            _flatten(val, 'multimedia', 'media')
            _flatten(val, 'extradata', 'fechas', 'rango')
            _flatten(val, 'extradata', 'fechas', 'exclusion')
            _flatten(val, 'extradata', 'fechas', 'inclusion')
            _flatten(val, 'extradata', 'categorias', 'categoria')
            cats = get_path(val, 'extradata', 'categorias', 'categoria', default=[])
            if not isinstance(cats, list):
                raise ValueError("No es un {{'serviceList': {'service': [{'extradata': {'categoria': [...]}}]}} "+url)
            for i, cat in enumerate(cats):
                if not isinstance(cat, dict):
                    raise ValueError("No es un {{'serviceList': {'service': [{'extradata': {'categoria': [dict]}}]}} "+url)
                _flatten(cat, 'subcategorias', 'subcategoria')
                val['extradata']['categorias']['categoria'][i] = cat
        return lst

    @Cache("rec/esmadrid/dataset.json")
    def get_data(self) -> list[dict]:
        lst = self.__get_data()

        def _unwrap(obj, key: str):
            if not isinstance(obj, (list, dict)):
                return obj
            if isinstance(obj, list):
                obj = [_unwrap(o, key) for o in obj]
                if all(is_dict(o, 'key') for o in obj):
                    obj = [o['item'] for o in obj]
                return obj
            obj = {k: _unwrap(v, key) for k, v in obj.items()}
            wrap = obj.get(key)
            if isinstance(wrap, dict):
                for kk, vv in wrap.items():
                    if obj.get(kk) not in (None, vv):
                        raise ValueError(f"El unwrap de {key} machacaría un valor")
                    obj[kk] = vv
                del obj[key]
            return obj

        def _cast(obj, key: str, tp):
            if not isinstance(obj, (list, dict)):
                return obj
            if isinstance(obj, list):
                return [_cast(i, key, tp) for i in obj]
            obj = {k: _cast(v, key, tp) for k, v in obj.items()}
            val = obj.get(key)
            if isinstance(val, list):
                obj[key] = list(map(tp, val))
            elif val is not None:
                obj[key] = tp(val)
            return obj

        def _cast_phone(x):
            if x is None:
                return None
            if not isinstance(x, str):
                raise ValueError(x)
            x = re.sub(r"\+", "00", x)
            x = re.sub(r"[\(\)\s]+", "", x)
            x = re.sub(r"^0034", "", x)
            if len(x) == 11:
                x = re.sub(r"^34", "", x)
            return int(x)

        def _cast_unwrap(key: str):
            def _aux_unwrap(x):
                if x is None:
                    return None
                if not is_dict(x, key):
                    raise ValueError(x)
                return x[key]
            return _aux_unwrap

        def _cast_date(x):
            if x is None:
                return None
            if not isinstance(x, str):
                raise ValueError(x)
            if not re.match(r"^\d+/\d+/\d+$", x):
                raise ValueError(x)
            d, m, y = map(int, re.findall("\d+", x))
            return f"{y:04d}-{m:02d}-{d:02d}"

        lst = _unwrap(lst, 'item')
        for k in ('@id', 'idrt', 'zipcode', 'idCategoria', 'idSubCategoria', 'idTipo'):
            lst = _cast(lst, k, int)
        for k in ('latitude', 'longitude'):
            lst = _cast(lst, k, float)
        for k in ('inicio', 'fin', 'exclusion', 'inclusion'):
            lst = _cast(lst, k, _cast_date)
        lst = _cast(lst, "phone", _cast_phone)
        lst = _cast(lst, "multimedia", _cast_unwrap('media'))
        lst = _cast(lst, "categorias", _cast_unwrap('categoria'))
        lst = _cast(lst, "subcategorias", _cast_unwrap('subcategoria'))
        lst = _cast(lst, "dias", lambda x: list(sorted(map(int, re.split(r"\s*,\s*", x)))))

        return lst

    @TupleCache("rec/esmadrid.json", builder=EsMadridEvent.build)
    def get_events(self):
        events: set[EsMadridEvent] = set()
        for obj in map(DictWraper, self.get_data()):
            e = self.__to_event(obj)
            if e:
                events.add(e)
        return tuple(sorted(events))

    def __to_event(self, o: dict):
        obj = DictWraper(o)
        id = obj.get_int('@id')
        b = obj.get_dict('basicData')
        g = obj.get_dict('geoData')
        e = obj.get_dict('extradata')
        web = b.get_str('web')

        latitude = g.get_float_or_none('latitude')
        longitude = g.get_float_or_none('longitude')
        address = g.get_str_or_none('address')
        if None in (latitude, longitude) and address is None:
            logger.warning(f"{id} descartado por falta de ubicación {web}")
            return None

        for k, (fnc, ok, ko) in {
            'language': (b.get_str, ('es', ), None),
            'fechas': (e.get_dict_or_none, None, (None, )),
        }.items():
            x = fnc(k)
            if (ok and x not in ok) or (ko and x in ko):
                logger.warning(f"{id} descartado por {k}={x} {web}")
                return None
        tp_fechas = self.__get_dates(obj)
        if len(tp_fechas) == 0:
            if (ok and x not in ok) or (ko and x in ko):
                logger.warning(f"{id} descartado por no tener fechas {web}")
                return None

        price = self.__get_price(obj)
        if isinstance(price, str):
            logger.warning(f"{id} descartado por price={price} {web}")
            return None

        e = EsMadridEvent(
            id=obj.get_int('@id'),
            web=web,
            fechaActualizacion=obj.get_str('@fechaActualizacion'),
            name=b.get_str('name'),
            body=b.get_str('body'),
            address=address,
            zipcode=g.get_int_or_none('zipcode'),
            latitude=latitude,
            longitude=longitude,
            imgs=tuple(m['url'] for m in obj.get_list_or_empty('multimedia') if m.get('@type') == 'image'),
            price=price,
            dates=tp_fechas,
            category=self.__get_categorias(obj)
        )
        return e

    def __get_dates(self, obj: DictWraper):
        set_fechas: set[str] = set()
        e = obj.get_dict('extradata')
        fechas = e.get_dict('fechas')
        exc = fechas.get_list_or_empty('exclusion')
        inc = fechas.get_list_or_empty('inclusion')
        rango = fechas.get_list('rango')
        for r in map(DictWraper, rango):
            inicio = r.get_date('inicio', '%Y-%m-%d')
            fin = r.get_date('fin', '%Y-%m-%d')
            while inicio <= fin:
                dt = inicio.strftime("%Y-%m-%d")
                if inicio >= TODAY and dt not in exc or dt in inc:
                    set_fechas.add(dt)
                inicio = inicio + timedelta(days=1)
        return tuple(sorted(set_fechas))

    def __get_price(self, obj: DictWraper):
        web = obj.get_dict('basicData').get_str('web')

        def _get_txt(html: str):
            if html is not None:
                soup = buildSoup(web, html)
                text = get_text(soup)
                return text

        def _get_price(text: str):
            if text in (
                None,
                'Entradas agotadas',
                'Por confirmar',
                'Por determinar'
            ):
                return text
            num: set[float] = set()
            if re_or(text, "gratuit[oa]s?", "entradas? libres?", "Acceso libre", flags=re.I):
                num.add(0)
            for x in re.findall(r"(\d[,\.\d]*)\s*(?:€|euros?)", text):
                num.add(float(x.replace(",", ".")))
            if len(num):
                p = max(num)
                if int(p) == p:
                    p = int(p)
                return p
            if re_or(
                text,
                ".*Inscripciones agotadas.*",
                flags=re.I
            ):
                return text

        body = _get_txt(obj.get_dict('basicData').get_str('body'))
        pago = _get_txt(obj.get_dict('extradata').get_str_or_none('Servicios de pago'))

        price_pago = _get_price(pago)
        if isinstance(price_pago, (float, int)):
            return price_pago
        price_body = _get_price(body)
        if isinstance(price_body, (float, int)):
            return price_body

        if isinstance(price_pago, str):
            return price_pago

        if pago in (
            None,
            'Consultar página oficial',
            'Consultar web oficial. ¡Inscripciones abiertas!',
            'Consultar programación'
        ):
            return None
        if re_or(
            pago,
            ".*consultar (precios? )?(en )?(web|p[aá]gina|programa(ci[oó]n)?)",
            "Reserva tu visita",
            flags=re.I
        ):
            return None

        logger.warning(f"Precio no encontrado en: {pago}")
        #raise ValueError([text, pago])

    def __get_categorias(self, obj: DictWraper):
        def _iter_cat():
            for i in map(DictWraper, obj.get_dict('extradata').get_list_or_empty('categorias')):
                c = i.get_str('Categoria')
                subs = i.get_list_or_empty('subcategorias')
                if len(subs) == 0:
                    yield c
                    continue
                for x in map(DictWraper, subs):
                    s = x.get_str('SubCategoria')
                    yield f"{c} - {s}"

        cats: list[str] = []
        for c in _iter_cat():
            if c is not cats:
                cats.append(c)
        return tuple(cats)

if __name__ == "__main__":
    from core.log import config_log
    config_log("log/esmadrid.log")
    d = ApiEsMadrid().get_events()
    print(len(d))
