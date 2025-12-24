import xmltodict
from core.filemanager import FileManager
from core.cache import Cache
from core.web import Driver


def is_dict(obj, *keys: str):
    if not isinstance(obj, dict):
        return False
    ks = tuple(sorted(obj.keys()))
    if ks != tuple(sorted(keys)):
        return False
    return True


def flatten(lst):
    if isinstance(lst, list):
        for x in lst:
            if isinstance(x, list):
                yield from flatten(x)
            else:
                yield x


def re_parse(value):
    val = list_item_to_obj(value)
    if isinstance(val, dict):
        return val
    #val = list_obj_item_to_obj(value)
    #if isinstance(val, dict):
    #    return val


def list_obj_item_to_obj(arr):
    if not isinstance(arr, list):
        return None
    if len(arr) == 0:
        return None
    obj = {}
    for a in arr:
        if not isinstance(a, dict):
            return None
        ks = tuple(sorted(a.keys()))
        if ks not in ('item', ):
            return None
        v = obj['item']
        if not isinstance(v, dict):
            return None
        for kk, vv in v.items():
            if kk in obj and obj[kk] != vv:
                return None
            obj[kk] = vv
    if len(obj):
        return obj


def list_item_to_obj(arr: list):
    if not isinstance(arr, list):
        return None
    if len(arr) == 0:
        return None
    obj = {}
    for a in arr:
        if not isinstance(a, dict):
            return None
        ks = tuple(sorted(a.keys()))
        if ks not in (('#text', '@name'), ('@name',)):
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
    def __init__(self):
        self.__s = Driver.to_session(
            "firefox",
            "https://www.esmadrid.com/"
        )

    @Cache("rec/esmadrid/dataset.json")
    def get_data(self) -> list[dict]:
        url = "https://www.esmadrid.com/opendata/agenda_v1_es.xml"
        r = self.__s.get(url)
        r.raise_for_status()
        obj = xmltodict.parse(
            r.text,
        )
        obj = FileManager.parse_obj(
            obj,
            compact=True,
            re_parse=re_parse

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

        def _get(obj: dict, *path):
            obj = {None: obj}
            path = list(reversed(path))
            path.append(None)
            while path and obj and isinstance(obj, dict):
                obj = obj.get(path.pop())
                if len(path) == 0:
                    return obj

        def _flatten(obj: dict, *path):
            prt = _get(obj, *path[:-1])
            val = _get(obj, *path)
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
            cats = _get(val, 'extradata', 'categorias')
            if cats is not None:
                if not is_dict(cats, "categoria") or val['extradata'].get('categoria') is not None:
                    raise ValueError("No es un {{'serviceList': {'service': [{'extradata': {'categorias': {'categoria': ...}}}}]}} "+url)
                val['extradata']['categoria'] = cats['categoria']
                del val['extradata']['categorias']
            cats = _get(val, 'extradata', 'categoria')
            if isinstance(cats, list):
                for i, cat in enumerate(cats):
                    if isinstance(cat, dict):
                        _flatten(cat, 'subcategorias', 'subcategoria')
                        subcats = _get(cat, 'subcategorias')
                        if subcats is not None:
                            if not isinstance(subcats, dict):
                                raise ValueError("subcategorias no es un dict en "+url)
                            if not is_dict(subcats, "subcategoria") or not not is_dict(cat.get('subcategoria'), 'item'):
                                raise ValueError("subcategorias no es un {'subcategoria': {'item': ...}} en "+url)
                            cat['subcategoria'] = subcats['subcategoria']['item']
                            del cat['subcategorias']
                        val['extradata']['categoria'][i] = cat

        return lst


if __name__ == "__main__":
    d = ApiEsMadrid().get_data()
    print(len(d))