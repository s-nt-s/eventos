from requests import Session
from core.cache import Cache
from core.filemanager import FM
from urllib.parse import urljoin


def _has_to_be(i: dict, k: str, val):
    v = i.get(k)
    if v is None and val is not None:
        raise ValueError(f"¿{k}={v}?")
    if val is None and v is not None:
        raise ValueError(f"¿{k}={v}?")
    if type(v) is not type(val):
        raise ValueError(f"¿{k}={v}?")
    if v != val:
        raise ValueError(f"¿{k}={v}?")
    del i[k]


def _has_to_be_value_dict(i: dict, k: str, attr: str):
    v = i.get(k)
    if v is None:
        return
    if not isinstance(v, dict) or tuple(v.keys()) != (attr, ):
        raise ValueError(f"¿{k}={v}?")
    val = v[attr]
    i[k] = val


def _has_to_be_list_value_dict(i: dict, k: str, attr: str):
    v = i.get(k)
    if v is None:
        return
    if not isinstance(v, list):
        raise ValueError(f"¿{k}={v}?")
    arr = []
    for x in v:
        if not isinstance(x, dict) or tuple(x.keys()) != (attr, ):
            raise ValueError(f"¿{k}={v}?")
        arr.append(x[attr])
    i[k] = arr


class ReinaSofia:
    ROOT = "https://www.museoreinasofia.es"
    IMG = "https://recursos.museoreinasofia.es/styles/large_landscape/public/"
    SEARCH = "https://buscador.museoreinasofia.es/api/search?langcode=es&exactMatch=false"

    def __init__(self):
        self.__s = Session()
        self.__size = 100

    @property
    @Cache("rec/reinasofia/index.json")
    def _index(self):
        r = self.__s.get(f"{ReinaSofia.SEARCH}&pageSize={self.__size}")
        js = r.json()
        arr: list[dict] = []
        for i in js['results']:
            if not isinstance(i, dict):
                raise ValueError(i)
            t = i.get('template')
            if t == "past":
                continue
            _has_to_be(i, "template", "future")
            _has_to_be(i, "hidden", False)
            _has_to_be(i, "isPublished", True)
            _has_to_be(i, "bundle", "activity")
            _has_to_be_value_dict(i, 'url', 'path')
            _has_to_be_value_dict(i, 'title', 'value')
            _has_to_be_value_dict(i, 'subtitle', 'value')
            _has_to_be_value_dict(i, 'description', 'value')
            _has_to_be_value_dict(i, 'language', 'id')
            _has_to_be_value_dict(i, 'mainMedia', 'entity')
            _has_to_be_value_dict(i, 'parent', 'entity')
            _has_to_be_list_value_dict(i, 'processedDates', 'value')
            for k in ('score', ):
                if k in i:
                    del i[k]
            i['id'] = int(i['id'])
            i['url'] = urljoin(ReinaSofia.ROOT, i['url'])
            src = i['mainMedia']['image']['originalSrc']
            i['mainMedia']['image']['originalSrc'] = urljoin(ReinaSofia.IMG, src)
            p = i['parent']
            if p:
                _has_to_be_value_dict(p, 'url', 'path')
                _has_to_be_value_dict(p, 'title', 'value')
                _has_to_be_value_dict(p, 'subtitle', 'value')
                _has_to_be_value_dict(p, 'description', 'value')
                _has_to_be_value_dict(p, 'mainMedia', 'entity')
                _has_to_be_value_dict(p, 'activities', 'data')
                _has_to_be_list_value_dict(p, 'categories', 'entity')
                p['id'] = int(p['id'])
                p['url'] = urljoin(ReinaSofia.ROOT, p['url'])
                for a in p['activities']:
                    if int(a['id']) != i['id']:
                        continue
                    _has_to_be_list_value_dict(a, 'categories', 'entity')
                    for k in ('categories', 'duration'):
                        if not i.get(k):
                            i[k] = a.get(k)
                for k in set(p.keys()).difference({'id', 'url', 'title', 'description', 'mainMedia', 'categories'}):
                    continue
                    del p[k]
            arr.append(i)
        arr = FM.parse_obj(
            arr,
            compact=True
        )
        return arr

if __name__ == "__main__":
    r = ReinaSofia()
    r._index
