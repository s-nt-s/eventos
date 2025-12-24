import xmltodict
from core.filemanager import FileManager
from core.cache import Cache
from core.web import Driver


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
    def get_data(self):
        url = "https://www.esmadrid.com/opendata/agenda_v1_es.xml"
        r = self.__s.get(url)
        r.raise_for_status()
        obj = xmltodict.parse(
            r.text
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
        
        return lst


if __name__ == "__main__":
    ApiEsMadrid().get_data()