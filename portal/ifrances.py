from core.web import Web, get_text, Tag
from functools import cached_property
from core.cache import Cache, TupleCache
from urllib.parse import urlencode, urljoin
from core.util import parse_obj, re_or
from core.event import Event, Category, CategoryUnknown, Session, Place
import re
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

re_sp = re.compile(r"\s+")


def _max(i: dict, *keys):
    val = None
    for k in keys:
        v = i.get(k)
        if val is None or (v is not None and v>val):
            val = v
    return val


def _clean_name(s: str):
    if s is None:
        return None
    s = re.sub(r"^.*?\s+\|\s+", "", s)
    s = re_sp.sub(" ", s).strip()
    s = re.sub(r"^Preestreno\s*:\s*", "", s, flags=re.I)
    s = re.sub(r"\s*\-\s*\d+\s*h(\s*\d+m?)?$", "", s, flags=re.I)
    s = re.sub(r"^CINE\s*//\s*Preestreno\s*:?\s*", "", s, flags=re.I)
    if len(s) == 0:
        return None
    return s


def _find_var(soup: Tag, name: str):
    re_var = re.compile(r"\b" + re.escape(name) +r"\s*=\s*['\"](.*?)['\"]")
    for s in map(get_text, soup.select("script")):
        m = re_var.search(s or "")
        if m:
            v = m.group(1).strip()
            if len(v):
                return v


def _re_parse(obj):
    if not isinstance(obj, dict):
        return obj
    for k in (
        "product_price",
        "member_price",
        "event_start_day",
        "product_price_without_tax",
        "member_price_without_tax",
        "reduced_price",
        "reduced_price_without_tax",
        "qty_hours_part",
        "qty_teaching_units",
        "qty_teaching_units_by_session",
        "product_price_purchase",
        "donation_min_amount",
        "code_account_income",
        "code_account_discount",
        "code_account_refund",
        "session_hours_qty"
    ):
        v = obj.get(k)
        if isinstance(v, str):
            try:
                f = float(v)
            except:
                raise ValueError(f"{k}={v}")
            i = int(f)
            obj[k] = i if f == i else f
    for k in (
        "is_class_book",
        "is_exercise_book",
        "manageable",
        "donation_allow",
        "locked",
        "only_sold_with_product",
        "active"
    ):
        v = obj.get(k)
        if isinstance(v, str):
            if v not in ("Y", "N"):
                raise ValueError(f"{k}={v}")
            obj[k] = v =="Y"
    cp = obj.get("event_location_zipcode")
    if isinstance(cp, str):
        obj["event_location_zipcode"] = int(cp.strip().split()[0])
    for k, v in list(obj.items()):
        if v in ("0000-00-00 00:00:00", "<TEXTFORMAT></TEXTFORMAT>"):
            del obj[k]
    return obj


class InstitutoFrances:
    ROOT = "https://madrid.extranet-aec.com/"
    SEARCH = "https://ifespagne.aec.app/api/public/core/v1/events/availableEventsToday"
    
    @cached_property
    def __w(self) -> Web:
        w = Web()
        w.s.headers.update({
            "Accept-Encoding": "gzip, deflate",
        })
        soup = w.get("https://madrid.extranet-aec.com/events/view/16-ENTRADACULTURAL#/")
        w.s.headers.update({
            "API_KEY": _find_var(soup, "aecExtranetWebAppsAPIKey")
        })
        return w
    
    def __search(self, params: dict):
        return self.__w.json(InstitutoFrances.SEARCH+"?"+urlencode(params))
    
    @Cache("rec/ifrances/items.json")
    def get_items(self):
        js:list[dict] = self.__search({
            "CURRENT_LANG": "es_ES",
            "EXTRANET_URL": "madrid.extranet-aec.com",
            "eventType": 16,
            "etablishmentBranchId": 0,
        })
        js = parse_obj(
            js,
            compact=True,
            re_parse=_re_parse
        )
        arr: list[dict] = []
        for i in js:
            if not self.__is_madrid(i):
                continue
            r = i.get('reference')
            if r is not None:
                i['reference'] = int(r)
            i["event_url"] = urljoin(InstitutoFrances.ROOT,  i["event_url"])
            iid = i.get("image_resource_id")
            ixt = i.get("image_extension")
            if not None in (iid, ixt):
                i['image_url'] = f"https://res.cloudinary.com/aec/image/upload/{iid}.{ixt}"
                del i['image_resource_id']
                del i['image_extension']
            arr.append(i)
        return arr
    
    def __is_madrid(self, i: dict):
        cp = i.get("event_location_zipcode")
        if cp is not None and (cp // 1000) != 28:
            return False
        for k in (
            "event_location",
            "event_location_indication"
        ):
            v = i.get(k)
            if v is None:
                continue
            if re.search(r"\bMadrid\b", v):
                return True
            if re_or(
                v,
                "Barcelona",
                "Bilbao"
            ):
                return False
        return True
        
    @cached_property
    @TupleCache("rec/ifrances.json", builder=Event.build)
    def events(self):
        logger.info("Instituto francés: Buscando eventos")
        evs: set[Event] = set()
        for i in self.get_items():
            name = _clean_name(i['product_name'])
            if re.search(r"\ben franc[eé]s$", name, flags=re.I):
                continue
            duration, sessions = self.__find_duration_sessions(i)
            e = Event(
                id=f"if{i['IDPRODUCT']}",
                url=i['event_url'],
                name=name,
                img=i.get('image_url'),
                price=self.__find_price(i),
                category=self.__find_category(i),
                place=self.__find_location(i),
                duration=duration,
                sessions=sessions,
                cycle=None
            )
            evs.add(e)
        logger.info(f"Instituto francés: Buscando eventos = {len(evs)}")
        return tuple(sorted(evs))
    
    def __find_duration_sessions(self, i: dict):
        duration = _max(
            i,
            "qty_minutes",
            "qty_minutes_by_session",
            "teaching_unit_duration_in_minutes",
        )
        st = datetime(*map(int, re.findall(r"\d+", i['event_date'])))
        if duration is None:
            logger.warning(f"NOT FOUND duration {i['event_url']}")
            duration = 0
        return duration, (Session(date=st.strftime("%Y-%m-%d %H:%M")), )


    def __find_location(self, i: dict):
        lc = i.get("event_location")
        ln = i.get("event_location_name")
        la = i.get("event_location_address")
        lu = i.get("event_location_url")
        #lz = i.get("event_location_zipcode")
        li = i.get("event_location_indication")
        ll = None
        if ln in (
            "all_etablishment_branchs",
        ):
            ln = None
        if re.match(r"^https?://(maps\.app\.goo\.gl|(www\.)?google.com)/\.*", lu or "", flags=re.I):
            m = re.search(r"(\+|\-)?\d+\.\d+\s*,\s*(\+|\-)?\d+\.\d+", lu)
            if m:
                ll = m.group()
        else:
            lu = None
        return Place(
            name=ln or lc or li or la,
            address=la,
            latlon=ll,
            map=lu
        ).normalize()

    def __find_category(self, i: dict):
        name = i['product_name']
        for n in re.split(r"\s+\|\s+", name):
            if re_or(
                n,
                "infantil",
                flags=re.I
            ):
                return Category.CHILDISH
            if re_or(
                n,
                "cine",
                flags=re.I
            ):
                return Category.CINEMA
            if re_or(
                n,
                "exposici[oó]n",
                flags=re.I
            ):
                return Category.EXPO
            if re_or(
                n,
                "concierto",
                flags=re.I
            ):
                return Category.MUSIC
        logger.warning(str(CategoryUnknown(i['event_url'], name)))
        return Category.UNKNOWN


    def __find_price(self, i:dict):
        prcs = _max(
            i,
            "product_price",
            "product_price_without_tax",
            "member_price",
            "member_price_without_tax",
            "reduced_price",
            "reduced_price_without_tax",
        )
        if prcs is not None:
            return prcs

        logger.warning(f"NOT FOUND price {i['event_url']}")
        return 0
        

if __name__ == "__main__":
    i = InstitutoFrances()
    print(len(i.events))
