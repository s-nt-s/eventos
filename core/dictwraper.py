from typing import TypeVar, Type
from types import NoneType
from collections.abc import Sized
from datetime import datetime, date, timezone
from zoneinfo import ZoneInfo

TZ_ZONE = 'Europe/Madrid'


T = TypeVar("T")


class DictWraper:
    def __init__(self, obj: dict):
        self.__obj = obj

    def __str__(self):
        return str(self.__obj)

    def get(self, k: str, mandatory: bool = False):
        v = self.__obj.get(k)
        if v is None:
            if mandatory:
                raise ValueError(f"{k} is None in {self}")
            return None
        if isinstance(v, str):
            v = v.strip()
        if isinstance(v, Sized) and len(v) == 0:
            v = None
        if v is None:
            if mandatory:
                raise ValueError(f"{k} is empty in {self}")
            return None
        return v

    def __get_type(self, k: str, *tps: Type[T]) -> T:
        v = self.get(k, mandatory=(NoneType not in tps))
        if int in tps and float not in tps and isinstance(v, float) and int(v) == v:
            v = int(v)
        if int in tps and str not in tps and isinstance(v, str) and v.isdecimal():
            v = int(v)
        if not isinstance(v, tps):
            raise ValueError(f"{k} is not {tps} in {self}")
        return v

    def get_str(self, k: str):
        return self.__get_type(k, str)

    def get_str_or_none(self, k: str):
        return self.__get_type(k, str, NoneType)

    def get_bool_or_none(self, k: str):
        v = self.__get_type(k, bool, int, float, str, NoneType)
        if v is None:
            return None
        if isinstance(v, bool):
            return v
        if isinstance(v, str):
            v = v.strip().lower()
        ok = (1, 1.0, "1", "true")
        ko = (0, 0.0, "0", "false")
        if v not in (ok+ko):
            raise ValueError(f"{k} is not a bool in {self}")
        return v in ok

    def get_bool(self, k: str):
        v = self.get_bool_or_none(k)
        if v is None:
            raise ValueError(f"{k} is None in {self}")
        return v

    def get_int_or_none(self, k: str):
        return self.__get_type(k, int, NoneType)

    def get_int(self, k: str):
        return self.__get_type(k, int)

    def get_datetime_or_none(self, k: str, dt_format: str = None):
        tps = [datetime, int, NoneType]
        if dt_format:
            tps.append(str)
        v = self.__get_type(k, *tps)
        if v is None:
            return None
        if isinstance(v, int):
            v = datetime.fromtimestamp(v, tz=timezone.utc)
            v = v.astimezone(ZoneInfo("Europe/Madrid"))
        if isinstance(v, date):
            return datetime.combine(v, datetime.min.time(), tzinfo=ZoneInfo(TZ_ZONE))
        if isinstance(v, str) and dt_format:
            v = datetime.strptime(v, dt_format)
        if not isinstance(v, datetime):
            raise ValueError(f"{k} is not datetime in {self}")
        if v.tzinfo is None:
            v = v.replace(tzinfo=ZoneInfo(TZ_ZONE))
        return v

    def get_datetime(self, k: str, dt_format: str = None):
        v = self.get_datetime_or_none(k, dt_format=dt_format)
        if v is None:
            raise ValueError(f"{k} is None in {self}")
        return v

    def get_dict(self, k: str):
        return DictWraper(self.__get_type(k, dict))

    def get_dict_or_none(self, k: str):
        obj = self.__get_type(k, dict, NoneType)
        if obj is not None:
            return DictWraper(obj)

    def get_dict_or_empty(self, k: str):
        return DictWraper(self.__get_type(k, dict, NoneType) or {})

    def get_float(self, k: str):
        return self.__get_type(k, float)

    def get_float_or_none(self, k: str):
        return self.__get_type(k, float, NoneType)

    def get_list_or_none(self, k: str):
        return self.__get_type(k, list, NoneType)
