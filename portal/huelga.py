from core.web import Web, get_text
import re
import json
from core.cache import Cache
from core.util import re_or
import logging
from portal.base import Base
from core.event import Event, Category, Session, Place
from datetime import datetime
from zoneinfo import ZoneInfo
from core.zone import Zones

re_json = re.compile(r'{\\"activities\\":(\[{\\"id\\".*?\]),\\"labels\\":{\\"title\\"')

logger = logging.getLogger(__name__)


def to_datetime(s: str):
    dt = datetime.fromisoformat(s)
    return dt.astimezone(ZoneInfo("Europe/Madrid"))


class HuelgaError(Exception):
    pass


class Huelga(Base):

    @Cache("rec/huelga.json")
    def get_items(self) -> list[dict]:
        url = "https://participa.huelgaporlavivienda.org/activities"
        soup = Web().get(url)
        items: list[dict] = []
        for txt in map(get_text, soup.select("script")):
            m = re_json.search(txt or '')
            if m is None:
                continue
            js = m.group(1)
            js = re.sub(r'\\"', '"', js)
            js = re.sub(r'\\\\', r'\\', js)
            obj = json.loads(js)
            if not isinstance(obj, list):
                continue
            for i in obj:
                if not isinstance(i, dict) or i.get("status") == "cancelled":
                    continue
                if not isinstance(i.get("id"), str):
                    continue
                if (
                    i.pop("draft"),
                    i.pop("province"),
                    i.pop("municipality"),
                ) != (False, "Madrid", "Madrid"):
                    continue
                committees = i.pop("committees", None)
                if committees is not None:
                    if not isinstance(committees, list):
                        raise HuelgaError(f"committees type={type(committees)}")
                    if len(committees) != 1:
                        raise HuelgaError(f"committees len={len(committees)}")
                    committee = committees[0]
                    if not isinstance(committee, dict):
                        raise HuelgaError(f"committees list[{type(committee)}]")
                    name = committee.get('name')
                    if not isinstance(name, str):
                        raise HuelgaError(f"committees.name type={type(name)}")
                    i['committee'] = committee
                items.append(i)
            if items:
                return items

        raise ValueError(f"data not found in {url}")

    def _get_events(self):
        evs: set[Event] = set()
        for i in self.get_items():
            if not self.__is_ok(i):
                continue
            st = to_datetime(i['starts_at'])
            en = to_datetime(i['ends_at'])
            duration = int((en-st).total_seconds() // 60)
            place = self.__get_place(i)
            if place:
                place = place.normalize()
            e = Event(
                id=i['id'],
                name=i['title'],
                url="https://participa.huelgaporlavivienda.org/activities/"+i['id'],
                price=0,
                category=Category.ACTIVISM,
                place=place,
                duration=duration,
                sessions=(
                    Session(date=st.strftime("%Y-%m-%d %H:%M")),
                ),
                description=i.get('description')
            )
            evs.add(e)
        evs = set(Event.fusionIfSimilar(
            evs,
            ('name', 'place')
        ))
        return tuple(sorted(evs))

    def __is_ok(self, i: dict):
        lat = i['latitude']
        lon = i['longitude']
        for z in (
            Zones.LEGAZPI,
            Zones.SOL,
            Zones.PUERTA_TOLEDO
        ):
            if z.value.is_in(lat, lon):
                return True
        return False

    def __get_place(self, i: dict):
        name = i['location_name']
        for k, v in {
            r"\s*,\s+28\d{3}.*$": "",
            r"Pabell[oó]n (poli)?deportivo Pabell[oó]n (poli)?Deportivo": "Pabellón Deportivo"
        }.items():
            name = re.sub(k, v, name, flags=re.I)
        p = Place(
            name=name,
            address=i['location_name'],
            latlon=f"{i['latitude']},{i['longitude']}"
        )
        return p


if __name__ == "__main__":
    h = Huelga()
    h._get_events()
