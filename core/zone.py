from typing import NamedTuple, Union
from enum import Enum
from .util import getKm


class Circle(NamedTuple):
    lat: float
    lon: float
    kms: float

    def get_km(self, lat: float, lon: float):
        return getKm(self.lat, self.lon, lat, lon)

    def is_in(self, lat: float, lon: float):
        return self.get_km(lat, lon) <= self.kms


class Zone(NamedTuple):
    name: str
    area: tuple[Circle, ...]

    def get_km(self, lat: float, lon: float) -> float:
        km = None
        for c in self.area:
            aux = c.get_km(lat, lon)
            if km is None or km > aux:
                km = aux
        return km

    def is_in(self, lat: float, lon: float):
        for c in self.area:
            if c.is_in(lat, lon):
                return True
        return False

    @classmethod
    def build(cls, name: str, *area: Union[Circle, "Circles"]):
        areas: list[Circle] = []
        for a in area:
            if isinstance(a, Circles):
                a = a.value
            if not isinstance(a, Circle):
                raise ValueError(a)
            if a not in areas:
                areas.append(a)
        return cls(
            name=name,
            area=tuple(areas)
        )


class Circles(Enum):
    CENTRO_SOL = Circle(lat=40.416776435516745, lon=-3.7033224277568415, kms=2)
    LEGAZPI = Circle(lat=40.391225, lon=-3.695124, kms=2)
    BANCO_ESPANA = Circle(lat=40.419529, lon=-3.693949, kms=3)
    MONCLOA = Circle(lat=40.434616, lon=-3.719097, kms=1)
    PACIFICO = Circle(lat=40.401874, lon=-3.674703, kms=1)
    SAINZ_BARANDA = Circle(lat=40.414912, lon=-3.669639, kms=1)
    VILLAVERDE_BAJO = Circle(lat=40.352672, lon=-3.684576, kms=1)
    OPORTO = Circle(lat=40.388966, lon=-3.731448, kms=1)
    VISTA_ALEGRE = Circle(lat=40.388721, lon=-3.739912, kms=1)
    TRIBUNAL = Circle(lat=40.42643799145984, lon=-3.7012786845904095, kms=0.5)
    SAN_ISIDRO = Circle(lat=40.41271801132734, lon=-3.7073444235919695, kms=0.5)
    LAVAPIES = Circle(lat=40.40897556386815, lon=-3.7010840545616155, kms=0.3)
    DELICIAS = Circle(lat=40.40006636655174, lon=-3.6939322883846866, kms=0.5)


class Zones(Enum):
    LEGAZPI = Zone.build(
        "Legazpi",
        Circles.LEGAZPI
    )
    DELICIAS = Zone.build(
        "Delicias",
        Circles.DELICIAS
    )
    BANCO_ESPANA = Zone.build(
        "Banco España",
        Circles.BANCO_ESPANA
    )
    MONCLOA = Zone.build(
        "Moncloa",
        Circles.MONCLOA
    )
    PACIFICO = Zone.build(
        "Pacifico",
        Circles.PACIFICO
    )
    SAINZ_BARANDA = Zone.build(
        "Sainz de Baranda",
        Circles.SAINZ_BARANDA
    )
    VILLAVERDE_BAJO = Zone.build(
        "Villaverde bajo",
        Circles.VILLAVERDE_BAJO
    )
    CARABANCHEL = Zone.build(
        "Carabanchel",
        Circles.OPORTO,
        Circles.VISTA_ALEGRE
    )
    TRIBUNAL = Zone.build(
        "Tribunal",
        Circles.TRIBUNAL
    )
    LA_LATINA = Zone.build(
        "La Latina",
        Circles.SAN_ISIDRO
    )
    LAVAPIES = Zone.build(
        "Lavapies",
        Circles.LAVAPIES
    )
