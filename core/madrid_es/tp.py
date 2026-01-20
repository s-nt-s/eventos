from typing import NamedTuple, Optional


class Place(NamedTuple):
    latitude: float
    longitude: float
    location: str
    address: str
    district: Optional[str] = None
