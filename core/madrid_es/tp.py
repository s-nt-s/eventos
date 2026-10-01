from typing import NamedTuple, Optional


class Place(NamedTuple):
    location: str
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    address: Optional[str] = None
    district: Optional[str] = None
