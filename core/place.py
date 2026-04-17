from dataclasses import dataclass, asdict, fields, replace
from core.util import get_obj, plain_text, re_or, re_and
from core.util.strng import capitalize
from urllib.parse import quote
import logging
import re

from core.zone import Zones
from enum import Enum

logger = logging.getLogger(__name__)


def find_cp(s: str):
    if s is None:
        return None
    cp: set[int] = set()
    for c in map(int, re.findall(r"\b(28\d+)", s)):
        if c <= 28999:
            cp.add(c)
    if len(cp) == 1:
        return cp.pop()


def safe_lt(a: str | None, b: str | None):
    if (a, b) == (None, None):
        return None
    if a is None and b is not None:
        return True
    if a is not None and b is None:
        return False
    if a.__eq__(b):
        return None
    return a.__lt__(b)


@dataclass(frozen=True)
class Place:
    name: str
    address: str
    latlon: str = None
    zone: str = None
    map: str = None

    def merge(self, **kwargs):
        return replace(self, **kwargs)

    def _asdict(self):
        return asdict(self)

    def __lt__(self, o):
        if not isinstance(o, Place):
            return NotImplemented
        for lt in (
            safe_lt(self.zone, o.zone),
            safe_lt(self.name, o.name),
            safe_lt(self.address, o.address),
            safe_lt(self.latlon, o.latlon),
        ):
            if lt is not None:
                return lt
        return False

    @classmethod
    def build(cls, *args, **kwargs):
        obj = get_obj(*args, **kwargs)
        if obj is None:
            return None
        ks = set(f.name for f in fields(cls))
        obj = {k: v for k, v in obj.items() if k in ks}
        return Place(**obj)

    def __post_init__(self):
        for f in fields(self):
            v = getattr(self, f.name, None)
            if isinstance(v, list):
                v = tuple(v)
            elif isinstance(v, str):
                v = v.strip()
                if len(v) == 0 and f.name != 'zone':
                    v = None
            if f.name == "name":
                v = capitalize(v)
            object.__setattr__(self, f.name, v)
        self.__fix()

    def __fix(self):
        doit = True
        while doit:
            doit = False
            for f in fields(self):
                if self._fix_field(f.name):
                    doit = True

    def _fix_field(self, name: str, fnc=None):
        old_val = getattr(self, name, None)
        if fnc is None:
            fnc = getattr(self, f'_fix_{name}', None)
        if fnc is None or not callable(fnc):
            return False
        fix_val = fnc()
        if fix_val == old_val:
            return False
        object.__setattr__(self, name, fix_val)
        return True

    @property
    def url(self):
        if self.map is not None:
            return self.map
        if self.latlon is not None:
            return "https://www.google.com/maps?q=" + self.latlon
        if self.address is None:
            return "#"
        if re.match(r"^[\d\.,]+$", self.address):
            return "https://www.google.com/maps?q=" + self.address
        return "https://www.google.com/maps/place/" + quote(self.address)

    def _fix_name(self):
        if self.name:
            name = re.sub(r"^Biblioteca P[uú]blica( Municipal)?", "Biblioteca", self.name, flags=re.I)
            return name

    def _fix_zone(self):
        if self.zone is not None:
            return self.zone
        name = plain_text(self.name) or ''
        addr = plain_text(self.address) or ''
        if re_or(
            name,
            r"d?el retiro",
            ("biblioteca", "eugenio trias"),
            "casa de vacas",
            r"jardin(es)? del?\b.*\bretiro\b",
            flags=re.I
        ):
            return "El Retiro"
        if re_or(
            name,
            r"Parque\b.*\bEnrique Tierno Galv[aá]n",
            "matadero",
            "cineteca",
            "Casa del Reloj",
            "Nave Terneras",
            "La Lonja",
            flags=re.I
        ):
            return Zones.LEGAZPI.value.name
        if re_or(
            name,
            "Biblioteca.* Gerardo Diego",
            flags=re.I
        ):
            return Zones.VALLECAS.value.name
        if re_and(addr, "conde duque", "28015"):
            return "Plaza España"
        if re_or(
            name,
            "clara del rey",
            "museo abc",
            flags=re.I
        ):
            return "Plaza España"
        if re_or(
            name,
            "jardines del campo del moro",
            flags=re.I
        ):
            return Zones.SOL.value.name
        if re_or(
            name,
            "Centro cultural.*Dao[ií]z y Velarde",
            flags=re.I
        ):
            return Zones.PACIFICO.value.name
        if self.latlon:
            lat, lon = map(float, self.latlon.split(","))
            for zn in (
                Zones.CARABANCHEL,
                Zones.VILLAVERDE_BAJO,
                Zones.PACIFICO,
                Zones.TRIBUNAL,
                Zones.MONCLOA,
                Zones.SOL,
                Zones.PUERTA_TOLEDO,
                Zones.LAVAPIES,
                Zones.LEGAZPI,
                Zones.MARQUES_DE_VADILLO,
                Zones.USERA,
                Zones.VALLECAS,
                Zones.MANUEL_BECERRA,
                Zones.NUNEZ_BOLBOA,
                Zones.ALCALA_DE_HENARES,
                Zones.AV_AMERICA,
                Zones.COMPLUTENSE,
            ):
                z = zn.value
                if z.is_in(lat, lon):
                    return z.name
        if re_or(
            self.address,
            r"Av(\.|enida)? Complutense",
            flags=re.I
        ):
            return Zones.COMPLUTENSE.value.name
        if re_or(
            self.address,
            "parque de san Isidro",
            flags=re.I
        ):
            return Zones.MARQUES_DE_VADILLO.value.name
        if re_or(
            self.address,
            r"plaza jes[uí]s.*28014",
            flags=re.I
        ):
            return "Paseo del Prado"
        if re_or(
            self.address,
            r"Alcal[aá]( de)? Henares",
            flags=re.I
        ):
            return Zones.ALCALA_DE_HENARES.value.name
        cp = find_cp(self.address) or find_cp(self.name)
        zone = {
            #
        }.get(cp)
        if zone is not None:
            return zone
        if cp:
            logger.debug(f"NOT FOUND cp={cp}")
        return None

    def _fix_latlon(self):
        if self.latlon:
            return self.latlon
        if re_or(self.address, "Sierra (de )?Alquife,? 12", flags=re.I):
            return "40.38888553445172,-3.66665737114293"

    def normalize(self):
        name = self.name or ''
        address = self.address or ''
        if re.match(r"^Faro de (la )?Moncloa$", name, flags=re.I):
            return Places.FARO_MONCLOA.value
        if re_or(
            name,
            r"^Conde ?Duque$",
            (r"Contempor[aá]nea", r"Conde ?Duque"),
            flags=re.I
        ):
            return Places.CONDE_DUQUE.value
        if re_or(
            name,
            r"Casa [aÁ]rabe",
            flags=re.I
        ) and re_or(
            address,
            "Alcal[aá]",
            flags=re.I
        ):
            return Places.CASA_ARABE.value
        if re.match(r"^Sala Berlanga$", name, flags=re.I) and re.search(r"Andr[ée]s Mellado.*53", address, flags=re.I):
            return Places.SALA_BERLANGA.value
        if re.match(r"^Teatro Español$", name, flags=re.I):
            return Places.TEATRO_ESPANOL.value
        if re.match(r"^Teatro Circo Price$", name, flags=re.I):
            return Places.TEATRO_PRICE.value
        if re.match(r"(^Centro\s*Centro$|.*\bPalacio de Cibeles\b.*)", name, flags=re.I):
            return Places.CENTRO_CENTRO.value
        if re.search("cineteca", name, flags=re.I) and (self.latlon == Places.CINETECA.value.latlon or re_or(self.address, "Legazpi", flags=re.I)):
            return Places.CINETECA.value
        if re.search(r"\bESLA EKO\b", name, flags=re.I) or re_and(address, "[aá]nade,? 10", flags=re.I):
            return Places.EKO.value
        if re_or(
            name,
            r"FAL",
            r"Fundaci[óo]n Anselmo Lorenzo",
            flags=re.I
        ) and re_and(address, "Peñuelas", flags=re.I):
            return Places.FUNDACION_ALSELMO_LORENZO.value
        if re.search(r"auditorio francisca (martinez|Mtnez\.?) garrido", name, flags=re.I):
            return Places.AUDITORIO_FRANCISCA_MARTINEZ_GARRIDO.value
        if re.search(r"\b(CS la cheli|local de xr madrid)\b", name, flags=re.I):
            return Places.CS_LA_CHELI.value
        if re.search(r"CS[ROA]* [dD]is[ck]ordia", name) and re.search(r"Antoñita Jiménez", self.address, flags=re.I):
            return Places.CSO_DISKORDIA.value
        if re.search(r"Sala Clamores", name, flags=re.I) and re.search(r"Alburquerque.*14", address, flags=re.I):
            return Places.SALA_CLAMORES.value
        if re.search(r"casa del barrio.*carabanchel", name, flags=re.I):
            return Places.CASA_DEL_BARRIO_CARABANCHEL.value
        if re.search(r"la an[oó]nima", name, flags=re.I) and re.search(r"Embajadores.*166", address, flags=re.I):
            return Places.LA_ANONIMA.value
        if re.search(r"santander", name, flags=re.I) and re.search(r"valmojado.*291", address, flags=re.I):
            return Places.LIBRERIA_SANTANDER.value
        if re.search(r"mary read", name, flags=re.I) and re.search(r"Marqu[eé]s (de )?Toca", address, flags=re.I):
            return Places.LIBRERIA_MARY_READ.value
        if re_or(
            self.name,
            ("ateneo", "madrid"),
            ("biblioteca", "ateneo"),
            flags=re.I
        ) and re_and(
            self.address,
            "prado",
            flags=re.I
        ):
            return Places.ATENEO_MADRID.value
        if re_and(self.name, "ateneo", "maliciosa", flags=re.I) and re_and(self.address, "peñuelas", flags=re.I):
            return Places.ATENEO_MALICIOSA.value
        if re_and(self.name, "espacio", flags=re.I) and re_and(self.address, "Sierra Carbonera.* 32", flags=re.I):
            return Places.ESPACIO.value
        if re_and(self.name, "templo", "debod", flags=re.I) and re_and(self.address, "ferraz", flags=re.I):
            return Places.DEBOD.value
        if re_or(self.name, "Biblioteca David Gistau", "Centro cultural Buenavista", flags=re.I) and re_and(self.address, "toreros,? 5", flags=re.I):
            return Places.BUENAVISTA.value
        if re_or(self.name, "MakeSpace", ("Make", "Space"), flags=re.I) and re_and(self.address, "ruiz palacios,? 7", flags=re.I):
            return Places.MAKESPACE.value
        if re_or(self.name, "ASOCIACI[Óo]N GALEGA CORREDOR DO HENARES", flags=re.I) and re_and(self.address, "28806", flags=re.I):
            return Places.ALCALA_HENARES_GALEGA.value
        if re.search(r"CS[ROA]* [lL]a [rR]osa", name) and re.search(r"bastero", self.address, flags=re.I):
            return Places.CSO_ROSA.value
        if re_and(self.address, r"CNT", "embajadores", flags=re.I):
            return Places.CNT_EMBAJADORES.value
        if re_and(self.name, "museo", "prado", flags=re.I):
            return Places.MUSEO_PRADO.value
        if re_and(self.name, "demo", "Swing", "Lab", flags=re.I) and re_and(self.address, "Magdalena", flags=re.I):
            return Places.SWING_LAB.value
        if re_and(address, r"Dr\. Fourquet,? 18", "28012", flags=re.I):
            return Places.ARCHIVO_ARKHE.value
        if re_or(name, "tu patio", flags=re.I) and re_and(address, "Eduardo Marquina", flags=re.I):
            return Places.TU_PATIO.value
        if re_and(name, "csoa?", "enredadera", flags=re.I) and re_and(address, "coruña", flags=re.I):
            return Places.CSO_LA_ENREDADERA.value
        if re_or(name, "AVA", ("[aA]socaci[oó]n", "[vV]ecinal", "[aA]luche")) and re_and(address, "quero", flags=re.I):
            return Places.AVA.value
        if re_or(name, r"Serrer[ií]a Belga", flags=re.I) and re_and(address, r"(calle\s*)?alameda", flags=re.I):
            return Places.SERRERIA_BELGA.value
        if re_or(name, r"3\s*peces?\s*3", flags=re.I) and re_and(address, "peces", flags=re.I):
            return Places.TRES_PECES_TRES.value
        if re_and(name, ("ucm", "complutense"), "ciencias", "informaci[oó]", flags=re.I) and re_and(address, r"complutense", flags=re.I):
            return Places.UCM_CIENCIAS_INFORMACION.value
        if re_and(name, "catedral", "la almudena", flags=re.I):
            return Places.LA_ALMUDENA.value
        if re_and(name, "teatro", "monumental", flags=re.I) and re_and(address, "atocha", flags=re.I):
            return Places.TEATRO_MONUMENTAL.value
        if re_or(
            name,
            r"librer[ií]a parent\(?h\)?esis",
            flags=re.I
        ) and re_and(
            address,
            "valencia",
            flags=re.I
        ):
            return Places.LIBRERIA_PARENTHESIS.value
        if re_or(
            name,
            r"instituto? franc[eé]s",
            r"instituto? français",
            "Galerie du 10",
            flags=re.I
        ):
            return Places.INSTITUTO_FRANCES.value
        if re_or(
            name,
            "c[íi]rculo de bellas artes",
            flags=re.I
        ):
            return Places.CIRCULO_BELLAS_ARTES.value
        if re_or(
            name,
            "casa del lector",
            flags=re.I
        ):
            return Places.CASA_DEL_LECTOR.value

        if re_or(
            name,
            "Marimala de Lavapi[eé]s",
            flags=re.I
        ):
            return Places.LA_MARIMALA.value
        if re_or(
            name,
            "cornisa",
            flags=re.I
        ) and re_or(
            address,
            "Lepanto",
            flags=re.I
        ):
            return Places.AV_CORNISA.value
        if re_or(
            name,
            "villana de vallekas",
            flags=re.I
        ) and re_or(
            address,
            "Sierra de alquife",
            flags=re.I
        ):
            return Places.VILLANA_VALLEKAS.value
        if re_or(
            name,
            "PCE",
            flags=re.I
        ) and re_or(
            address,
            "mart[íi]n de vargas",
            flags=re.I
        ):
            return Places.PCE_MADRID.value
        if re_or(
            name,
            "sin tarima",
            flags=re.I
        ) and re_or(
            address,
            "magdalena",
            flags=re.I
        ):
            return Places.SIN_TARIMA.value
        if re_or(
            name,
            "teatro (del )?barrio",
            flags=re.I
        ) and re_or(
            address,
            "zurita",
            flags=re.I
        ):
            return Places.TEATRO_BARRIO.value
        if re_or(
            name,
            "Plaza Puerto Rubio",
            flags=re.I
        ):
            return Places.PLAZA_PUERTO_RUBIO.value
        if re_or(
            name,
            "Museo de Am[eé]rica",
            flags=re.I
        ) and re_or(
            address,
            "reyes cat[óo]licos",
            flags=re.I
        ):
            return Places.MUSEO_AMERICA.value
        if re_or(
            name,
            "espacio afro",
            flags=re.I
        ) and (not address or re_or(
            address,
            "c[aá]ceres",
            flags=re.I
        )):
            return Places.ESPACIO_AFRO.value
        if re_or(
            name,
            "ateneo villaverde villaverde",
            flags=re.I
        ) and (not address or re_or(
            address,
            "alberto palacios",
            flags=re.I
        )):
            return Places.ATENEO_VILLAVERDE.value

        if re_or(
            f"{name} {address}",
            ("vallekas", "Piketa"),
            flags=re.I,
        ):
            return Places.CS_PILEKA.value
        for plc in Places:
            p = plc.value
            if (p.name, p.address) == (self.name, self.address):
                return p
            if (p.name, p.latlon) == (self.name, self.latlon):
                return p
        return self


class Places(Enum):
    CASA_MEXICO = Place(
        name="Casa Mexico",
        address="C. de Alberto Aguilera, 20, Chamberí, 28015 Madrid",
        latlon="40.430223201367404,-3.709325557672713",
        zone='Moncloa',
        map="https://maps.app.goo.gl/pYMaJnYZFpK8rm8n8",
    )
    ACADEMIA_CINE = Place(
        name="Academia de cine",
        address="C/ de Zurbano, 3, Chamberí, 28010 Madrid",
        latlon="40.427566448169316,-3.6939387798888634",
        zone='Alonso Martinez',
        map="https://maps.app.goo.gl/qV55n7KZ4fXNCg8dA",
    )
    MUSEO_PRADO = Place(
        name="Museo del Prado",
        address="Paseo del Prado s/n, 28014 Madrid",
        latlon="40.41391229422596,-3.692084176021338",
        zone='Paseo del Pardo',
        map="https://maps.app.goo.gl/LUZCvz8ifCcisn17A"
    )
    MUSEO_REINA_SOFIA = Place(
        name="Museo Reina Sofia",
        address="C. de Sta. Isabel, 52, Centro, 28012 Madrid",
        latlon="40.40805112459524,-3.694589081934405",
        zone='Paseo del Pardo',
        map="https://maps.app.goo.gl/MhKbtxWwpiWCisBN6"
    )
    CAIXA_FORUM = Place(
        name="Caixa Forum",
        address="Paseo del Prado, 36, Centro, 28014 Madrid",
        latlon="40.41134208472603,-3.6935713500263523",
        zone='Paseo del Pardo',
        map="https://maps.app.goo.gl/kNnSrfU2sygXNXBp8"
    )
    CASA_AMERICA = Place(
        name="Casa America",
        address="Plaza Cibeles, s/n, Salamanca, 28014 Madrid",
        latlon="40.419580635299525,-3.693332407512017",
        zone='Paseo del Pardo',
        map="https://maps.app.goo.gl/Zo6QR5VWzUbCSvZt5"
    )
    CASA_ENCENDIDA = Place(
        name="Casa encendida",
        address="Rda. de Valencia, 2, Centro, 28012 Madrid",
        latlon="40.4062337055155,-3.6999346068731525",
        zone='Lavapiés',
        map="https://maps.app.goo.gl/RoXc2KPwvFygSPrj7"
    )
    CIRCULO_BELLAS_ARTES = Place(
        name="Circulo de Bellas Artes",
        address="C/ Alcalá, 42, Centro, 28014 Madrid, España",
        latlon="40.4183042,-3.6991136",
        zone='Sol',
        map="https://maps.app.goo.gl/9hybjJzL1BjTzF2a9"
    )
    DORE = Place(
        name="Cine Doré",
        address="C/ de Santa Isabel, 3, Centro, 28012 Madrid",
        latlon="40.411950735826316,-3.699066276358703",
        zone='Sol',
        map="https://maps.app.goo.gl/nCkcBhPr7YmpJYMM6"
    )
    SALA_BERLANGA = Place(
        name="Sala Berlanga",
        address="C/ de Andrés Mellado, 53, Chamberí, 28015 Madrid",
        latlon="40.436106653741795,-3.714403054648641",
        zone='Moncloa',
        map="https://maps.app.goo.gl/o92bjiD1dYErb8zd7"
    )
    SALA_EQUIS = Place(
        name="Sala Equis",
        address="C/ del Duque de Alba, 4, Centro, 28012 Madrid, España",
        latlon="40.412126715926796,-3.7059047815506396",
        zone='Sol',
        map="https://maps.app.goo.gl/31Eh8CZjSFUUVab36"
    )
    FUNDACION_TELEFONICA = Place(
        name="Fundación Telefónica",
        address="C/ Fuencarral, 3, Centro, 28004 Madrid",
        latlon="40.42058956643586,-3.7017498812379235",
        zone='Sol',
        map="https://maps.app.goo.gl/s9szJmrimSz7As6J7"
    )
    TEATRO_ESPANOL = Place(
        name="Teatro Español",
        address="C/ del Príncipe, 25, Centro, 28012 Madrid",
        latlon="40.414828532240946,-3.700164949543688",
        zone='Sol',
        map="https://maps.app.goo.gl/VRFfsrbjz4oiDxg3A"
    )
    TEATRO_PRICE = Place(
        name="Teatro Circo Price",
        address="Ronda de Atocha, 35. 28012 Madrid",
        latlon="40.40596936645757,-3.698589986849812",
        zone='Lavapiés',
        map="https://maps.app.goo.gl/5PwdoH8Qfqxm89Lf9"
    )
    CENTRO_CENTRO = Place(
        name="Centro Centro",
        address="Pl. Cibeles, 1, Retiro, 28014 Madrid",
        latlon="40.41902261618159,-3.692188193693138",
        zone='Paseo del Pardo',
        map="https://maps.app.goo.gl/LEAyN5ATZFz5a9Mn9"
    )
    CINETECA = Place(
        name="Cineteca",
        address="Pl. de Legazpi, 8, Arganzuela, 28045 Madrid",
        latlon="40.39130985242181,-3.6958028442054074",
        zone='Legazpi',
        map="https://maps.app.goo.gl/HqyYqsb1ErsJH5yM6"
    )
    CONDE_DUQUE = Place(
        name="Conde Duque",
        address="C/ del Conde Duque, 11, 28015 Madrid",
        latlon="40.42739911262292,-3.710589286287491",
        map="https://maps.app.goo.gl/beRYjJhuqpbGF1rj8"
    )
    FARO_MONCLOA = Place(
        name="Faro de Moncloa",
        address="Av. de la Memoria, 2, 28040 Madrid",
        latlon="40.43727075977316,-3.721682694006853",
        zone='Moncloa',
        map="https://maps.app.goo.gl/UdfGgSc7WRUMvWjR8"
    )
    TEATRO_MONUMENTAL = Place(
        name="Teatro Monumental",
        address="C. de Atocha, 65, Centro, 28012 Madrid",
        latlon='40.41248873703834,-3.699161734460963',
        zone='Sol',
        map="https://maps.app.goo.gl/ueu3R2ikfwHBtN7o6"
    )
    EKO = Place(
        name="CSO EKO",
        address="C. del Ánade, 10, Carabanchel, 28019 Madrid",
        latlon="40.391899629090574,-3.7310781522792906",
        map="https://maps.app.goo.gl/CkMnFa3ph4cNGDXs7"
    )
    FUNDACION_ALSELMO_LORENZO = Place(
        name="Fundación Anselmo Lorenzo",
        address="Calle de las Peñuelas, 41, Arganzuela, 28005 Madrid",
        latlon="40.4008721991779, -3.7021363154852938",
        zone='Legazpi',
        map="https://maps.app.goo.gl/g5w42RsHsrom6h5m8"
    )
    AUDITORIO_FRANCISCA_MARTINEZ_GARRIDO = Place(
        name="Auditorio Francisca Martínez Garrido",
        address="P.º de la Chopera, 6, Arganzuela, 28045 Madrid",
        latlon="40.3948050403511,-3.7003903328011405",
        zone="Legazpi"
    )
    CS_LA_CHELI = Place(
        name="CS La Cheli",
        address="C. de la Iglesia, 12, Carabanchel, 28019 Madrid",
        latlon="40.39584448961841,-3.7177346134909293",
        zone="Marques de Vadillo",
        map="https://maps.app.goo.gl/UVDe5M5jwip47W9h6"
    )
    CS_PILEKA = Place(
        name="CS Pileka",
        address="Calle Alfredo Castro Camba, 24, 28053 Madrid",
        latlon="40.38646500445769,-3.6696186711636445",
        map="https://maps.app.goo.gl/VAqokrQHBeiKiwbo8"
    )
    CSO_DISKORDIA = Place(
        name="CSO Diskordia",
        address="C. de Antoñita Jiménez, 60, Carabanchel, 28019 Madrid",
        latlon="40.39131044903329,-3.7197457145163964",
        zone="Marques de Vadillo"
    )
    CSO_ROSA = Place(
        name="CSO la Rosa",
        address="C. del Bastero, 1, Centro, Centro, 28005 Madrid",
        latlon="40.409645939312156,-3.7096701288640967",
        zone="Sol",
        map="https://maps.app.goo.gl/aY6g8QTTuCMusmWg8"
    )
    SALA_CLAMORES = Place(
        name="Sala Clamaroes",
        address="C. de Alburquerque, 14, Chamberí, 28010 Madrid",
        latlon="40.431136283125035,-3.7008120268850164",
        zone="Tribunal",
        map="https://maps.app.goo.gl/npKXxMALBGBP3Cms8"
    )
    CASA_DEL_BARRIO_CARABANCHEL = Place(
        name="Casa del barrio",
        address="Av. de Carabanchel Alto, 64, Carabanchel, 28044 Madrid",
        latlon="40.37004495963912,-3.7534234636546335",
        zone="Carabanchel",
        map="https://maps.app.goo.gl/kJDkNzgwGF5hByqy5"
    )
    LA_ANONIMA = Place(
        name="Anónima",
        address="C. de Embajadores, 166, Arganzuela, 28045 Madrid",
        latlon="40.39618124632335,-3.696199766490811",
        zone="Legazpi",
        map="https://maps.app.goo.gl/SyVAW8MUHZ4UR9n37"
    )
    LIBRERIA_SANTANDER = Place(
        name="Librería Santander",
        address="C. de Valmojado, 291, Latina, 28047 Madrid",
        latlon="40.38681110054098,-3.7588677722869073",
        zone="Carabanchel",
        map="https://maps.app.goo.gl/3ebMHzCmxqE9Vgz17"
    )
    LIBRERIA_MARY_READ = Place(
        name="Librería Mary Read",
        address="C. del Marqués de Toca, 3, Centro, 28012 Madrid",
        latlon="40.41033677820543,-3.6960205749461768",
        zone="Paseo del Pradro",
        map="https://maps.app.goo.gl/4YzSBMzutmijk6X39"
    )
    ATENEO_MADRID = Place(
        name="Ateneo Madrid",
        address="C. del Prado, 21, Centro, 28014 Madrid",
        latlon="40.41526343432519,-3.698205767581124",
        zone="Sol",
        map="https://maps.app.goo.gl/5k8X6P1cu1vW8o8j6"
    )
    ATENEO_MALICIOSA = Place(
        name="Ateneo la maliciosa",
        address="Calle de las Peñuelas, 12, Arganzuela, 28005 Madrid",
        latlon="40.40362500123191,-3.7043296154194074",
        zone="Embajadores",
        map="https://maps.app.goo.gl/YorUQc6wj7M1fLPW6"
    )
    LIBRERIA_PARENTHESIS = Place(
        name="Librería parenthesis",
        address="C. de Valencia, 30, Centro, 28012 Madrid",
        latlon="40.40644150333828,-3.6997760460139233",
        zone="Lavapiés",
        map="https://maps.app.goo.gl/PDwE9N2BHaWAMdWq6"
    )
    ESPACIO = Place(
        name="El espacio",
        address="C/ de Sierra Carbonera, 32, Puente de Vallecas, 28053 Madrid",
        latlon="40.39225251088216,-3.6642723003364335",
        zone="Vallecas",
        map="https://maps.app.goo.gl/4s7AYFzgXoag2HJX8"
    )
    DEBOD = Place(
        name="Templo de Debod",
        address="C. de Ferraz, 1, Moncloa - Aravaca, 28008 Madrid",
        latlon="40.42442583459242,-3.7177694868554996",
        zone="Plaza España",
        map="https://maps.app.goo.gl/5S17AtYAhd7rKZZ37"
    )
    BUENAVISTA = Place(
        name="Centro cultural Buenavista",
        address="Av. de los Toreros, 5, Salamanca, 28028 Madrid",
        latlon="40.43225106824249,-3.670682203781317",
        zone="Manuel Becerra"
    )
    MAKESPACE = Place(
        name="MakeSpace",
        address="Calle Ruiz Palacios, 7, Tetuán, 28039 Madrid",
        latlon="40.46212420746715,-3.7043117038105775",
        zone="Tetuán",
        map="https://maps.app.goo.gl/ACf7PCqB52SGbbZM6"
    )
    ALCALA_HENARES_GALEGA = Place(
        name="Asociación Galega Corredor do Henares",
        address="C. de Campo Real, 1, 28806 Alcalá de Henares, Madrid",
        latlon="40.49593546319083,-3.3790130490260237",
        zone="Alcalá de Henares",
        map="https://maps.app.goo.gl/AFaqSQ6RhN8214Vh7"
    )
    CNT_EMBAJADORES = Place(
        name="CNT Embajadores",
        address="Glorieta Embajadores, 7, Arganzuela, 28012 Madrid",
        latlon="40.40454139223952,-3.702903411452709",
        zone="Embajadores",
        map="https://maps.app.goo.gl/XtYSptXMNJ2RUdiR9"
    )
    SWING_LAB = Place(
        name="Demo swing lab",
        address="Calle de la Magdalena, 7, Centro, 28012 Madrid",
        latlon="40.412636935493836,-3.702379332778133",
        zone="Sol",
        map="https://maps.app.goo.gl/3p4bartQPC2EZknV6"
    )
    ARCHIVO_ARKHE = Place(
        name="Archivo Arkhé",
        address="Calle del Dr. Fourquet, 18, planta baja, Centro, 28012 Madrid",
        latlon="40.40776391373512,-3.6975274443272483",
        zone="Lavapiés",
        map="https://maps.app.goo.gl/C6TP88rPyi9z1j3g8"
    )
    TU_PATIO = Place(
        name="Tu patio",
        address="C. de Eduardo Marquina, 7, Carabanchel, 28019 Madrid",
        latlon="40.393677424170875,-3.7124811902926353",
        zone="Marques de Vadillo",
        map="https://maps.app.goo.gl/bvHBirqG1AJqTNBD9"
    )
    CSO_LA_ENREDADERA = Place(
        name="CSO la Enredadera",
        address="C/ de la Coruña, 5, Tetuán, 28020 Madrid",
        latlon="40.45585417293838,-3.701519330690853",
        zone="Tetuan",
        map="https://maps.app.goo.gl/24aK8BHNPoQcMEPF7"
    )
    AVA = Place(
        name="Asociación vecinal Aluche",
        address="C. de Quero, 69, Latina, 28024 Madrid",
        latlon="40.39019457364059,-3.7608253422986184",
        zone="Aluche",
        map="https://maps.app.goo.gl/QSKZ7RmN2svJXwnm7"
    )
    SERRERIA_BELGA = Place(
        name="Serrería belga",
        address="C. de la Alameda, 15, Centro, 28014 Madrid",
        latlon="40.4106964292281,-3.6936188373826417",
        zone='Paseo del Pardo',
        map="https://maps.app.goo.gl/GULDeLCgPbx8oTDb7"
    )
    TRES_PECES_TRES = Place(
        name="CSA 3 peces 3",
        address="Calle de los Tres Peces, 3, Centro, 28012 Madrid",
        latlon="40.41097432843205,-3.7001688291322816",
        zone='Lavapiés',
        map="https://maps.app.goo.gl/fAs8GXQtUBLbgNSE6"
    )
    UCM_CIENCIAS_INFORMACION = Place(
        name="UCM Ciencias información",
        address="Av. Complutense, 3, 28040 Madrid",
        latlon="40.44590443938829,-3.7283811785778678",
        zone='Complutense',
        map="https://maps.app.goo.gl/YzQUkHfWsCffrmpr9"
    )
    LA_ALMUDENA = Place(
        name="Almudena",
        address="C. de Bailén, 10, Centro, 28013 Madrid",
        latlon="40.41586328949746,-3.714627123889976",
        zone='Sol',
        map="https://maps.app.goo.gl/VVVy7ALcGj93MZAH9"
    )
    GOETHE = Place(
        name="Instituto Goethe",
        address="C. de Zurbarán, 21, Chamberí, 28010 Madrid",
        latlon="40.42997093840387,-3.6915048305975926",
        zone='Alonso Martinez',
        map="https://maps.app.goo.gl/kiKtgoio8SMSzugA8"
    )
    CUARTA_PARED = Place(
        name="Teatro Cuarta Pared",
        address="C. de Ercilla, 17, Arganzuela, 28005 Madrid",
        latlon="40.40299003125452,-3.7025320269818605",
        zone='Embajadores',
        map="https://maps.app.goo.gl/zyZaf9mRqeixu7EW9"
    )
    VALLE_INCLAN = Place(
        name="Teatro Valle-Inclán",
        address="Pl. de Ana Diosdado, s/n, Centro, 28012 Madrid",
        latlon="40.40855249837478,-3.7005777883635664",
        zone='Lavapiés',
        map="https://maps.app.goo.gl/nsUhwMvC9aPR7N2P6"
    )
    MARIQUEEN = Place(
        name="Mari Queen",
        address="C. de Barbieri, 10, Centro, 28004 Madrid",
        latlon="40.4211317930352,-3.698135986509069",
        zone='Sol',
        map="https://maps.app.goo.gl/gJvWcZwQcUapc1Cs9"
    )
    REPLIKA = Place(
        name="Teatro Réplika",
        address="C. de la Explanada, 14, Moncloa - Aravaca, 28040 Madrid",
        latlon="40.44856348226549,-3.711603184725959",
        zone='Cuatro Caminos',
        map="https://maps.app.goo.gl/7K6nALjgv6GfYtWG9"
    )
    INSTITUTO_FRANCES = Place(
        name="Instituto francés",
        address="Calle del Marqués de la Ensenada, 12, Local 1, Centro, 28004 Madrid",
        latlon="40.42487366237583,-3.6923599422848086",
        map="https://maps.app.goo.gl/WtMSrpGw6HybmMGF7",
        zone='Alonso Martinez',
    )
    CASA_DEL_LECTOR = Place(
        name="Casa del lector",
        address="P.º de la Chopera, 14, Arganzuela, 28045 Madrid",
        latlon="40.39276355483462,-3.6984684576727127",
        map="https://maps.app.goo.gl/HE9kuejYm1ZmtxL56",
        zone='Legazpi',
    )
    LA_MARIMALA = Place(
        name="Marimala",
        address="Calle Provisiones 18 esquina, C. del Mesón de Paredes, 76, 28012 Madrid",
        latlon="40.40740184292837,-3.702491186509069",
        map="https://maps.app.goo.gl/uXWbXAJhKAva2Vp46",
        zone='Lavapiés',
    )
    AV_CORNISA = Place(
        name="AV Cornisa",
        address="C. Cristo de Lepanto, 9, Usera, 28026 Madrid",
        latlon="40.38093534685343,-3.7008894267081742",
        map="https://maps.app.goo.gl/QiuU4DBcChCUL37Q9",
        zone='Almendrales',
    )
    VILLANA_VALLEKAS = Place(
        name="Villana Vallekas",
        address="C. Sierra de Alquife, 12, Puente de Vallecas, 28053 Madrid",
        latlon="40.388937365812666,-3.6667314056831035",
        map="https://maps.app.goo.gl/S7obRpiZAGWKFc8S9",
        zone='Vallecas',
    )
    PCE_MADRID = Place(
        name="PCE Madrid",
        address="C. de Martín de Vargas, 46, Arganzuela, 28005 Madrid",
        latlon="40.401346703786785,-3.700752071237691",
        map="https://maps.app.goo.gl/Gs1Y38Lmi5j5s6J3A",
        zone='Legazpi',
    )
    SIN_TARIMA = Place(
        name="Sin tarima",
        address="Calle de la Magdalena, 32, Centro, 28012 Madrid",
        latlon="40.41257169833034,-3.7003979865090706",
        zone='Sol',
    )
    TEATRO_BARRIO = Place(
        name="Teatro del Barrio",
        address="C. de Zurita, 20, Centro, 28012 Madrid",
        latlon="40.40978378427696,-3.6993138845307407",
        zone='Lavapiés',
        map="https://maps.app.goo.gl/gtmoQWKw3KHAJCqB9"
    )
    PLAZA_PUERTO_RUBIO = Place(
        name="Plaza Puerto Rubio",
        address="Puente de Vallecas, 28053 Madrid",
        latlon="40.39671543801934,-3.6676335614560585",
        zone='Vallecas',
        map="https://maps.app.goo.gl/rWEYZTauTccvm8Zu7"
    )
    CASA_ARABE = Place(
        name="Casa Árabe",
        address="C. Alcalá, 62, Salamanca, 28009 Madrid",
        latlon="40.421664106756026,-3.681901515345426",
        zone='Retiro',
        map="https://maps.app.goo.gl/JVXHvG8tg5CeYWGN6"
    )
    MUSEO_AMERICA = Place(
        name="Museo de América",
        address="Av. de los Reyes Católicos, 6, Moncloa - Aravaca, 28040 Madrid",
        latlon="40.438245416724506,-3.7221641522565263",
        zone='Moncloa',
        map="https://maps.app.goo.gl/2DSchB1L2tgMVQE97"
    )
    ESPACIO_AFRO = Place(
        name="Espacio Afro",
        address="C. de Cáceres, 49, Arganzuela, 28045 Madrid",
        latlon="40.39908347733006,-3.700088539854162",
        zone='Legazpi',
        map="https://maps.app.goo.gl/AgPDHHxp8KXPSY6eA"
    )
    ATENEO_VILLAVERDE = Place(
        name="Ateneo Villaverde",
        address="P.º de Alberto Palacios, 2, Villaverde, 28021 Madrid",
        latlon="40.346850085863956,-3.7092837326225037",
        zone='Villaverde Alto'
    )
