import re
from functools import cache

_QUOTES = (
    '""',
    "''",
    '`´',
    '‘’',
    '“”',
    '«»'
)
_QT = "".join(_QUOTES)

_SPECIAL_WORDS = (
    "María la Rica",
    "Carmen",
    "Sevilla",
    "Cervantes",
    "Alcalá",
    "Henares",
    "Antezana",
    "Santiago",
    "Complutense",
    "Mononoke",
    "IV",
    "BSMM",
    "Paco de Lucía",
    "AWWZ",
    "CSO",
    "EKO",
    "IA",
    "AI",
    "centro cultural",
    "XXX",
    "VHZ",
    "XXV",
    "Quijote",
    "Sara Torres",
    "Karelis Zambrano",
    "Carmen Rojas",
    "Shakespeare",
    "Finzi Pasca",
)

_RG_SPECIAL_WORDS = re.compile(
    r"\b(" + "|".join(map(re.escape, _SPECIAL_WORDS)) + r")\b",
    re.I
)
_RE_SPECIAL_WORDS = {x.lower(): x for x in _SPECIAL_WORDS}


def capitalize(name: str):
    if name == name.upper():
        name = name.capitalize()

    name = _RG_SPECIAL_WORDS.sub(
        lambda m: _RE_SPECIAL_WORDS[m.group(0).lower()],
        name
    )

    w1 = name[0]
    if w1.isalpha():
        name = w1.upper()+name[1:]

    return name


def normalize_quote(s: str):
    if s is None:
        return None
    bak = ''
    s = s.strip()
    while len(s) and bak != s:
        bak = str(s)
        for q in _QUOTES:
            if s.count(q) == 1:
                s = s.replace(q, "")
            if len(s) >= 2 and (s[0]+s[-1]) == q:
                s = s[1:-1]
            if len(s) >= 2:
                count = sum(map(s.count, set(q)))
                if count == 1:
                    if s[0] in q:
                        s = s[1:].strip()
                    elif s[-1] in q:
                        s = s[:-1].strip()
        s = s.strip()
        if "'" not in s:
            s = re.sub(r'['+_QT+']', "'", s)
    return s


@cache
def _rm_prefix():
    SP = r":\-\.\|"
    SEP = r"["+SP+r"]"
    PREFIX_1 = r"|".join([
        r"Concierto de piano",
        r"Tardes romanas\b[^"+SP+r"]*?",
        r"Anarkademia",
        r"Cine\s*Club\s*Goethe",
        r"📢 VALLECAS",
        r"(?:Obra de |Representaci[óo]n de )?[tT]eatro(?: para adultos| Comedia Sat[ií]rica)?",
        r"Colecci[óo]n\.? Arte contempor[aá]neo",
        r"Celebra\d+",
        r"Cap[ií]tulo XXX",
        r"CIMA (?:proyecta|Conversa)",
        r"(?:Grupo|Club) de lectura",
        r"Charla",
        r"Concierto",
        r"Ciclo de conferencias?",
        r"Magia",
        r"Cine",
        r"M[uú]sica",
        r"Semana de la Ciencia \d+",
        r"Charlas con altura",
        r"Pel[íi]cula",
        r"Visita(?: a la exposici[oó]n| comentada| guiada)?",
        r"Lectura dramatizada",
        r"Presentación del libro",
        r"Cinef[oó]rum(?:(?: de)? (?:Isabel S[aá]nchez|Esqueria))?",
        r"Madrid, plató de cine",
        r"Conferencia",
        r"Conferencia y audiovisual",
        r"Proyecci[oó]n(?: del documental| de la pel[ií]cula)?",
        r"Exposici[oó]n",
        r"Danza",
        r"Noches? de Cl[aá]sicos?",
        r"21 Distritos",
        r"Representaci[óo]n teatral",
        r"Taller",
        r"Conversaciones WAIQ",
    ])
    PREFIX_2 = r"|".join({
        r"POM Condeduque [\d\-]+",
    })
    PREFIX_3 = r"|".join({
        r".*CinePlaza:.*?> (?:Proyección|Cine)[^:]*:",
    })
    re_1 = r"(?:(?:"+PREFIX_1+r")\s*"+SEP+r"+)"
    re_2 = r"(?:(?:"+PREFIX_2+r")\s*"+SEP+r"*)"
    re_3 = r"(?:"+PREFIX_3+r")"
    rm_prefix = re.compile("^(?:" + re_1 + r"|" + re_2 + r"|" + re_3 + r")\s*", flags=re.I)
    return rm_prefix


@cache
def _rm_sufix():
    SEP = r"[\-\.\|]"
    SUFIX_1 = "|".join([
        r"(?:Actividades )?(?:viernes|s[aá]bado|domingo) (?:tarde|mañana)",
        r"Las tertulias de Eirene Editorial",
        r"Visita a la colecci[oó]n del Museo",
        r"CSO? La Cheli",
        r"Rebeli[oó]n o Extinci[oó]n",
        r"Moncloa(?:[ \-\.]+Aravaca)?",
        r"Villaverde",
        r"Centro",
        r"en el Espacio de Igualdad Lourdes Hernández",
        r"Encuentro con el p[uú]blico",
        r"[IÍ]dem",
        r"conferencia",
        r"Arganzuela",
        r"Retiro",
        r"Chamberi",
        r"Salamanca",
        r"D[ií]a Internacional del Teatro",
        r"Lectura dramatizada",
        r"Biblioteca Ana Mar[ií]a Matute",
        r"III Edici[oó]n",
    ])
    SUFIX_2 = "|".join([
        r"en el Espacio de Igualdad Lourdes Hernández",
    ])
    re_1 = r"(?:" + SEP+r"+\s*(?:"+SUFIX_1+r"))"
    re_2 = r"(?:" + SEP+r"*\s*(?:"+SUFIX_2+r"))"
    re_3 = r"(?:" + SEP+r")"
    rm_sufix = re.compile(r"\s*(?:" + re_1 + r"|" + re_2 + r"|" + re_3+ r"+)\s*$", flags=re.I)
    return rm_sufix


@cache
def _rm_quote():
    NQ = r"[^"+_QT+"]"
    PREFIX = "|".join([
        r"Concierto(?: de)?",
        r"Cineclub(?: con)?",
        r"Proyección(?: de)?",
        r"Ciclo de conferencias de la Sociedad Española de Ret[oó]rica'?",
        r"Proyecci[óo]n y coloquio",
        r"Estreno del largometraje documental",
        r"Taller(?: de)?",
    ])
    re_3 = r"(?:"+PREFIX+r")"
    re_prefix = re.compile(r"^"+re_3+NQ+r"*(["+_QT+r"])", flags=re.I)
    return re_prefix


@cache
def _sub_1():
    R = "|".join([
        r"(Matadero) (?:Madrid )?Centro de Creaci[oó]n Contempor[aá]nea",
        r"(Red de Escuelas) Municipales del Ayuntamiento de Madrid",
        r"(Piano City) (?:Madrid *'?\d+|Madrid|'?\d+)"
        r"(Asociación de Jubilados) (?:del )?Ayuntamiento(?: de Madrid)?",
        r"^[a-zA-ZáéÁÉ]+ con Historia[\.\s]+([vV]isitas guiadas tem[aá]ticas a la colecci[oó]n)[\.\s]+[a-zA-Z]+",
    ])
    re_1 = re.compile(R, flags=re.I)
    return re_1


def clean_name(name: str):
    if name is None:
        return None
    if not isinstance(name, str):
        raise ValueError(f"name must be a str, but is a {type(name)}: {name}")
    if re.search(r"Visitas? dialogadas? Matadero", name):
        return "Visita dialogada Matadero"
    bak = ['']

    while bak[-1] != name:
        bak.append(str(name))
        name = normalize_quote(name)
        name = re.sub(r"\.\.\.\s*", "… ", name).strip()
        name = _rm_prefix().sub("", name)
        name = _rm_sufix().sub("", name)
        name = _rm_quote().sub(r"\1", name)
        name = _sub_1().sub(r"\1", name)
        if name:
            name = capitalize(name)
        if len(name) < 2:
            name = bak[-1]
    w1 = name[0]
    if w1.isalpha():
        name = w1.upper()+name[1:]
    return name
