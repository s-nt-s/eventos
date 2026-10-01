from core.web import get_text, Driver, Web
from functools import cached_property
from typing import NamedTuple


class Item(NamedTuple):
    title: str
    url: str
    where: str
    when: str


class Fnac:
    AGENDA = "https://www.fnac.es/Eventos-Fnac/Proximos-eventos/csb4374/w-4?PageIndex={page}&SpaceID=38#{page}"

    def __init__(self):
        self.__w = Web()
        self.__w.s = Driver.to_session(
            "firefox",
            "https://www.fnac.es/",
        )

    @cached_property
    def items(self):
        old_size = -1
        items: list[Item] = list()
        page = 0
        while len(items) > old_size:
            old_size = len(items)
            page = page + 1
            url = Fnac.AGENDA.format(page=page)
            soup = self.__w.get(url)
            for div in soup.select("div.article"):
                sb = div.select_one("a.subtitle")
                i = Item(
                    title=get_text(sb),
                    url=sb.attrs["href"],
                    where=get_text(div.select_one("div.where a")),
                    when=get_text(div.select_one("div.when a"))
                )
                if i not in items:
                    items.append(i)
        return tuple(items)


if __name__ == "__main__":
    f = Fnac()
    print(*f.items, sep="\n")
