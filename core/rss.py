import rfeed
from datetime import datetime
from textwrap import dedent
from xml.dom.minidom import parseString as parseXml
import re
import os
from typing import List

from .event import Event

re_last_modified = re.compile(
    r'^\s*<lastBuildDate>[^>]+</lastBuildDate>\s*$',
    flags=re.MULTILINE
)
NOW = datetime.now()


class EventosRss:
    def __init__(self, destino, root: str, eventos: List[Event]):
        self.root = root
        self.eventos = eventos
        self.destino = destino

    def save(self, out: str):
        feed = rfeed.Feed(
            title="Eventos a 5€ o menos",
            link=self.root+'/'+out,
            description="Eventos a 5€ o menos",
            language="es-ES",
            lastBuildDate=NOW,
            items=list(self.iter_items())
        )

        destino = self.destino + out
        directorio = os.path.dirname(destino)

        if not os.path.exists(directorio):
            os.makedirs(directorio)

        rss = self.__get_rss(feed)
        if self.__is_changed(destino, rss):
            with open(destino, "w") as f:
                f.write(rss)

    def __is_changed(self, destino, new_rss):
        if not os.path.isfile(destino):
            return True
        with open(destino, "r") as f:
            old_rss = f.read()
        new_rss = re_last_modified.sub("", new_rss)
        old_rss = re_last_modified.sub("", old_rss)
        if old_rss == new_rss:
            return False
        return True

    def __get_rss(self, feed: rfeed.Feed):
        def bkline(s: str, i: int):
            return s.split("\n", 1)[i]
        rss = feed.rss()
        dom = parseXml(rss)
        prt = dom.toprettyxml()
        rss = bkline(rss, 0)+'\n'+bkline(prt, 1)
        return rss

    def iter_items(self):
        for e in self.eventos:
            link = e.url or e.sessions[0].url
            yield rfeed.Item(
                title=f'{e.name}',
                link=link,
                guid=rfeed.Guid(link),
                categories=rfeed.Category(str(e.category)),
                description=dedent(f'''
                    {int(round(e.price))}€ {e.category},
                    <a href="{e.place.url}">{e.place.name} ({e.place.address})</a>
                ''').strip().replace("\n", "<br/>"),
                pubDate=NOW
            )
