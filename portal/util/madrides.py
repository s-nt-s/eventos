from core.web import WEB, get_text
from core.util import get_a_href, get_domain
from functools import cache
import re

KO_MORE = (
    None,
    'https://www.semanacienciamadrid.org/',
    'https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/Actividades-en-el-Centro-Dotacional-Integrado-Arganzuela-Angel-del-Rio/?vgnextfmt=default&vgnextoid=0758c4a248991910VgnVCM2000001f4a900aRCRD&vgnextchannel=ca9671ee4a9eb410VgnVCM100000171f5a0aRCRD',
    'https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/Actividades-en-el-Centro-Sociocultural-Oporto/?vgnextfmt=default&vgnextoid=e990f36edd371910VgnVCM2000001f4a900aRCRD&vgnextchannel=ca9671ee4a9eb410VgnVCM100000171f5a0aRCRD',
    'https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/Actividades-en-el-Centro-Cultural-Casa-del-Reloj/?vgnextfmt=default&vgnextoid=b8ce2420dc891910VgnVCM1000001d4a900aRCRD&vgnextchannel=ca9671ee4a9eb410VgnVCM100000171f5a0aRCRD',
    'https://www.madrid.es/portales/munimadrid/es/Inicio/Actualidad/Actividades-y-eventos/Actividades-en-el-Centro-Cultural-Fernando-Lazaro-Carreter/?vgnextfmt=default&vgnextoid=25bff36edd371910VgnVCM2000001f4a900aRCRD&vgnextchannel=ca9671ee4a9eb410VgnVCM100000171f5a0aRCRD',
    'imccwem.munimadrid.es'
)


def isOkMore(url: str):
    if url in KO_MORE:
        return False
    dom = get_domain(url)
    if dom in KO_MORE:
        return False
    if dom != "madrid.es":
        return True
    tt = get_text(WEB.get_cached_soup(url).select_one("title"))
    if not isinstance(tt, str):
        return False
    if re.search(r"^Actividades en |Actividades( de .*)? en el Distrito .*", tt):
        return False
    return True


@cache
def find_more_url(url: str):
    href = None
    soup = WEB.get_cached_soup(url)
    h4 = soup.find('h4', string='Amplíe información')
    if h4 is not None:
        href = get_a_href(h4.find_next('a'))
        if isOkMore(href):
            return href
    link_more = ['Para más información del evento', 'Más información']
    while href is None and len(link_more) > 0:
        link = link_more.pop(0)
        for lk in (link, link+'.'):
            href = get_a_href(soup.find('a', string=lk))
            if isOkMore(href):
                return href
    for a in soup.select('div.tramites-content div.tiny-text a'):
        href = get_a_href(a)
        if isOkMore(href) and get_domain(href) != "madrid.es":
            return href
