from core.web import WEB
from core.util import get_a_href, get_domain
from functools import cache


@cache
def find_more_url(url: str):
    href = None
    soup = WEB.get_cached_soup(url)
    h4 = soup.find('h4', string='Amplíe información')
    if h4 is not None:
        href = get_a_href(h4.find_next('a'))
        if href:
            return href
    link_more = ['Para más información del evento', 'Más información']
    while href is None and len(link_more) > 0:
        link = link_more.pop(0)
        for lk in (link, link+'.'):
            href = get_a_href(soup.find('a', string=lk))
            if href:
                return href
    for a in soup.select('div.tramites-content div.tiny-text a'):
        href = get_a_href(a)
        if href and get_domain(href) != "madrid.es":
            return href
