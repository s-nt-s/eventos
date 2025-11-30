from core.web import WEB
from core.util import get_a_href, get_domain
from functools import cache

KO_MORE = (
    None,
    'imccwem.munimadrid.es'
)


@cache
def find_more_url(url: str):
    soup = WEB.get_cached_soup(url)
    href = get_a_href(soup.select_one("a.c-mod-file-event__content-link"))
    dom = get_domain(href)
    if href and href not in KO_MORE and dom not in KO_MORE:
        return href
