from core.web import WEB
from core.util import get_a_href
from functools import cache

KO_MORE = (
    None,
)

@cache
def find_more_url(url: str):
    soup = WEB.get_cached_soup(url)
    href = get_a_href(soup.select_one("a.c-mod-file-event__content-link"))
    if href and href not in KO_MORE:
        return href
