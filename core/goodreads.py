from core.web import Web, get_text
from urllib.parse import quote
from typing import NamedTuple
import re
from functools import cache


class Book(NamedTuple):
    url: str
    title: str
    author: tuple[str, ...]
    rate: float
    reviews: int


class GoodReads:
    def __init__(self):
        self.__w = Web()

    @cache
    def __search(self, url: str):
        books: set[Book] = set()
        self.__w.get(url)
        for link in self.__w.soup.select("table.tableList a.bookTitle[href]"):
            tt = get_text(link)
            authors: list[str] = []
            td = link.find_parent("td")
            for at in map(get_text, td.select("a.authorName")):
                if at not in authors:
                    authors.append(at)
            m = re.search(r"([\d\.]+) avg rating [—\-] ([\d,]+) ratings?", get_text(td))
            book = Book(
                url=link.attrs['href'].split("-")[0],
                title=tt,
                author=tuple(authors),
                rate=float(m.group(1)),
                reviews=int(m.group(2).replace(",", ""))
            )
            books.add(book)
        rtn = tuple(sorted(books, key=lambda b: (
            -b.reviews,
            -b.rate,
            b.title,
            b.url,
            b
        )))
        return rtn

    def search(self, title: str, author: str) -> tuple[Book, ...]:
        re_dot = re.compile(r"[:,] ")
        books: list[Book] = []

        for qr in (
            f"{title} {author}",
            title,
        ):
            url = "https://www.goodreads.com/search?utf8=%E2%9C%93&query="+quote(qr)
            for b in self.__search(url):
                if len(b.author) == 0:
                    continue
                t1 = title.lower()
                t2 = b.title.lower()
                matchTitle = re_dot.sub(" ", t1) == re_dot.sub(" ", t2) or (t1.startswith(t2+": ") or t2.startswith(t1+": "))
                likeTitle = (t1 in t2) or (t2 in t1)
                if not matchTitle and not likeTitle:
                    continue
                if len(b.author) == 1:
                    a1 = b.author[0].lower()
                    a2 = author.lower()
                    if a1 == a2:
                        books.append(b)
                        continue
                    if matchTitle and ((a1 in a2) or (a2 in a1)):
                        books.append(b)
                        continue
                if not matchTitle:
                    continue
                check_author = str(author)
                for a in b.author:
                    check_author = re.sub(re.escape(a), "", check_author, flags=re.I)
                if len(check_author) < len(author):
                    books.append(b)
            if books:
                return tuple(books)
        return tuple()

    def find(self, title_author: str):
        for r in (
            re.compile(r"^'(.+)',?\s*escrito por (.+)$"),
            re.compile(r"^'(.+)',\s*de (.+)$"),
            re.compile(r"^'(.+)'\s*de (.+)$"),
            re.compile(r"^(.+),\s*de (.+)$"),
            re.compile(r"^(.+) de (.+)$"),
        ):
            m = r.match(title_author)
            if m:
                return self.search(m.group(1), m.group(2))


GR = GoodReads()

if __name__ == "__main__":
    import sys
    print(GR.find(sys.argv[1]))
