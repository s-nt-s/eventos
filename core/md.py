from markdownify import MarkdownConverter, ATX, ASTERISK, STRIP
from bs4 import Tag, BeautifulSoup
from typing import Union
import re
from markdown import markdown as md_to_html

re_sp = re.compile(r"\s+")


def safe_positive_int(s: Union[int, float, str, None], default=0):
    if isinstance(s, (int, float)):
        if s < 0:
            return default
        i = int(s)
        if i != s:
            return default
        return i
    if not isinstance(s, str):
        return default
    s = s.strip()
    if not s.isdigit():
        return default
    return int(s)


def is_decorative_img(n: Tag):
    if not isinstance(n, Tag):
        return False
    if n.name != "img":
        return False
    alt = n.attrs.get("alt")
    title = n.attrs.get("title")
    return not(alt or title)


def safe_select(table: Tag, slc: str):
    arr: list[Tag] = []
    for n in table.select(slc):
        if n.find_parent(table.name) == table:
            arr.append(n)
    return tuple(arr)

class MyMarkdownConverter(MarkdownConverter):
    def __init__(self):
        super().__init__(
            heading_style=ATX,
            bullets='*+-',
            strong_em_symbol=ASTERISK,
            sub_symbol='<sub>',
            sup_symbol='<sup>',
            escape_asterisks=True,
            escape_underscores=True,
            escape_misc=False,
            wrap=False,
            strip_document=STRIP
        )

    @property
    def __keep_inline_images_in(self) -> list[str]:
        return self.options['keep_inline_images_in']

    def convert_img(self, el: Tag, text: str, parent_tags: list[str]):
        if is_decorative_img(el):
            return ''
        if el.parent.name not in self.__keep_inline_images_in:
            self.__keep_inline_images_in.append(el.parent.name)
        return super().convert_img(el, text, parent_tags)

    def convert(self, html: Union[Tag, str]):
        if html is None:
            return None
        md = self.__convert(html)
        md = re.sub(r"\n\s*?\n\s*?\n+", "\n\n", md)
        fake_br = '$$$$NNN~~~~%%%'
        md = md.replace("\n", fake_br)
        md = re.sub(r"\s+", " ", md)
        md = md.replace(fake_br, "\n")
        md = "\n".join(map(str.rstrip, md.split("\n")))
        md = re.sub(r"\n\n\n+", "\n\n", md)
        md = md.rstrip()
        if len(md) == 0:
            return None
        return md

    def __convert(self, html: Union[Tag, str]):
        if isinstance(html, str):
            soup = BeautifulSoup(html, 'html.parser')
        elif isinstance(html, Tag):
            soup = BeautifulSoup(str(html), 'html.parser')
        else:
            raise ValueError(f"html debe ser str o bs4.Tag, no {type(html)}")
        self.replace_fake_table(soup)
        for table in soup.select("table"):
            self.compact_table(table)
        self.replace_fake_table(soup)
        return super().convert_soup(soup)

    def convert_a(self, el: Tag, text: str, parent_tags: list[str]):
        txt = re_sp.sub(r" ", el.get_text()).strip()
        if len(txt):
            href: str = re_sp.sub(r" ", (el.attrs.get("href") or '')).strip()
            if len(href) == 0 or href.startswith("#"):
                return txt
            lwhref = href.lower().rstrip("#?/ ")
            lwtxt = txt.lower().rstrip("#?/ ")
            if lwhref == f"mailto:{lwtxt}":
                return txt
            if lwhref in (lwtxt, f"http://{lwtxt}", f"https://{lwtxt}"):
                return href
        return super().convert_a(el, text, parent_tags)
    
    def convert_table(self, el: Tag, text: str, parent_tags: list[str]):
        if el.find_all(attrs={'colspan': True}) or el.find_all(attrs={'rowspan': True}):
            self.compact_table(el)
            html: str = el.prettify()
            html = re.sub(r"<(\w+)([^>]*)>\s*([^<>]*?)\s*</\1>", r"<\1\2>\3</\1>", html)
            html = html.strip()
            return '\n\n' + html + '\n\n'
        md = super().convert_table(el, text, parent_tags)
        md = re.sub(r"\|\n\n+\|", r"|\n|", md)
        md = re.sub(r"^\|( \-+ \|)+$", lambda x: x.group().replace(" ", "-"), md, flags=re.MULTILINE)
        md = re.sub(r"\n^\|[ \|]+\|$", "", md, flags=re.MULTILINE)
        md = re.sub(r"\n\n\|[\-\|]+\|\n", r"\n", md)
        md = re.sub(r"\n\|[\-\|]+\|\n\n", r"\n", md)
        return md

    def compact_table(self, table: Tag):
        bak = ''
        while str(table) != bak:
            bak = str(table)
            self.__unwarp_redundant_p(table)
            self.__extract_not_significant(table)
            self.__extract_empty_row(table)
            self.__extract_empty_col(table)
            self.__fix_thead(table)


    def replace_fake_table(self, soup: Tag):
        bak = ''
        while str(soup) != bak:
            bak = str(soup)
            self.__replace_fake_table(soup)

    def __replace_fake_table(self, soup: Tag):
        if soup.name == "table":
            for c in safe_select(soup, "caption"):
                if re_sp.sub("", c.get_text()):
                    return False
            tds = safe_select(soup, "td, th")
            if len(tds) == 1:
                td = tds[0]
                if re_sp.sub(" ", td.get_text()).strip() == re_sp.sub(" ", soup.get_text()).strip():
                    td.name = "div"
                    soup.replace_with(td)
                    return True
            trs = safe_select(soup, "tr")
            if len(trs) == 1 and not safe_select(soup, "p"):
                tr = trs[0]
                for td in tr.select(":scope > td, :scope > th"):
                    td.unwrap()
                tr.name = "div"
                soup.replace_with(tr)
                return True
            return False
        flag = False
        while True:
            tables = list(soup.select("table"))
            if len(tables) == 0:
                return flag
            for index, table in enumerate(tables, start=-len(tables)+1):
                if self.__replace_fake_table(table):
                    flag = True
                    break
                elif index == 0:
                    return flag

    def __fix_thead(self, table: Tag):
        if table.select_one("thead") is not None:
            return
        trs = safe_select(table, "tr")
        if not trs or trs[0].attrs.get("rowspan") not in (None, 1, "1"):
            return
        for tr in trs[1:]:
            if tr.select(":scope > th"):
                return
        first_tr = trs[0]
        ths = first_tr.select(":scope > th")
        tds = first_tr.select(":scope > td")
        if len(ths) == 0 or len(tds) > 0:
            return
        thead =  BeautifulSoup("<thead></thead>", 'html.parser').find()
        first_tr.extract()
        thead.append(first_tr)
        table.insert(0, thead)

    def __unwarp_redundant_p(self, tag: Tag):
        for p in tag.select("th > p, td > p, th > div, td > div, p > p"):
            if len(p.parent.select(":scope > *"))== 1 and re_sp.sub("", p.parent.get_text()) == re_sp.sub("", p.get_text()):
                p.unwrap()

    def __extract_not_significant(self, tag: Tag):
        for n in tag.select("img, colgroup, col"):
            if n.name!="img" or is_decorative_img(n):
                n.extract()
        for n in tag.select("thead, tbody"):
            if len(n.select(":scope *"))==0:
                n.extract()
        for n in [tag]+tag.select(":scope *"):
            n.attrs = {
                k:v for k,v in n.attrs.items() 
                if
                    (k in ('href', 'src', 'title', 'alt') and isinstance(v, str) and len(v.strip())>0) or
                    (k in ('rowspan', 'colspan') and safe_positive_int(v)>1)
            }
        for n in tag.select("span"):
            if len(n.attrs)==0:
                n.unwrap()

    def __extract_empty_row(self, table: Tag):
        def _get_rowspan(tr: Tag):
            rowspan = 1
            for td in tr.select(":scope > td, :scope > th"):
                rowspan = max(rowspan, safe_positive_int(td.attrs.get("rowspan"), 1))
            return rowspan
    
        rowspan = 0
        for tr in safe_select(table, "tr"):
            if rowspan == 0 and len(re_sp.sub("", tr.get_text())) == 0 and not tr.select_one("img"):
                tr.extract()
            rowspan = max(rowspan - 1, _get_rowspan(tr) - 1)

    def __extract_empty_col(self, table: Tag):
        for td in safe_select(table, "td, th"):
            if safe_positive_int(td.attrs.get("colspan"), 1) > 1:
                return
        cols_rm: list[list[Tag]] = []
        for tr in safe_select(table, "tr"):
            for i, td in enumerate(tr.select(":scope > td, :scope > th")):
                while len(cols_rm)<=i:
                    cols_rm.append([])
                cols_arr = cols_rm[i]
                if len(re_sp.sub("", td.get_text())) > 0 or td.select_one("img"):
                    cols_rm[i] = None
                elif isinstance(cols_arr, list):
                    cols_arr.append(td)
        for cols_arr in cols_rm:
            for td in (cols_arr or []):
                td.extract()

    def to_soup(self, md: str) -> BeautifulSoup:
        return BeautifulSoup('<div>'+md_to_html(md, extensions=["tables"])+'</div>', 'html.parser')
    
    def fix_h(self, md: str):
        soup = self.to_soup(md)
        hs: list[list[Tag]] = []
        for i in range(20):
            h = soup.select(f"h{i}")
            if len(h):
                hs.extend(h)
        for i, _h_ in enumerate(hs, start=1):
            for h in _h_: 
                h.name = f"h{i}"
        return self.convert(soup)
                
    
MD = MyMarkdownConverter()