from PIL import Image, UnidentifiedImageError, ImageChops
import requests
from io import BytesIO
import logging
from os.path import dirname
from os import makedirs
from typing import List, Tuple, NamedTuple, Union, Dict
from functools import cached_property, cache
from os.path import isfile
from core.web import Driver
from selenium.webdriver.common.by import By

logger = logging.getLogger(__name__)


class CornerColor(NamedTuple):
    top_left: Tuple[int, int, int]
    top_right: Tuple[int, int, int]
    bottom_left: Tuple[int, int, int]
    bottom_right: Tuple[int, int, int]

    def get_count(self) -> Dict[Tuple[int, int, int], int]:
        count = {}
        for c in self:
            count[c] = count.get(c, 0) + 1
        return count

    def get_most_common(self):
        count = self.get_count()
        order: List[Tuple[Tuple[int, int, int], int]] = sorted(count.items(), key=lambda kv:(kv[1], kv[0]))
        color = order.pop()[0]
        return color


class MyImage:
    def __init__(self, image: Union[str, Image.Image], parent: Image.Image = None, background: Tuple[int, int, int]=None):
        self.__path_or_image = image
        self.__url = None
        self.__parent = parent
        self.__background = background
        if self.__background is None:
            corner = self.get_corner_colors()
            if corner:
                self.__background = corner.get_most_common()

    @staticmethod
    @cache
    def get(url: str):
        return MyImage(url)

    @property
    def background(self):
        im = self
        while im.__background is None and im.__parent is not None:
            im = im.__parent
        return im.__background

    @property
    def parent(self):
        return self.__parent

    @property
    def url(self):
        if isinstance(self.__url, str):
            return self.__url
        if isinstance(self.__path_or_image, str) and not isfile(self.__path_or_image):
            return self.__path_or_image
        raise ValueError("No hay url asociada a esta imagen")

    @url.setter
    def url(self, url: str):
        self.__url = url

    @property
    def path(self):
        if isinstance(self.__path_or_image, str):
            return self.__path_or_image

    @property
    def proto(self):
        return self.path.split("://")[0].lower()

    @cached_property
    def im(self):
        if isinstance(self.__path_or_image, Image.Image):
            return self.__path_or_image
        path = str(self.path)
        if not isfile(path):
            path = self.__get(self.path)
            if path is None:
                return None
        try:
            im = Image.open(path)
            im = im.convert('RGB')
            return im
        except requests.exceptions.RequestException:
            logger.critical("No se pudo descargar la imagen "+str(self.path), exc_info=True)
        except UnidentifiedImageError:
            logger.critical("La ruta no apunta a una imagen válida "+str(self.path), exc_info=True)
        return None

    def __get(self, url: str):
        r = requests.get(url)
        if r.status_code != 403 and r.content:
            return BytesIO(r.content)
        with Driver(browser="firefox") as f:
            f.get(url)
            img = f.safe_wait("img", by=By.CSS_SELECTOR)
            if img:
                return BytesIO(img.screenshot_as_png)
        logger.critical(f"status_code={r.status_code} en {self.path}")

    def trim(self):
        count = self.get_corner_colors().get_count()
        order: List[Tuple[Tuple[int, int, int], int]] = sorted(count.items(), key=lambda kv:(kv[1], kv[0]))
        color = order.pop()[0]
        im = self.__trim(color)
        if im is None or im.isKO:
            return None
        if count[color] < 2 or len(order) == 0:
            return im
        color = order.pop()[0]
        if count[color] < 2:
            return im
        im2 = im.__trim(color)
        if im2 is None or im2.isKO:
            return im
        diff_area2 = im.area - im2.area
        if diff_area2 <= (im.area - im2.area):
            im2.__background = im.__background
        return im2

    def __trim(self, color):
        bg = Image.new(self.im.mode, self.im.size, color)
        diff = ImageChops.difference(self.im, bg)
        diff = ImageChops.add(diff, diff, 1, -50)
        bbox = diff.getbbox(alpha_only=False)
        if not bbox:
            logger.warning(f"trim: diff.getbbox() is None en {self.origin.name}")
            return None
        size_bbox = (bbox[2] - bbox[0], bbox[3] - bbox[1])
        if self.im.size == size_bbox:
            logger.debug(f"trim: no tiene marco {self.origin.name}")
            return None
        im = self.im.crop(bbox)
        return MyImage(im, parent=self, background=color)

    def get_corner_colors(self) -> CornerColor:
        if self.im is None:
            return None
        top_left_color = self.im.getpixel((0, 0))
        top_right_color = self.im.getpixel((self.im.width - 1, 0))
        bottom_left_color = self.im.getpixel((0, self.im.height - 1))
        bottom_right_color = self.im.getpixel((self.im.width - 1, self.im.height - 1))
        return CornerColor(
            top_left=top_left_color,
            top_right=top_right_color,
            bottom_left=bottom_left_color,
            bottom_right=bottom_right_color
        )

    @property
    def isOK(self):
        return isinstance(self.im, Image.Image)

    @property
    def isKO(self):
        return not isinstance(self.im, Image.Image)

    @property
    def isLandscape(self):
        return self.im.width >= self.im.height

    @property
    def isPortrait(self):
        return not self.isLandscape

    @property
    def orientation(self):
        if not self.isOK:
            return ""
        if self.isLandscape:
            return "landscape"
        return "portrait"

    def thumbnail(self, width: int, height: int):
        im = self.im.copy()
        im.thumbnail((round(width), round(height)))
        return MyImage(im, parent=self, background=self.background)

    @property
    def area(self):
        return self.im.width*self.im.height

    @property
    def name(self):
        if isinstance(self.__path_or_image, str):
            return self.__path_or_image
        return str(self.im)

    def save(self, filename: str, quality: float = None):
        dr = dirname(filename)
        if dr:
            makedirs(dr, exist_ok=True)
        try:
            self.im.save(filename, quality=quality)
        except IOError:
            logger.critical(f"No se pudo copiar {self.name} a {filename}", exc_info=True)
            return None
        return MyImage(filename, parent=self, background=self.background)

    @property
    def origin(self):
        p = self
        while p.parent is not None:
            p = p.parent
        return p
