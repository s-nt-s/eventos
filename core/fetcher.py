from asyncio import Semaphore, Lock, sleep, gather, get_event_loop, run
from aiohttp import ClientSession, BasicAuth, ClientTimeout, FormData, CookieJar

from typing import Dict, Optional, NamedTuple
import logging
import enum
from typing import TypeVar, Generic
from yarl import URL
from requests.cookies import RequestsCookieJar

T = TypeVar("T")

logger = logging.getLogger(__name__)


def aio_cookiejar_from_requests(req_cookies: RequestsCookieJar):
    jar = CookieJar()

    for c in req_cookies:
        jar.update_cookies(
            {c.name: c.value},
            response_url=URL(
                f"http{'s' if c.secure else ''}://{c.domain}{c.path}"
            )
        )

    return jar


class ResponseType(enum.Enum):
    TEXT = "text"
    JSON = "json"
    BYTES = "bytes"


class URLRequest(NamedTuple):
    url: str
    method: Optional[str] = "GET"
    data: Optional[FormData] = None


class AsyncFetcher(Generic[T]):
    def __init__(
        self,
        response_type: ResponseType | str,
        max_concurrency: int = 20,
        timeout: float = 10.0,
        raise_for_status: bool = True,
        cookie_jar: Optional[CookieJar | RequestsCookieJar] = None,
        headers: Optional[Dict[str, str]] = None,
        rate_limit: Optional[float] = None,
        proxy: Optional[str] = None,
        auth: Optional[BasicAuth] = None,
        retries: int = 0,
        retry_delay: float = 0.5,
    ):
        if isinstance(response_type, str):
            response_type = ResponseType(response_type)
        if not isinstance(response_type, ResponseType):
            raise ValueError("response_type must be a ResponseType or str")
        if isinstance(cookie_jar, RequestsCookieJar):
            cookie_jar = aio_cookiejar_from_requests(cookie_jar)
        if cookie_jar is not None and not isinstance(cookie_jar, CookieJar):
            raise ValueError("cookie_jar must be a CookieJar or RequestsCookieJar")
        self.__cookie_jar = cookie_jar
        self.__max_concurrency = max_concurrency
        self.__timeout = ClientTimeout(total=timeout)
        self.__response_type = response_type
        self.__raise_for_status = raise_for_status
        self.__headers = headers or {}
        self.__rate_limit = rate_limit
        self.__proxy = proxy
        self.__auth = auth
        self.__last_request = 0.0
        self.__retries = retries
        self.__retry_delay = retry_delay
        self.__rate_lock = Lock()

    async def __respect_rate_limit(self):
        if not self.__rate_limit:
            return

        async with self.__rate_lock:
            now = get_event_loop().time()
            wait = self.__rate_limit - (now - self.__last_request)
            if wait > 0:
                await sleep(wait)
            self.__last_request = get_event_loop().time()

    async def __fetch_once(
        self,
        semaphore: Semaphore,
        session: ClientSession,
        rqs: URLRequest,
    ):
        async with semaphore:
            await self.__respect_rate_limit()

            async with session.request(
                rqs.method,
                rqs.url,
                data=rqs.data,
                proxy=self.__proxy,
                auth=self.__auth,
            ) as response:
                if self.__raise_for_status:
                    response.raise_for_status()
                if self.__response_type == ResponseType.TEXT:
                    return await response.text()
                if self.__response_type == ResponseType.JSON:
                    return await response.json()
                if self.__response_type == ResponseType.BYTES:
                    return await response.read()

    async def __fetch_with_retries(
        self,
        semaphore: Semaphore,
        session: ClientSession,
        rqs: URLRequest,
    ):
        for attempt in range(self.__retries + 1):
            try:
                return await self.__fetch_once(
                    semaphore,
                    session,
                    rqs,
                )
            except Exception:
                if attempt >= self.__retries:
                    raise
                await sleep(self.__retry_delay)

        return None

    async def fetch(
        self,
        *rqs: URLRequest | str
    ) -> list[T]:
        if len(rqs) == 0:
            return []

        semaphore = Semaphore(self.__max_concurrency)
        rqs = list(rqs)
        for i, rq in enumerate(rqs):
            if isinstance(rq, str):
                rqs[i] = URLRequest(url=rq)
            elif not isinstance(rq, URLRequest):
                raise ValueError("rqs must be URLRequest or str")
        async with ClientSession(
            timeout=self.__timeout,
            headers=self.__headers,
            raise_for_status=self.__raise_for_status,
            cookie_jar=self.__cookie_jar,
        ) as session:
            tasks = [
                self.__fetch_with_retries(
                    semaphore,
                    session,
                    rq
                )
                for rq in rqs
            ]

            results = await gather(*tasks)

        return results

    def run(
        self,
        *rqs: URLRequest | str
    ):
        return run(self.fetch(*rqs))


class URLText(NamedTuple):
    url: str
    body: str


class URLList(NamedTuple):
    url: str
    body: list


class URLDict(NamedTuple):
    url: str
    body: dict


class URLByte(NamedTuple):
    url: str
    body: bytes


class Getter():
    def __init__(
        self,
        headers: Optional[Dict[str, str]] = None,
        cookie_jar: Optional[CookieJar | RequestsCookieJar] = None,
    ):
        self.__fetcher_text = AsyncFetcher[str](
            response_type=ResponseType.TEXT,
            headers=headers,
            cookie_jar=cookie_jar
        )
        self.__fetcher_json = AsyncFetcher(
            response_type=ResponseType.JSON,
            headers=headers,
            cookie_jar=cookie_jar
        )
        self.__fetcher_bytes = AsyncFetcher[bytes](
            response_type=ResponseType.BYTES,
            headers=headers,
            cookie_jar=cookie_jar
        )

    def __get(self, fetcher: AsyncFetcher[T], *urls: str) -> tuple[list[str], list[T]]:
        if len(urls) == 0:
            return [], []
        urls = sorted(set(urls))
        logger.debug(f"Fetching {len(urls)} URLs")
        bodies = fetcher.run(*urls)
        #logger.info(f"Fetching {len(urls)} URLs DONE")
        return urls, bodies

    def get_text(self, *urls: str):
        urls, bodies = self.__get(self.__fetcher_text, *urls)
        return tuple(URLText(url=u, body=b) for u, b in zip(urls, bodies))

    def get_bytes(self, *urls: str):
        urls, bodies = self.__get(self.__fetcher_bytes, *urls)
        return tuple(URLByte(url=u, body=b) for u, b in zip(urls, bodies))

    def get_list(self, *urls: str):
        urls, bodies = self.__get(self.__fetcher_json, *urls)
        arr: list[URLList] = []
        for u, b in zip(urls, bodies):
            if not isinstance(b, list):
                raise ValueError(f"URL {u} did not return a list")
            arr.append(URLList(url=u, body=b))
        return tuple(arr)

    def get_dict(self, *urls: str):
        urls, bodies = self.__get(self.__fetcher_json, *urls)
        arr: list[URLDict] = []
        for u, b in zip(urls, bodies):
            if not isinstance(b, dict):
                raise ValueError(f"URL {u} did not return a dict")
            arr.append(URLDict(url=u, body=b))
        return tuple(arr)


if __name__ == "__main__":
    GT = Getter()
    from core.filemanager import FM
    urls: set[str] = set()
    for i in FM.load("rec/api_madrid_es/dataset.json"):
        vgnextoid = i['link'].split("=")[-1]
        urls.add(f"https://www.madrid.es/ContentPublisher/jsp/cont/microformatos/obtenerVCal.jsp?vgnextoid={vgnextoid}")
    urls = sorted(urls)
    print(len(urls))
    results = GT.get_text(*urls)
    print(len(urls), len(results))
