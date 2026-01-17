from asyncio import Semaphore, Lock, sleep, gather, get_event_loop, run
from aiohttp import ClientSession, BasicAuth, ClientTimeout, FormData, CookieJar

from typing import Dict, Optional, NamedTuple, Callable, Awaitable, TypeVar, Generic
import logging
import enum
from yarl import URL
from requests.cookies import RequestsCookieJar
from aiohttp import ClientResponse

ProcessedResponse = TypeVar("ProcessedResponse")
AsyncResponseHandler = Callable[[ClientResponse], Awaitable[ProcessedResponse]]

T = TypeVar("T")

logger = logging.getLogger(__name__)


async def rq_to_text(r: ClientResponse):
    return await r.text()


async def rq_to_bytes(r: ClientResponse):
    return await r.read()


async def rq_to_json(r: ClientResponse):
    return await r.json()


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


class AsyncFetcher(Generic[ProcessedResponse]):
    def __init__(
        self,
        onread: AsyncResponseHandler,
        max_concurrency: int = 40,
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
        if isinstance(cookie_jar, RequestsCookieJar):
            cookie_jar = aio_cookiejar_from_requests(cookie_jar)
        if cookie_jar is not None and not isinstance(cookie_jar, CookieJar):
            raise ValueError("cookie_jar must be a CookieJar or RequestsCookieJar")
        self.__cookie_jar = cookie_jar
        self.__max_concurrency = max_concurrency
        self.__timeout = ClientTimeout(total=timeout)
        self.__onread = onread
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
                return await self.__onread(response)

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
    ) -> list[ProcessedResponse]:
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


class Getter(Generic[ProcessedResponse]):
    def __init__(
        self,
        onread: AsyncResponseHandler,
        headers: Optional[Dict[str, str]] = None,
        cookie_jar: Optional[CookieJar | RequestsCookieJar] = None,
    ):
        self.__fetcher = AsyncFetcher(
            onread=onread,
            headers=headers,
            cookie_jar=cookie_jar
        )

    def get(self, *urls: str) -> dict[str, ProcessedResponse]:
        if len(urls) == 0:
            return {}
        urls = sorted(set(urls))
        logger.debug(f"Fetching {len(urls)} URLs")
        bodies = self.__fetcher.run(*urls)
        return dict(zip(urls, bodies))

if __name__ == "__main__":
    GT = Getter(
        onread=rq_to_text
    )
    from core.filemanager import FM
    urls: set[str] = set()
    for i in FM.load("rec/api_madrid_es/dataset.json"):
        vgnextoid = i['link'].split("=")[-1]
        urls.add(f"https://www.madrid.es/ContentPublisher/jsp/cont/microformatos/obtenerVCal.jsp?vgnextoid={vgnextoid}")
    urls = sorted(urls)
    print(len(urls))
    results = GT.get(*urls)
    print(len(urls), len(results))
