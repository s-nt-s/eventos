import os
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse
import logging


logger = logging.getLogger(__name__)


class Downloader:
    def __init__(self, max_workers=10, timeout=10):
        self.max_workers = max_workers
        self.timeout = timeout

    def _get_filename(self, url, index):
        path = urlparse(url).path
        name = os.path.basename(path)
        if not name:
            name = f"image_{index}.jpg"
        return name

    def _download_one(self, url, folder, index):
        try:
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()

            filename = self._get_filename(url, index)
            filepath = os.path.join(folder, filename)

            with open(filepath, "wb") as f:
                f.write(response.content)

            return url, True
        except Exception:
            return url, False

    def dwn(self, folder, *urls):
        urls = tuple(sorted(set(urls)))
        if len(urls) == 0:
            return tuple()
        logger.debug(f"Downloading {len(urls)} urls in {folder}, ej: {urls[0]}")
        os.makedirs(folder, exist_ok=True)

        results: list[tuple[str, bool]] = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [
                executor.submit(self._download_one, url, folder, i)
                for i, url in enumerate(urls)
            ]

            for future in as_completed(futures):
                results.append(future.result())

        ok = []
        ko = []
        for u, b in results:
            if b:
                ok.append(u)
            else:
                ko.append(u)
        if ko:
            logger.critical(f"Downloading {len(ko)} KO, ej: {ko[0]}")
        return tuple(ok)


DWN = Downloader()
