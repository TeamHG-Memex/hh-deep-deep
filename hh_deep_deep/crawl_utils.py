import json
import hashlib
import logging
from pathlib import Path
import math
import time
from typing import Any, Dict, Optional, List

import tldextract


def get_domain(url):
    parsed = tldextract.extract(url)
    domain = parsed.registered_domain
    if not domain:  # e.g. localhost which is used in tests
        domain = '.'.join(filter(None, [parsed.domain, parsed.suffix]))
    return domain.lower()


def gen_job_path(id_: str, root: Path) -> Path:
    return root.joinpath('{}_{}'.format(
        int(time.time()),
        hashlib.md5(id_.encode('utf8')).hexdigest()[:12]
    ))


class CrawlPaths:
    def __init__(self, root: Path):
        root = root.absolute()
        self.root = root
        self.pid = root.joinpath('pid.txt')
        self.meta = root.joinpath('meta.json')
        self.seeds = root.joinpath('seeds.txt')
        self.models = Path('./models')

    def mkdir(self):
        self.root.mkdir(parents=True, exist_ok=True)


class CrawlProcess:
    id_field = 'id'
    default_docker_image = None
    target_sample_rate_pm = 10  # per minute

    def __init__(self, *,
                 id_: str,
                 workspace_id: str,
                 seeds: List[str],
                 jobs_root: Path,
                 docker_image: str=None,
                 host_root: str=None,
                 page_limit: int=None,
                 proxy_container: str=None,
                 test_server_container: str=None):
        self.id_ = id_
        self.workspace_id = workspace_id
        self.seeds = seeds
        self.docker_image = docker_image or self.default_docker_image
        self.host_root = Path(host_root or '.').absolute()
        self.jobs_root = jobs_root
        self.page_limit = page_limit
        self.proxy_container = proxy_container
        self.test_server_container = test_server_container
        self.last_progress = None  # last update sent in self.get_updates
        self.last_page = None  # last page sample sent in self.get_updates
        self.last_page_time = None

    @classmethod
    def load_all_running(cls, **kwargs) -> Dict[str, 'CrawlProcess']:
        """ Return a dictionary of currently running processes.
        """
        running = {}
        jobs_root = kwargs['jobs_root']
        logging.info('Loading jobs from {}'.format(jobs_root))
        if jobs_root.exists():
            for job_root in sorted(jobs_root.iterdir()):
                process = cls.load_running(job_root, **kwargs)
                if process is not None:
                    old_process = running.get(process.id_)
                    if old_process is not None:
                        old_process.stop()
                    running[process.id_] = process
        return running

    @classmethod
    def load_running(cls, root: Path, **kwargs) -> Optional['CrawlProcess']:
        """ Initialize a process from a directory.
        """
        raise NotImplementedError

    def start(self):
        raise NotImplementedError

    def stop(self, verbose=False):
        raise NotImplementedError

    def is_running(self):
        raise NotImplementedError

    def get_updates(self) -> Dict[str, Any]:
        """ Return a dict of updates with a progress message,
        and possibly a sample of crawled urls, and optionally other items.
        """
        updates = self._get_updates()
        if updates.get('pages'):
            self.last_page_time = time.time()
        return updates

    def _get_updates(self) -> Dict[str, Any]:
        raise NotImplementedError

    def get_n_last(self):
        """ Return desired number of last items in order to maintain
        self.target_sample_rate_pm
        """
        if self.last_page_time is None:
            return 1
        delay_m = (time.time() - self.last_page_time) / 60
        return math.ceil(self.target_sample_rate_pm * delay_m)

    def to_host_path(self, path: Path) -> Path:
        """ Convert path to a host path, which must lie under ".".
        The reason for this is that we might be in a docker container,
        but the commands we issue to docker are interpreted on the host,
        so the paths must also be host paths.
        """
        rel_path = path.absolute().relative_to(Path('.').absolute())
        return self.host_root.joinpath(rel_path)


class JsonLinesFollower:
    """ Follow json lines file contents: iteration allows to read all new items
    since last iteration.
    """
    def __init__(self, path: Path, encoding='utf8'):
        self.path = path
        self.encoding = encoding
        self._pos = 0
        self._last_item = None

    def get_new_items(self, at_least_last=False):
        """ Get new items since the file was last read.
        If at_least_last is True, always try to return at least one item -
        if there are no new items, yield the last item.
        """
        with self.path.open('rb') as f:
            f.seek(self._pos)
            line = ''
            any_read = False
            last_read = True
            for line in f:
                try:
                    self._last_item = json.loads(line.decode(self.encoding))
                except Exception:
                    last_read = False
                else:
                    last_read = any_read = True
                    yield self._last_item
            self._pos = f.tell()
            if not last_read:
                self._pos -= len(line)
        if at_least_last and not any_read and self._last_item is not None:
            yield self._last_item
