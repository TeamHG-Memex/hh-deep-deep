from collections import deque
import json
import gzip
import hashlib
from pathlib import Path
import time
from typing import Dict, Optional, List, Tuple


def gen_job_path(id_: str, root: Path) -> Path:
    return root.joinpath('{}_{}'.format(
        int(time.time()),
        hashlib.md5(id_.encode('utf8')).hexdigest()[:12]
    ))


class CrawlPaths:
    def __init__(self, root: Path):
        root = root.absolute()
        self.root = root
        self.id = root.joinpath('id.txt')
        self.page_clf = root.joinpath('page_clf.joblib')
        self.seeds = root.joinpath('seeds.txt')

    def mkdir(self):
        self.root.mkdir(parents=True, exist_ok=True)


def get_last_valid_items(gzip_path: str, n_last: int) -> List[Dict]:
    # TODO - make it more efficient, skip to the end of the file
    buffer = deque(maxlen=n_last + 1)
    with gzip.open(gzip_path, 'rt') as f:
        try:
            for line in f:
                buffer.append(line)
        except Exception:
            pass
    results = []
    for line in reversed(buffer):
        try:
            results.append(json.loads(line))
        except Exception:
            pass
    return results[:n_last]


class CrawlProcess:
    jobs_root = None
    default_docker_image = None
    paths_cls = CrawlPaths

    def __init__(self, *,
                 id_: str,
                 seeds: List[str],
                 docker_image: str=None,
                 pid: str=None):
        self.pid = pid
        self.id_ = id_
        self.seeds = seeds
        self.docker_image = docker_image or self.default_docker_image
        self.last_progress = None  # last update sent in self.get_updates
        self.last_progress_time = None

    @classmethod
    def load_all_running(cls, **kwargs) -> Dict[str, 'CrawlProcess']:
        """ Return a dictionary of currently running processes.
        """
        running = {}
        if cls.jobs_root.exists():
            for job_root in sorted(cls.jobs_root.iterdir()):
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

    def stop(self):
        raise NotImplementedError

    def get_updates(self) -> Optional[Tuple[str, List[str]]]:
        """ Return a tuple of progress update, and a list (possibly empty)
        of sample crawled urls.
        If nothing changed from the last time, return None.
        """
        updates = self._get_updates()
        if updates is not None:
            progress, pages = updates
            if progress != self.last_progress:
                self.last_progress = progress
                self.last_progress_time = time.time()
                return progress, pages

    def _get_updates(self) -> Tuple[str, List[str]]:
        raise NotImplementedError
