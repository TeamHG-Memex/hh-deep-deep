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


def get_last_valid_item(gzip_path: str) -> Optional[Dict]:
    # TODO - make it more efficient, skip to the end of the file
    with gzip.open(gzip_path, 'rt') as f:
        prev_line = cur_line = None
        try:
            for line in f:
                prev_line = cur_line
                cur_line = line
        except Exception:
            pass
        for line in [cur_line, prev_line]:
            if line is not None:
                try:
                    return json.loads(line)
                except Exception:
                    pass


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
        self.last_updates = None  # last update sent in self.get_updates

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
        if updates != self.last_updates:
            self.last_updates = updates
            return updates

    def _get_updates(self) -> Tuple[str, List[str]]:
        raise NotImplementedError
