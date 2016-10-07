from collections import deque
import csv
import logging
from pathlib import Path
import math
import multiprocessing
import subprocess
from typing import Optional, Tuple, List, Dict

from .crawl_utils import CrawlPaths, CrawlProcess, gen_job_path


class DDCrawlerPaths(CrawlPaths):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.link_clf = self.root.joinpath('Q.joblib')
        self.out = self.root.joinpath('out')
        self.redis_conf = self.root.joinpath('redis.conf')


class DDCrawlerProcess(CrawlProcess):
    jobs_root = Path('dd-jobs')
    default_docker_image = 'dd-crawler'

    def __init__(self, *,
                 page_clf_data: bytes,
                 link_clf_data: bytes,
                 root: Path=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.paths = DDCrawlerPaths(
            root or gen_job_path(self.id_, self.jobs_root))
        self.page_clf_data = page_clf_data
        self.link_clf_data = link_clf_data

    @classmethod
    def load_running(cls, root: Path, **kwargs) -> Optional['DDCrawlerProcess']:
        """ Initialize a process from a directory.
        """
        return None
        # TODO - check if the process is actually running
        paths = DDCrawlerPaths(root)
        if not all(p.exists() for p in [
                paths.id, paths.seeds, paths.page_clf, paths.link_clf]):
            return
        with paths.seeds.open('rt') as f:
            seeds = [line.strip() for line in f]
        id_ = paths.id.read_text()
        return cls(
            pid=id_,
            id_=id_,
            seeds=seeds,
            page_clf_data=paths.page_clf.read_bytes(),
            link_clf_data=paths.link_clf.read_bytes(),
            root=root,
            **kwargs)

    def start(self):
        assert self.pid is None
        self.paths.mkdir()
        self.paths.id.write_text(self.id_)
        self.paths.page_clf.write_bytes(self.page_clf_data)
        self.paths.link_clf.write_bytes(self.link_clf_data)
        self.paths.seeds.write_text('\n'.join(url for url in self.seeds))
        cur_dir = Path(__file__).parent  # type: Path
        compose_templates = (
            cur_dir.joinpath('dd-crawler-compose.template.yml').read_text())
        self.paths.root.joinpath('docker-compose.yml').write_text(
            compose_templates.format(
                docker_image=self.docker_image,
                **{p: self.to_host_path(getattr(self.paths, p)) for p in [
                    'seeds', 'page_clf', 'link_clf', 'redis_conf', 'out']}
            ))
        redis_config = cur_dir.joinpath('redis.conf').read_text()
        self.paths.redis_conf.write_text(redis_config)
        logging.info('Starting crawl in {}'.format(self.paths.root))
        self._compose_cmd('up', '-d')
        n_processes = multiprocessing.cpu_count()
        self._compose_cmd('scale', 'crawler={}'.format(n_processes))
        logging.info('Crawl "{}" started'.format(self.id_))
        self.pid = self.id_

    def stop(self):
        assert self.pid is not None
        self._compose_cmd('down', '-v')
        logging.info('Crawl "{}" stopped'.format(self.pid))
        self.pid = None

    def _get_updates(self) -> Tuple[str, List[str]]:
        n_last = self.get_n_last()
        csv_paths = list(self.paths.out.glob('*.csv'))
        if csv_paths:
            n_last_per_file = math.ceil(n_last / len(csv_paths))
            last_lines = []
            for csv_path in csv_paths:
                for ts, url, _, _, score in get_last_csv_items(
                        csv_path, n_last_per_file, exp_len=5):
                    last_lines.append((float(ts), url, float(score)))
            last_lines.sort(key=lambda x: x[0])
            last_lines = last_lines[-n_last:]
            pages = [{'url': url, 'score': 100 * score}
                        for _, url, score in last_lines]
        else:
            pages = []
        return 'TODO', pages

    def _compose_cmd(self, *args):
        subprocess.check_call(
            ['docker-compose'] + list(args), cwd=str(self.paths.root))


def get_last_csv_items(csv_path: Path, n_last: int, exp_len: int) -> List[Dict]:
    # This is only valid if there are no newlines in items
    # TODO - more efficient, skip to the end of file
    last_lines = deque(csv_path.open('rt'), maxlen=n_last + 1)
    last_items = [it for it in csv.reader(last_lines) if len(it) == exp_len]
    return last_items[-n_last:]
