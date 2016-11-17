import csv
import logging
from pathlib import Path
import math
import multiprocessing
import subprocess
from typing import Optional, Tuple, List, Dict

from .crawl_utils import CrawlPaths, CrawlProcess, gen_job_path, get_last_lines


class DDCrawlerPaths(CrawlPaths):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.link_clf = self.root.joinpath('Q.joblib')
        self.out = self.root.joinpath('out')
        self.redis_conf = self.root.joinpath('redis.conf')


class DDCrawlerProcess(CrawlProcess):
    _jobs_root = Path('dd-jobs')
    default_docker_image = 'dd-crawler-hh'

    def __init__(self, *,
                 page_clf_data: bytes,
                 link_clf_data: bytes,
                 root: Path=None,
                 max_workers: int=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.paths = DDCrawlerPaths(
            root or gen_job_path(self.id_, self.jobs_root))
        self.page_clf_data = page_clf_data
        self.link_clf_data = link_clf_data
        self.max_workers = max_workers

    @classmethod
    def load_running(cls, root: Path, **kwargs) -> Optional['DDCrawlerProcess']:
        """ Initialize a process from a directory.
        """
        paths = DDCrawlerPaths(root)
        if not all(p.exists() for p in [
                paths.pid, paths.id, paths.seeds, paths.page_clf, paths.link_clf]):
            return
        if not cls._is_running(paths.root):
            logging.warning('Cleaning up job in {}.'.format(paths.root))
            subprocess.check_call(
                ['docker-compose', 'down', '-v'], cwd=str(paths.root))
            paths.pid.unlink()
            return
        with paths.seeds.open('rt') as f:
            seeds = [line.strip() for line in f]
        return cls(
            pid=paths.pid.read_text(),
            id_=paths.id.read_text(),
            seeds=seeds,
            page_clf_data=paths.page_clf.read_bytes(),
            link_clf_data=paths.link_clf.read_bytes(),
            root=root,
            **kwargs)

    @staticmethod
    def _is_running(root: Path):
        running_containers = (
            subprocess.check_output(['docker-compose', 'ps', '-q'], cwd=str(root))
            .decode('utf8').strip().split('\n'))
        # Only container is not normal,
        # should be at least redis and one crawler.
        return len(running_containers) >= 2

    def is_running(self):
        return self.pid is not None and self._is_running(self.paths.root)

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
        self._compose_call('up', '-d')
        n_processes = multiprocessing.cpu_count()
        if self.max_workers:
            n_processes = min(self.max_workers, n_processes)
        self._compose_call('scale', 'crawler={}'.format(n_processes))
        self.pid = self.id_
        self.paths.pid.write_text(self.pid)
        logging.info('Crawl "{}" started'.format(self.id_))

    def stop(self, verbose=False):
        assert self.pid is not None
        if verbose:
            self._compose_call('logs', '--tail', '30')
        self._compose_call('down', '-v')
        self.paths.pid.unlink()
        logging.info('Crawl "{}" stopped'.format(self.pid))
        self.pid = None

    def _get_updates(self) -> Tuple[str, List[str]]:
        n_last = self.get_n_last()
        csv_paths = list(self.paths.out.glob('*.csv'))
        if csv_paths:
            n_last_per_file = math.ceil(n_last / len(csv_paths))
            last_pages = []
            total_score = n_crawled = n_domains = n_relevant_domains = 0
            for csv_path in csv_paths:
                last_items = get_last_csv_items(
                    csv_path, n_last_per_file, exp_len=9)
                for item in last_items:
                    ts, url, _, _, score = item[:5]
                    last_pages.append((float(ts), url, float(score)))
                if last_items:
                    _total_score, _n_crawled, _n_domains, _n_relevant_domains\
                        = last_items[-1][5:]
                    total_score += float(_total_score)
                    n_crawled += int(_n_crawled)
                    # A very small fraction (before "scale crawler=N")
                    # might overlap between workers, more might overlap
                    # in case some workers die.
                    n_domains += int(_n_domains)
                    n_relevant_domains += int(_n_relevant_domains)
            last_pages.sort(key=lambda x: x[0])
            last_pages = last_pages[-n_last:]
            pages = [{'url': url, 'score': 100 * score}
                     for _, url, score in last_pages]
            if n_crawled == 0:
                progress = 'Crawl started, no updates yet'
            else:
                progress = (
                    '{n_crawled:,} pages processed from {n_domains:,} domains '
                    '({n_relevant_domains:,} relevant), '
                    'average score {mean_score:.1f}.'.format(
                        n_crawled=n_crawled,
                        n_domains=n_domains,
                        n_relevant_domains=n_relevant_domains,
                        mean_score=total_score / n_crawled,
                    ))
        else:
            progress, pages = 'Craw is not running yet', []
        return progress, pages

    def _compose_call(self, *args):
        subprocess.check_call(
            ['docker-compose'] + list(args), cwd=str(self.paths.root))


def get_last_csv_items(csv_path: Path, n_last: int, exp_len: int) -> List[Dict]:
    last_lines = get_last_lines(csv_path, n_last + 1)
    last_items = [it for it in csv.reader(last_lines) if len(it) == exp_len]
    return last_items[-n_last:]
