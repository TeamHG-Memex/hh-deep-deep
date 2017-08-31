from collections import deque
import logging
from pathlib import Path
import re
import math
import multiprocessing
import subprocess
from typing import Any, Dict, Optional, List, Set

from .crawl_utils import CrawlPaths, JsonLinesFollower, get_domain
from .dd_utils import BaseDDCrawlerProcess, is_running


# TODO - likely, hints must be removed


class DDCrawlerPaths(CrawlPaths):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.page_clf = self.root.joinpath('page_clf.joblib')
        self.link_clf = self.root.joinpath('Q.joblib')
        self.out = self.root.joinpath('out')
        self.redis_conf = self.root.joinpath('redis.conf')
        self.hints = self.root.joinpath('hints.txt')


class DDCrawlerProcess(BaseDDCrawlerProcess):
    _jobs_root = Path('dd-jobs')
    paths_cls = DDCrawlerPaths

    def __init__(self, *,
                 page_clf_data: bytes,
                 link_clf_data: bytes,
                 hints: List[str]=(),
                 broadness: str='BROAD',
                 **kwargs):
        super().__init__(**kwargs)
        self.page_clf_data = page_clf_data
        self.link_clf_data = link_clf_data
        self.broadness = broadness
        self.initial_hints = hints
        self._hint_domains = set(map(get_domain, hints))
        self._login_urls = set()  # type: Set[str]
        self._pending_login_domains = {}  # type: Dict[str, str]

    @classmethod
    def load_running(cls, root: Path, **kwargs) -> Optional['DDCrawlerProcess']:
        """ Initialize a process from a directory.
        """
        paths = DDCrawlerPaths(root)
        if not all(p.exists() for p in [
                paths.pid, paths.id, paths.seeds, paths.page_clf,
                paths.link_clf, paths.workspace_id]):
            return
        if not is_running(paths.root):
            logging.warning('Cleaning up job in {}.'.format(paths.root))
            subprocess.check_call(
                ['docker-compose', 'down', '-v'], cwd=str(paths.root))
            paths.pid.unlink()
            return
        with paths.seeds.open('rt', encoding='utf8') as f:
            seeds = [line.strip() for line in f]
        if paths.hints.exists():
            with paths.hints.open('rt', encoding='utf8') as f:
                hints = [line.strip() for line in f]
        else:
            hints = []
        return cls(
            pid=paths.pid.read_text(),
            id_=paths.id.read_text(),
            workspace_id=paths.workspace_id.read_text(),
            seeds=seeds,
            hints=hints,
            page_clf_data=paths.page_clf.read_bytes(),
            link_clf_data=paths.link_clf.read_bytes(),
            root=root,
            **kwargs)

    def start(self):
        assert self.pid is None
        self.paths.mkdir()
        self.paths.id.write_text(self.id_)
        self.paths.workspace_id.write_text(self.workspace_id)
        self.paths.page_clf.write_bytes(self.page_clf_data)
        self.paths.link_clf.write_bytes(self.link_clf_data)
        self.paths.seeds.write_text(
            '\n'.join(url for url in self.seeds), encoding='utf8')
        self.paths.hints.write_text(
            '\n'.join(url for url in self.initial_hints), encoding='utf8')
        n_processes = multiprocessing.cpu_count()
        if self.max_workers:
            n_processes = min(self.max_workers, n_processes)
        cur_dir = Path(__file__).parent  # type: Path
        compose_templates = (
            cur_dir.joinpath('dd-crawler-compose.template.yml').read_text())
        self.paths.root.joinpath('docker-compose.yml').write_text(
            compose_templates.format(
                docker_image=self.docker_image,
                page_limit=int(math.ceil(self.page_limit / n_processes)),
                max_relevant_domains=self._max_relevant_domains(self.broadness),
                relevancy_threshold=0.8,  # just a heuristics
                external_links=('["{}:proxy"]'.format(self.proxy_container)
                                if self.proxy_container else '[]'),
                proxy='http://proxy:8118' if self.proxy_container else '',
                **{p: self.to_host_path(getattr(self.paths, p)) for p in [
                    'seeds', 'hints', 'page_clf', 'link_clf', 'redis_conf', 'out',
                    'models',
                ]}
            ))
        redis_config = cur_dir.joinpath('redis.conf').read_text()
        self.paths.redis_conf.write_text(redis_config)
        logging.info('Starting crawl in {}'.format(self.paths.root))
        self._compose_call('up', '-d')
        self._compose_call('scale', 'crawler={}'.format(n_processes))
        self.pid = self.id_
        self.paths.pid.write_text(self.pid)
        logging.info('Crawl "{}" started'.format(self.id_))

    @staticmethod
    def _max_relevant_domains(broadness: str) -> str:
        if broadness == 'DEEP':
            return '0'
        elif broadness == 'BROAD':
            return ''
        else:
            return re.match('N(\d+)$', broadness).groups()[0]

    def handle_hint(self, url: str, pinned: bool):
        domain = get_domain(url)
        if pinned:
            self._hint_domains.add(domain)
        else:
            self._hint_domains.remove(domain)
        self._scrapy_command('hint', 'pin' if pinned else 'unpin', url)

    def handle_login(self, url, login, password):
        self._scrapy_command('login', url, login, password)

    def _get_updates(self) -> Dict[str, Any]:
        n_last = self.get_n_last()
        log_paths = list(self.paths.out.glob('*.log.jl'))
        updates = {}
        if log_paths:
            n_last_per_file = int(math.ceil(n_last / len(log_paths)))
            all_last_items = []
            total_score = n_crawled = n_domains = n_relevant_domains = 0
            for path in log_paths:
                follower = self._log_followers.setdefault(
                    path, JsonLinesFollower(path))
                last_items = deque(maxlen=n_last_per_file)
                for item in follower.get_new_items(at_least_last=True):
                    if item.get('has_login_form'):
                        self._handle_found_login_form(item, updates)
                    last_items.append(item)
                if last_items:
                    all_last_items.extend(last_items)
                    last = last_items[-1]
                    total_score += last['total_score']
                    n_crawled += last['n_crawled']
                    # A very small fraction (before "scale crawler=N")
                    # might overlap between workers, more might overlap
                    # in case some workers die.
                    n_domains += last['n_domains']
                    n_relevant_domains += last['n_relevant_domains']
            all_last_items.sort(key=lambda x: x['time'])
            updates['pages'] = [
                {'url': it['url'], 'score': 100 * it['score']}
                for it in all_last_items[-n_last:]]
            if n_crawled > 0:
                updates['progress'] = (
                    '{n_crawled:,} pages processed from {n_domains:,} domains '
                    '({n_relevant_domains:,} relevant), '
                    'average score {mean_score:.1f}.'.format(
                        n_crawled=n_crawled,
                        n_domains=n_domains,
                        n_relevant_domains=n_relevant_domains,
                        mean_score=100 * total_score / n_crawled,
                    ))
                updates['percentage_done'] = 100 * n_crawled / self.page_limit
        else:
            updates['progress'] = 'Crawl is not running yet'
        for domain in (self._hint_domains & set(self._pending_login_domains)):
            login_url = self._pending_login_domains.pop(domain)
            updates.setdefault('login_urls', []).append(login_url)
        return updates

    def _handle_found_login_form(self, item, updates):
        self._login_urls.add(item['url'])
        domain = get_domain(item['url'])
        if domain in self._hint_domains:
            login_urls = updates.setdefault('login_urls', [])
            login_urls.append(item['url'])
            self._pending_login_domains.pop(domain, None)
        elif domain not in self._pending_login_domains:
            self._pending_login_domains[domain] = item['url']
