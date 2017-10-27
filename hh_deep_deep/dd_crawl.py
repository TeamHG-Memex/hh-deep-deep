from collections import deque
import json
import logging
from pathlib import Path
import re
import math
import multiprocessing
import subprocess
from typing import Any, Dict, Optional

from .crawl_utils import JsonLinesFollower
from .dd_utils import BaseDDPaths, BaseDDCrawlerProcess, is_running
from .deepdeep_crawl import DEFAULT_TRAINER_PAGE_LIMIT


class DDCrawlerPaths(BaseDDPaths):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.page_clf = self.root.joinpath('page_clf.joblib')
        self.link_clf = self.root.joinpath('Q.joblib')


class DDCrawlerProcess(BaseDDCrawlerProcess):
    paths_cls = DDCrawlerPaths
    crawler_name = 'deepdeep'

    def __init__(self, *,
                 page_clf_data: bytes,
                 link_clf_data: bytes,
                 broadness: str='BROAD',
                 **kwargs):
        super().__init__(**kwargs)
        self.page_clf_data = page_clf_data
        self.link_clf_data = link_clf_data
        self.broadness = broadness

    @classmethod
    def load_running(cls, root: Path, **kwargs) -> Optional['DDCrawlerProcess']:
        """ Initialize a process from a directory.
        """
        paths = cls.paths_cls(root)
        if not all(p.exists() for p in [paths.pid, paths.meta, paths.seeds,
                                        paths.page_clf, paths.link_clf]):
            return
        if not is_running(paths.root):
            logging.warning('Cleaning up job in {}.'.format(paths.root))
            subprocess.check_call(
                ['docker-compose', 'down', '-v'], cwd=str(paths.root))
            paths.pid.unlink()
            return
        with paths.seeds.open('rt', encoding='utf8') as f:
            seeds = [line.strip() for line in f]
        if paths.login_credentials.exists():
            with paths.login_credentials.open('rt', encoding='utf8') as f:
                login_credentials = json.load(f)
        else:
            login_credentials = None
        meta = json.loads(paths.meta.read_text('utf8'))
        return cls(
            id_=meta['id'],
            workspace_id=meta['workspace_id'],
            seeds=seeds,
            login_credentials=login_credentials,
            page_clf_data=paths.page_clf.read_bytes(),
            link_clf_data=paths.link_clf.read_bytes(),
            root=root,
            **kwargs)

    def start(self):
        self.paths.mkdir()
        self.paths.meta.write_text(json.dumps({
            'id': self.id_,
            'workspace_id': self.workspace_id,
        }), encoding='utf8')
        self.paths.page_clf.write_bytes(self.page_clf_data)
        self.paths.link_clf.write_bytes(self.link_clf_data)
        # Create out/media beforehand to prevent a race condition
        self.paths.out.joinpath('media').mkdir(parents=True)
        self.paths.seeds.write_text(
            '\n'.join(url for url in self.seeds), encoding='utf8')
        with self.paths.login_credentials.open('wt', encoding='utf8') as f:
            json.dump(self.login_credentials, f)
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
                external_links=self.external_links,
                proxy=self.proxy,
                **{p: self.to_host_path(getattr(self.paths, p)) for p in [
                    'seeds', 'page_clf', 'link_clf', 'redis_conf', 'out',
                    'models', 'login_credentials',
                ]}
            ))
        redis_config = cur_dir.joinpath('redis.conf').read_text()
        self.paths.redis_conf.write_text(redis_config)
        logging.info('Starting crawl in {}'.format(self.paths.root))
        self._compose_call('up', '-d')
        self._compose_call('scale', 'crawler={}'.format(n_processes))
        self.paths.pid.write_text(self.id_)
        logging.info('Crawl "{}" started'.format(self.id_))

    @staticmethod
    def _max_relevant_domains(broadness: str) -> str:
        if broadness == 'DEEP':
            return '0'
        elif broadness == 'BROAD':
            return ''
        else:
            return re.match('N(\d+)$', broadness).groups()[0]

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
                        updates.setdefault('login_urls', []).append(item['url'])
                    if 'login_success' in item:
                        self._add_login_state_update(item, updates)
                    if 'url' in item:
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
                # This is correct as long as trainer crawler is really run
                # for DEFAULT_TRAINER_PAGE_LIMIT. It's not the case in tests,
                # but is true in production, where we don't set a custom limit.
                updates['percentage_done'] = 100 * (
                    (n_crawled + DEFAULT_TRAINER_PAGE_LIMIT) /
                    (self.page_limit + DEFAULT_TRAINER_PAGE_LIMIT))
        else:
            updates['progress'] = 'Crawl is not running yet'
        return updates
