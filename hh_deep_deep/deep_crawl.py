from collections import deque
import json
import logging
from pathlib import Path
import math
import multiprocessing
import subprocess
import time
from typing import Any, Dict, Optional, List

from .crawl_utils import CrawlPaths, JsonLinesFollower, get_domain
from .dd_utils import BaseDDCrawlerProcess, is_running


class DDCrawlerPaths(CrawlPaths):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.out = self.root.joinpath('out')
        self.redis_conf = self.root.joinpath('redis.conf')
        self.login_credentials = self.root.joinpath('login_credentials.json')


class DeepCrawlerProcess(BaseDDCrawlerProcess):
    _jobs_root = Path('deep-jobs')
    paths_cls = DDCrawlerPaths

    def __init__(self, *args,
                 login_credentials=None,
                 in_flight_ttl=60,
                 idle_before_close=100,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.login_credentials = login_credentials or []
        if self.login_credentials:
            # convert from service API format to dd-crawler format
            self.login_credentials = [{
                'url': c['url'],
                'login': c.get('login', c['key_values']['login']),
                'password': c.get('password', c['key_values']['password']),
            } for c in self.login_credentials]
        self.idle_before_close = idle_before_close
        self._domain_stats = {
            get_domain(url): {
                'url': url,
                'pages_fetched': 0,
                'last_times': deque(maxlen=50),
            } for url in sorted(self.seeds, reverse=True)}
        # ^^ Reversed alphabetical to have shorter urls first
        # in case of several seeds from one domain.
        # Tracking domain state:
        self._in_flight = dict()  # type: Dict[str, float]
        self._in_flight_ttl = in_flight_ttl  # seconds
        self._have_successes = set()
        self._have_failures = set()
        self._open_queues = set()
        self._open_queues_t = time.time()

    @classmethod
    def load_running(
            cls, root: Path, **kwargs) -> Optional['DeepCrawlerProcess']:
        """ Initialize a process from a directory.
        """
        paths = DDCrawlerPaths(root)
        if not all(p.exists() for p in [
                paths.pid, paths.id, paths.seeds, paths.workspace_id]):
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
        return cls(
            pid=paths.pid.read_text(),
            id_=paths.id.read_text(),
            workspace_id=paths.workspace_id.read_text(),
            seeds=seeds,
            login_credentials=login_credentials,
            root=root,
            **kwargs)

    def start(self):
        assert self.pid is None
        self.paths.mkdir()
        self.paths.id.write_text(self.id_)
        self.paths.workspace_id.write_text(self.workspace_id)
        self.paths.seeds.write_text(
            '\n'.join(url for url in self.seeds), encoding='utf8')
        with self.paths.login_credentials.open('wt', encoding='utf8') as f:
            json.dump(self.login_credentials, f)
        n_processes = multiprocessing.cpu_count()
        if self.max_workers:
            n_processes = min(self.max_workers, n_processes)
        cur_dir = Path(__file__).parent  # type: Path
        compose_templates = (
            cur_dir.joinpath('deepcrawler-compose.template.yml').read_text())
        self.paths.root.joinpath('docker-compose.yml').write_text(
            compose_templates.format(
                docker_image=self.docker_image,
                page_limit=int(math.ceil(self.page_limit / n_processes)),
                external_links=self.external_links,
                proxy=self.proxy,
                idle_before_close=self.idle_before_close,
                **{p: self.to_host_path(getattr(self.paths, p)) for p in [
                    'seeds', 'redis_conf', 'out', 'login_credentials',
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

    def _get_updates(self) -> Dict[str, Any]:
        n_last = self.get_n_last()
        log_paths = list(self.paths.out.glob('*.log.jl'))
        updates = {}
        if log_paths:
            status = 'running'
            # TODO - 'pages': sample domains first, then get last per domain
            # in order to have something for each domain
            n_last_per_file = int(math.ceil(n_last / len(log_paths)))
            all_last_items = []
            for path in log_paths:
                follower = self._log_followers.setdefault(
                    path, JsonLinesFollower(path))
                last_items = deque(maxlen=n_last_per_file)
                for item in follower.get_new_items(at_least_last=True):
                    if 'url' in item:
                        last_items.append(item)
                        domain = get_domain(item['url'])
                        if domain in self._domain_stats:
                            s = self._domain_stats[domain]
                            s['pages_fetched'] += 1
                            # one domain should almost always be in one file
                            s['last_times'].append(item['time'])
                        if item.get('has_login_form'):
                            updates.setdefault('login_urls', [])\
                                   .append(item['url'])
                    if 'domain_state' in item:
                        self._track_domain_state(item)
                if last_items:
                    all_last_items.extend(last_items)
            all_last_items.sort(key=lambda x: x['time'])
            updates['pages'] = [{'url': it['url']}
                                for it in all_last_items[-n_last:]]
            pages_fetched = sum(s['pages_fetched']
                                for s in self._domain_stats.values())
            domains = self._get_domain_stats()
            rpm = sum(d['rpm'] for d in domains)
        else:
            rpm = pages_fetched = 0
            domains = []
            status = 'starting'
        updates['progress'] = {
            'status': status,
            'pages_fetched': pages_fetched,
            'rpm': rpm,
            'domains': domains,
        }
        return updates

    def _get_domain_stats(self) -> List[Dict]:
        return [{
            'url': s['url'],
            'domain': domain,
            'status': self._domain_status(domain),
            'pages_fetched': s['pages_fetched'],
            'rpm': get_rpm(s['last_times'])
        } for domain, s in self._domain_stats.items()]

    def _domain_status(self, domain: str) -> str:
        if domain in self._open_queues or domain in self._in_flight:
            return 'running'
        elif domain in self._have_successes:
            return 'finished'
        elif domain in self._have_failures:
            return 'failed'
        else:
            return 'running'  # really "starting", but we use "running" for now

    def _track_domain_state(self, item):
        ds = item['domain_state']
        self._have_successes.update(ds['worker_successes'])
        self._have_failures.update(ds['worker_failures'])
        if item['time'] > self._open_queues_t:
            self._open_queues = ds['global_open_queues']
            self._open_queues_t = item['time']
        for domain in ds['worker_in_flight']:
            if domain not in self._in_flight or (
                    self._in_flight[domain] < item['time']):
                self._in_flight[domain] = item['time']
        now = time.time()
        self._in_flight = {domain: t for domain, t in self._in_flight.items()
                           if now - t < self._in_flight_ttl}


def get_rpm(last_times):
    if len(last_times) < 2:
        return 0
    t_max = max(last_times)
    if time.time() - t_max > 100:  # no new pages for a while
        return 0
    dt = t_max - min(last_times)
    if dt < 1:  # not enough statistics
        return 0
    return len(last_times) / dt * 60
