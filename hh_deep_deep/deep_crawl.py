from collections import defaultdict, deque
import logging
from pathlib import Path
import math
import multiprocessing
import subprocess
import time
from typing import Any, Dict, Optional

from .crawl_utils import CrawlPaths, JsonLinesFollower, get_domain
from .dd_utils import BaseDDCrawlerProcess, is_running


class DDCrawlerPaths(CrawlPaths):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.out = self.root.joinpath('out')
        self.redis_conf = self.root.joinpath('redis.conf')


class DeepCrawlerProcess(BaseDDCrawlerProcess):
    _jobs_root = Path('deep-jobs')
    paths_cls = DDCrawlerPaths

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._domain_stats = defaultdict(lambda: {
            'pages_fetched': 0,
            'last_times': deque(maxlen=50),
            'main_url': None,
        })

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
        return cls(
            pid=paths.pid.read_text(),
            id_=paths.id.read_text(),
            workspace_id=paths.workspace_id.read_text(),
            seeds=seeds,
            root=root,
            **kwargs)

    def start(self):
        assert self.pid is None
        self.paths.mkdir()
        self.paths.id.write_text(self.id_)
        self.paths.workspace_id.write_text(self.workspace_id)
        self.paths.seeds.write_text(
            '\n'.join(url for url in self.seeds), encoding='utf8')
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
                external_links=('["{}:proxy"]'.format(self.proxy_container)
                                if self.proxy_container else '[]'),
                proxy='http://proxy:8118' if self.proxy_container else '',
                **{p: self.to_host_path(getattr(self.paths, p)) for p in [
                    'seeds', 'redis_conf', 'out',
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
                    last_items.append(item)
                    s = self._domain_stats[get_domain(item['url'])]
                    if (s['main_url'] is None or
                            len(item['url']) < len(s['main_url'])):
                        s['main_url'] = item['url']
                    s['pages_fetched'] += 1
                    # one domain should almost always be in one file
                    s['last_times'].append(item['time'])
                if last_items:
                    all_last_items.extend(last_items)
            all_last_items.sort(key=lambda x: x['time'])
            updates['pages'] = [{'url': it['url']}
                                for it in all_last_items[-n_last:]]
            pages_fetched = sum(
                s['pages_fetched'] for s in self._domain_stats.values())
            domains = [{
                'url': s['main_url'] or 'http://{}'.format(domain),
                'domain': domain,
                'status': 'running',  # TODO - this needs dd-crawler features
                'pages_fetched': s['pages_fetched'],
                'rpm': get_rpm(s['last_times'])
            } for domain, s in self._domain_stats.items()]
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
