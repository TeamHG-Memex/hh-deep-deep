from collections import deque
import csv
import json
import logging
from pathlib import Path
import re
import subprocess
import time
from typing import Any, Dict, Optional

from .crawl_utils import (
    CrawlPaths, CrawlProcess, gen_job_path, JsonLinesFollower)
from .dd_utils import DEFAULT_CRAWLER_PAGE_LIMIT


class DeepDeepPaths(CrawlPaths):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.page_clf = self.root.joinpath('page_clf.joblib')
        self.items = self.root.joinpath('items.jl')


DEFAULT_TRAINER_PAGE_LIMIT = 10000


class DeepDeepProcess(CrawlProcess):
    id_field = 'workspace_id'
    default_docker_image = 'deep-deep-hh'
    path_cls = DeepDeepPaths

    def __init__(self, *,
                 page_clf_data: bytes,
                 pid: str=None,
                 root: Path=None,
                 start_time: float=None,
                 checkpoint_interval: int=1000,
                 crawler_params: Dict=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.pid = pid
        self.page_limit = self.page_limit or DEFAULT_TRAINER_PAGE_LIMIT
        self.crawler_params = crawler_params
        self.paths = self.path_cls(
            root or gen_job_path(self.id_, self.jobs_root))
        self.log_follower = JsonLinesFollower(self.paths.items)
        self.page_clf_data = page_clf_data
        self.checkpoint_interval = checkpoint_interval
        self.start_time = start_time

    @classmethod
    def load_running(cls, root: Path, **kwargs) -> Optional['DeepDeepProcess']:
        """ Initialize a process from a directory.
        """
        paths = cls.path_cls(root)
        if not all(p.exists() for p in [
                paths.pid, paths.meta, paths.seeds, paths.page_clf]):
            return

        meta = json.loads(paths.meta.read_text('utf8'))
        pid = paths.pid.read_text()
        if not cls._is_running(pid):
            paths.pid.unlink()
            try:
                subprocess.check_output(['docker', 'rm', pid])
            except subprocess.CalledProcessError:
                pass
            return
        with paths.seeds.open('rt', encoding='utf8') as f:
            seeds = [url for url, in csv.reader(f)]
        return cls(
            pid=pid,
            id_=meta['id'],
            workspace_id=meta['workspace_id'],
            crawler_params=meta['crawler_params'],
            start_time=meta['start_time'],
            seeds=seeds,
            page_clf_data=paths.page_clf.read_bytes(),
            root=root,
            **kwargs)

    @staticmethod
    def _is_running(pid):
        try:
            inspect_result = json.loads(subprocess.check_output(
                ['docker', 'inspect', pid]).decode('utf8'))
        except subprocess.CalledProcessError:
            return False
        assert len(inspect_result) == 1
        state = inspect_result[0]['State']
        return bool(state.get('Running'))

    def is_running(self):
        return self.pid is not None and self._is_running(self.pid)

    def start(self):
        assert self.pid is None
        self.paths.mkdir()
        self.paths.page_clf.write_bytes(self.page_clf_data)
        with self.paths.seeds.open('wt', encoding='utf8') as f:
            csv.writer(f).writerows([url] for url in self.seeds)
        docker_args = [
            'docker', 'run', '-d',
            '-v', '{}:{}'.format(self.to_host_path(self.paths.root), '/job'),
            '-v', '{}:{}'.format(
                self.to_host_path(self.paths.models), '/models'),
            '--network', 'bridge',
        ]
        proxy = 'http://proxy:8118'
        if self.proxy_container:
            docker_args.extend(
                ['--link', '{}:proxy'.format(self.proxy_container)])
        if self.test_server_container:
            docker_args.extend(
                ['--link', '{}:test-server'.format(self.test_server_container)])
        docker_args.append(self.docker_image)
        args = docker_args + [
            'scrapy', 'crawl', 'relevant',
            '-a', 'seeds_url=/job/{}'.format(self.paths.seeds.name),
            '-a', 'checkpoint_path=/job',
            '-a', 'checkpoint_interval={}'.format(self.checkpoint_interval),
            '-a', 'classifier_path=/job/{}'.format(self.paths.page_clf.name),
            '-a', 'classifier_input=text_url',
            '-o', '/job/items.jl',
            '-a', 'export_cdr=0',
            '--logfile', '/job/spider.log',
            '-L', 'INFO',
            '-s', 'CLOSESPIDER_PAGECOUNT={}'.format(self.page_limit),
        ]
        if self.proxy_container:
            args.extend([
                '-s', 'HTTP_PROXY={}'.format(proxy),
                '-s', 'HTTPS_PROXY={}'.format(proxy),
            ])
        logging.info('Starting crawl in {}'.format(self.paths.root))
        self.pid = subprocess.check_output(args).decode('utf8').strip()
        self.start_time = time.time()
        self.paths.meta.write_text(json.dumps({
            'id': self.id_,
            'workspace_id': self.workspace_id,
            'crawler_params': self.crawler_params,
            'start_time': self.start_time,
        }), encoding='utf8')
        self.paths.pid.write_text(self.pid)
        logging.info('Crawl started, container id {}'.format(self.pid))

    def stop(self, verbose=False):
        assert self.pid is not None
        if verbose:
            try:
                subprocess.check_output(
                    ['docker', 'logs', '--tail', '30', self.pid])
            except subprocess.CalledProcessError:
                pass  # might be dead already
        try:
            subprocess.check_output(['docker', 'stop', self.pid])
        except subprocess.CalledProcessError:
            pass  # might be dead already
        logging.info('Crawl stopped, removing container')
        try:
            subprocess.check_call(['docker', 'rm', self.pid])
        except subprocess.CalledProcessError:
            pass  # might be removed already
        self.paths.pid.unlink()
        logging.info('Removed container id {}'.format(self.pid))
        self.pid = None

    def _get_updates(self) -> Dict[str, Any]:
        if not self.paths.items.exists():
            return {'progress': 'Crawl is not running yet'}
        n_last = self.get_n_last()
        last_items = deque(self.log_follower.get_new_items(), maxlen=n_last)
        if last_items:
            last_item = last_items[-1]
            progress = get_progress_from_item(last_item)
            pages = [get_sample_from_item(item) for item in last_items
                     if 'url' in item]
            response_received = last_item.get('response_received_count', 0)
            percentage_done = 100 * response_received / self.page_limit
            if is_trainer_started_by_crawler(self):
                progress = 'Trainer: {}'.format(progress)
                crawler_page_limit = self.crawler_params.get(
                    'page_limit', DEFAULT_CRAWLER_PAGE_LIMIT)
                trainer_ratio = (self.page_limit /
                                 (self.page_limit + crawler_page_limit))
                percentage_done *= trainer_ratio
            return {'progress': progress,
                    'pages': pages,
                    'percentage_done': percentage_done}
        return {}

    def get_model(self) -> Optional[bytes]:
        """ Return a data of the last model (if there is any), or None.
        """
        model_files = sorted(
            self.paths.root.glob('Q-*.joblib'),
            key=lambda p: int(re.match(r'Q-(\d+)\.joblib', p.name).groups()[0])
        )
        if model_files:
            model_file = model_files[-1]
            logging.info('Reading model from {}'.format(model_file))
            return model_file.read_bytes()


def is_trainer_started_by_crawler(process):
    return (isinstance(process, DeepDeepProcess) and
            process.crawler_params is not None)


def get_sample_from_item(item: Dict) -> Dict:
    page_item = {'url': item['url']}
    reward = item.get('reward')
    if reward is not None:
        page_item['score'] = 100 * reward
    return page_item


def get_progress_from_item(item):
    progress = (
        'Average score {score:.1f}, '
        '{pages:,} pages processed from {crawled_domains:,} domains '
        '({relevant_domains:,} relevant domains).'
        .format(
            pages=item.get('response_received_count', 0),
            crawled_domains=item.get('crawled_domains', 0),
            relevant_domains=item.get('relevant_domains', 0),
            score=(100 * item['return'] / item['t']) if item.get('t') else 0,
        )
    )
    return progress
