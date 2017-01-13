from collections import deque
import csv
import json
import logging
from pathlib import Path
import re
import subprocess
from typing import Dict, List, Optional, Tuple

from .crawl_utils import CrawlPaths, CrawlProcess, gen_job_path, get_last_lines


class DeepDeepPaths(CrawlPaths):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.items = self.root.joinpath('items.jl')


class DeepDeepProcess(CrawlProcess):
    _jobs_root = Path('deep-deep-jobs')
    default_docker_image = 'deep-deep-hh'

    def __init__(self, *,
                 page_clf_data: bytes,
                 root: Path=None,
                 checkpoint_interval: int=1000,
                 **kwargs):
        super().__init__(**kwargs)
        self.paths = DeepDeepPaths(root or gen_job_path(self.id_, self.jobs_root))
        self.page_clf_data = page_clf_data
        self.checkpoint_interval = checkpoint_interval
        # last model sent in self.get_new_model
        self.last_model_file = None  # type: Path

    @classmethod
    def load_running(cls, root: Path, **kwargs) -> Optional['DeepDeepProcess']:
        """ Initialize a process from a directory.
        """
        paths = DeepDeepPaths(root)
        if not all(p.exists() for p in [
                paths.pid, paths.id, paths.seeds, paths.page_clf]):
            return
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
            id_=paths.id.read_text(),
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
        self.paths.id.write_text(self.id_)
        self.paths.page_clf.write_bytes(self.page_clf_data)
        with self.paths.seeds.open('wt', encoding='utf8') as f:
            csv.writer(f).writerows([url] for url in self.seeds)
        args = [
            'docker', 'run', '-d',
            '-v', '{}:{}'.format(self.to_host_path(self.paths.root), '/job'),
            '-v', '{}:{}'.format(self.to_host_path(self.paths.models), '/models'),
            self.docker_image,
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
            '-s', 'CLOSESPIDER_ITEMCOUNT=200000',
        ]
        logging.info('Starting crawl in {}'.format(self.paths.root))
        self.pid = subprocess.check_output(args).decode('utf8').strip()
        logging.info('Crawl started, container id {}'.format(self.pid))
        self.paths.pid.write_text(self.pid)

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

    def _get_updates(self) -> Tuple[str, List[Dict]]:
        if not self.paths.items.exists():
            return 'Craw is not running yet', []
        n_last = self.get_n_last()
        last_items = get_last_valid_jl_items(self.paths.items, n_last)
        if last_items:
            item_progress = get_progress_from_item(last_items[-1])
            pages = [get_sample_from_item(item) for item in last_items
                     if 'url' in item]
            if self.last_model_file:
                model_progress = ('Last deep-deep model checkpoint {}.'
                                  .format(self.last_model_file.name))
            else:
                model_progress = 'No model checkpoints yet.'
            progress = '{} {}'.format(item_progress, model_progress)
            return progress, pages
        else:
            return 'Crawl started, no updates yet', []

    def get_new_model(self) -> Optional[bytes]:
        """ Return a data of the new model (if there is any), or None.
        """
        model_files = sorted(
            self.paths.root.glob('Q-*.joblib'),
            key=lambda p: int(re.match(r'Q-(\d+)\.joblib', p.name).groups()[0])
        )
        if model_files:
            model_file = model_files[-1]
            if model_file != self.last_model_file:
                logging.info('Sending new model from {}'.format(model_file))
                self.last_model_file = model_file
                return model_file.read_bytes()


def get_sample_from_item(item: Dict) -> Dict:
    page_item = {'url': item['url']}
    reward = item.get('reward')
    if reward is not None:
        page_item['score'] = 100 * reward
    return page_item


def get_progress_from_item(item):
    progress = (
        '{pages:,} pages processed from {crawled_domains:,} domains '
        '({relevant_domains:,} relevant domains), '
        'average score {score:.1f}, '
        '{enqueued:,} requests enqueued, {domains_open:,} domains open.'
        .format(
            pages=item.get('processed', 0),
            crawled_domains=item.get('crawled_domains', 0),
            relevant_domains=item.get('relevant_domains', 0),
            score=(100 * item['return'] / item['t']) if item.get('t') else 0,
            enqueued=item.get('enqueued', 0),
            domains_open=item.get('domains_open', 0),
        )
    )
    return progress


def get_last_valid_jl_items(path: Path, n_last: int) -> List[Dict]:
    last_lines = get_last_lines(path, n_last + 1)
    last_items = []
    for line in last_lines:
        try:
            last_items.append(json.loads(line))
        except Exception:
            pass
    return last_items[-n_last:]
