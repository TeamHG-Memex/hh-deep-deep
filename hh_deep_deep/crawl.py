import csv
import gzip
import json
import hashlib
import logging
from pathlib import Path
import re
import subprocess
import time
from typing import Dict, List, Optional, Tuple


class CrawlProcess:
    jobs_root = Path('jobs')

    def __init__(self, *, id_: str, seeds: List[str], page_clf_data: bytes,
                 deep_deep_image=None, pid=None, root=None):
        self.pid = pid
        self.id_ = id_
        self.seeds = seeds
        self.page_clf_data = page_clf_data
        root = root or self.jobs_root.joinpath(
            '{}_{}'.format(
                int(time.time()),
                hashlib.md5(id_.encode('utf8')).hexdigest()[:12]
            ))
        self.paths = CrawlPaths(root)
        self.deep_deep_image = deep_deep_image or 'deep-deep'
        self.last_updates = None  # last update sent in self.get_updates
        self.last_model_file = None  # last model sent in self.get_new_model

    @classmethod
    def load_all_running(cls, **kwargs) -> Dict[str, 'CrawlProcess']:
        """ Return a dictionary of currently running processes.
        """
        running = {}
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
        paths = CrawlPaths(root)
        if not all(p.exists() for p in [
                paths.id, paths.pid, paths.seeds, paths.page_clf]):
            return
        pid = paths.pid.read_text()
        try:
            inspect_result = json.loads(subprocess.check_output(
                ['docker', 'inspect', pid]).decode('utf8'))
        except subprocess.CalledProcessError:
            paths.pid.unlink()
            return
        assert len(inspect_result) == 1
        state = inspect_result[0]['State']
        if not state.get('Running'):
            # Remove stopped crawl container and pid file
            paths.pid.unlink()
            subprocess.check_output(['docker', 'rm', pid])
            return
        with paths.seeds.open('rt') as f:
            seeds = [url for url, in csv.reader(f)]
        return cls(
            pid=pid,
            id_=paths.id.read_text(),
            seeds=seeds,
            page_clf_data=paths.page_clf.read_bytes(),
            root=root,
            **kwargs)

    def start(self):
        assert self.pid is None
        self.paths.root.mkdir(parents=True, exist_ok=True)
        self.paths.id.write_text(self.id_)
        self.paths.page_clf.write_bytes(self.page_clf_data)
        with self.paths.seeds.open('wt') as f:
            csv.writer(f).writerows([url] for url in self.seeds)
        args = [
            'docker', 'run', '-d',
            '-v', '{}:{}'.format(self.paths.root, '/job'),
            self.deep_deep_image,
            'scrapy', 'crawl', 'relevant',
            '-a', 'seeds_url=/job/{}'.format(self.paths.seeds.name),
            '-a', 'checkpoint_path=/job',
            '-a', 'classifier_path=/job/{}'.format(self.paths.page_clf.name),
            '-o', 'gzip:/job/items.jl',
            '-a', 'export_cdr=0',
            '--logfile', '/job/spider.log',
            '-L', 'INFO',
            '-s', 'CLOSESPIDER_ITEMCOUNT=1000000',
        ]
        logging.info('Starting crawl in {}'.format(self.paths.root))
        self.pid = subprocess.check_output(args).decode('utf8').strip()
        logging.info('Crawl started, container id {}'.format(self.pid))
        self.paths.pid.write_text(self.pid)

    def stop(self):
        if self.pid:
            subprocess.check_output(['docker', 'stop', self.pid])
            logging.info('Crawl stopped, removing container')
            subprocess.check_output(['docker', 'rm', self.pid])
            self.paths.pid.unlink()
            logging.info('Removed container id {}'.format(self.pid))
            self.pid = None
        else:
            logging.info('Can not stop crawl: it is not running')

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
        if not self.paths.items.exists():
            return 'Craw is not running yet', []
        last_item = get_last_valid_item(str(self.paths.items))
        if last_item is not None:
            return get_updates_from_item(last_item)
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
                self.last_model_file = model_file
                return model_file.read_bytes()


class CrawlPaths:
    def __init__(self, root: Path):
        root = root.absolute()
        self.root = root
        self.id = root.joinpath('id.txt')
        self.pid = root.joinpath('pid.txt')
        self.page_clf = root.joinpath('page_clf.joblib')
        self.seeds = root.joinpath('seeds.csv')
        self.items = root.joinpath('items.jl.gz')


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


def get_updates_from_item(item):
    url = item.pop('url', None)
    if url:
        page_item = {'url': url}
        reward = item.pop('reward', None)  # type: Optional[float]
        if reward is not None:
            page_item['score'] = 100 * reward
        pages = [page_item]
    else:
        pages = []
    progress = (
        '{pages:,} pages processed from {domains_processed:,} domains, '
        'average score {score:.1f}, '
        '{enqueued:,} requests enqueued, {domains_open:,} domains open.'
        .format(
            pages=item.get('processed', 0),
            domains_processed=item.get('domains_processed', 0),
            score=(100 * item['return'] / item['t']) if item.get('t') else 0,
            enqueued=item.get('enqueued', 0),
            domains_open=item.get('domains_open', 0),
        )
    )
    return progress, pages
