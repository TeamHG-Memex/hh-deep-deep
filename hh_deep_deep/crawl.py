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
                 deep_deep_image=None):
        self.pid = None
        self.id_ = id_
        self.seeds = seeds
        self.page_clf_data = page_clf_data
        self.root = (
            self.jobs_root /
            '{}_{}'.format(
                int(time.time()),
                hashlib.md5(id_.encode('utf8')).hexdigest()[:12]
            )).absolute()
        self.deep_deep_image = deep_deep_image or 'deep-deep'
        self.last_updates = None  # last update sent in self.get_updates
        self.last_model_file = None  # last model sent in self.get_new_model

    @classmethod
    def load_all_running(cls, **kwargs) -> Dict[str, 'CrawlProcess']:
        """ Return a dictionary of currently running processes.
        """
        # TODO - load state from disk
        return {}

    def start(self):
        self.root.mkdir(parents=True, exist_ok=True)
        self.root.joinpath('id.txt').write_text(self.id_)
        self.root.joinpath('page_clf.joblib').write_bytes(self.page_clf_data)
        with self.root.joinpath('seeds.csv').open('wt') as f:
            csv.writer(f).writerows([url] for url in self.seeds)
        args = [
            'docker', 'run', '-d',
            '-v', '{}:{}'.format(self.root, '/job'),
            self.deep_deep_image,
            'scrapy', 'crawl', 'relevant',
            '-a', 'seeds_url=/job/seeds.csv',
            '-a', 'checkpoint_path=/job',
            '-a', 'classifier_path=/job/page_clf.joblib',
            '-o', 'gzip:/job/items.jl',
            '-a', 'export_cdr=0',
            '--logfile', '/job/spider.log',
            '-L', 'INFO',
            '-s', 'CLOSESPIDER_ITEMCOUNT=1000000',
        ]
        logging.info('Starting crawl in {}'.format(self.root))
        self.pid = subprocess.check_output(args).decode('utf8').strip()
        logging.info('Crawl started, container id {}'.format(self.pid))
        self.root.joinpath('pid.txt').write_text(self.pid)

    def stop(self):
        if self.pid:
            subprocess.check_output(['docker', 'stop', self.pid])
            logging.info('Crawl stopped, removing container')
            subprocess.check_output(['docker', 'rm', self.pid])
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
        items_path = self.root / 'items.jl.gz'
        if not items_path.exists():
            return 'Craw is not running yet', []
        last_item = get_last_valid_item(str(items_path))
        if last_item is not None:
            url = last_item.pop('url', None)
            # TODO - format a nice message
            progress = '\n'.join(
                '{}: {}'.format(k, v) for k, v in last_item.items())
            return progress, ([url] if url else [])
        else:
            return 'Crawl started, no updates yet', []

    def get_new_model(self) -> Optional[bytes]:
        """ Return a data of the new model (if there is any), or None.
        """
        model_files = sorted(
            self.root.glob('Q-*.joblib'),
            key=lambda p: int(re.match(r'Q-(\d+)\.joblib', p.name).groups()[0])
        )
        if model_files:
            model_file = model_files[-1]
            if model_file != self.last_model_file:
                self.last_model_file = model_file
                return model_file.read_bytes()


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
