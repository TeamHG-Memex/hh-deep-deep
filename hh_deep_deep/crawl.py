import csv
import hashlib
import logging
from pathlib import Path
import subprocess
from typing import Dict, List


class CrawlProcess:
    jobs_root = Path('jobs')

    def __init__(self, *, id_: str, seeds: List[str], page_clf_data: bytes,
                 deep_deep_image=None):
        self.pid = None
        self.id_ = id_
        self.seeds = seeds
        self.page_clf_data = page_clf_data
        self.root = (self.jobs_root /
                     hashlib.md5(id_.encode('utf8')).hexdigest()
                     ).absolute()
        self.deep_deep_image = deep_deep_image or 'deep-deep'

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
            csv.writer(f).writerows(self.seeds)
        args = [
            'docker', 'run', '-d',
            '-v', '{}:{}'.format(self.root, '/job'),
            self.deep_deep_image,
            'scrapy', 'crawl', 'relevant',
            '-a', 'seeds_url=/job/seeds.txt',
            '-a', 'checkpoint_path=/job',
            '-a', 'classifier_path=/job/page_clf.joblib',
            '-o', 'gzip:/job/items.jl',
            '--logfile', '/job/spider.log',
            '-L', 'INFO',
            '-s', 'CLOSESPIDER_ITEMCOUNT=1000000',
        ]
        logging.info('Starting crawl in {}'.format(self.root))
        self.pid = subprocess.check_output(args).decode('utf8')
        logging.info('Crawl started, container id {}'.format(self.pid))
        self.root.joinpath('pid.txt').write_text(self.pid)

    def stop(self):
        pass  # TODO


