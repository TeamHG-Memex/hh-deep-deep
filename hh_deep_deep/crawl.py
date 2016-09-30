import csv
import hashlib
import json
import logging
from pathlib import Path
import subprocess
from typing import Dict, List

import joblib


class CrawlProcess:
    jobs_root = Path('jobs')

    def __init__(self, *, id_: str, seeds: List[str], page_clf):
        self.pid = None
        self.id_ = id_
        self.seeds = seeds
        self.page_clf = page_clf
        self.root = (self.jobs_root /
                     hashlib.md5(id_.encode('utf8')).hexdigst()
                     ).absolute()

    @classmethod
    def load_all_running(cls) -> Dict[str, 'CrawlProcess']:
        """ Return a dictionary of currently running processes.
        """
        # TODO - load state from disk
        return {}

    def start(self):
        self.root.mkdir(exist_ok=True)
        self.root.joinpath('id.txt').write_text(self.id_)

        page_clf_path = str(self.root / 'page_clf.joblib')
        joblib.dump(self.page_clf, page_clf_path)

        seeds_path = str(self.root / 'seeds.csv')
        with open(seeds_path, 'wt') as f:
            csv.writer(f).writerows(self.seeds)

        log_path = str(self.root / 'spider.log')
        items_path = str(self.root / 'items.jl')
        args = [
            'scrapy', 'crawl', 'relevant',
            '-a', 'seeds_url={}'.format(seeds_path),
            '-a', 'checkpoint_path={}'.format(self.root),
            '-a', 'classifier_path={}'.format(page_clf_path),
            '-o', 'gzip:{}'.format(items_path),
            '--logfile', log_path,
            '-L', 'INFO',
            '-s', 'CLOSESPIDER_ITEMCOUNT=1000000',
        ]

        logging.info('Starting crawl in {}'.format(self.root))
        # TODO - get container name from docker
        subprocess.run(args, check=True)

    def stop(self):
        pass  # TODO


