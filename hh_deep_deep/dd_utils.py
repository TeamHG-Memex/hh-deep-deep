"""
Utils related to dd-crawler.
"""
import json
import logging
from pathlib import Path
import subprocess
from typing import Dict

from .crawl_utils import (
    CrawlPaths, CrawlProcess, gen_job_path, JsonLinesFollower)


class BaseDDCrawlerProcess(CrawlProcess):
    default_docker_image = 'dd-crawler-hh'
    paths_cls = CrawlPaths

    def __init__(self, *,
                 root: Path=None,
                 max_workers: int=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.paths = self.paths_cls(
            root or gen_job_path(self.id_, self.jobs_root))
        self.max_workers = max_workers
        self.page_limit = self.page_limit or 10000000
        self._log_followers = {}  # type: Dict[Path, JsonLinesFollower]

    def is_running(self):
        return self.pid is not None and is_running(self.paths.root)

    def stop(self, verbose=False):
        assert self.pid is not None
        if verbose:
            self._compose_call('logs', '--tail', '30')
        self._compose_call('down', '-v')
        self.paths.pid.unlink()
        logging.info('Crawl "{}" stopped'.format(self.pid))
        self.pid = None

    def handle_login(self, url, login, password):
        self._scrapy_command('login', url, login, password)

    def _scrapy_command(self, command, *args):
        self._compose_call(
            'exec', '-T', 'crawler', 'scrapy', command, 'deepdeep', *args,
            '-s', 'REDIS_HOST=redis', '-s', 'LOG_LEVEL=WARNING')

    def _compose_call(self, *args):
        subprocess.check_call(
            ['docker-compose'] + list(args), cwd=str(self.paths.root))


def is_running(root: Path) -> bool:
    running_containers = list(filter(
        None,
        subprocess
        .check_output(['docker-compose', 'ps', '-q'], cwd=str(root))
        .decode('utf8').strip().split('\n')))
    crawl_running = 0  # only really running crawlers
    for cid in running_containers:
        try:
            output = json.loads(subprocess.check_output(
                ['docker', 'inspect', cid]).decode('utf-8'))
        except (subprocess.CalledProcessError, ValueError):
            pass
        else:
            if len(output) == 1:
                meta = output[0]
                if 'crawler' in meta.get('Name', ''):
                    crawl_running += meta.get('State', {}).get('Running')
    return crawl_running > 0
