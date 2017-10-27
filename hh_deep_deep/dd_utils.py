"""
Utils related to dd-crawler.
"""
import json
import logging
from pathlib import Path
import re
import subprocess
from typing import Dict

from .crawl_utils import (
    CrawlPaths, CrawlProcess, gen_job_path, JsonLinesFollower, get_domain)


class BaseDDPaths(CrawlPaths):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.out = self.root.joinpath('out')
        self.redis_conf = self.root.joinpath('redis.conf')
        self.login_credentials = self.root.joinpath('login_credentials.json')


DEFAULT_CRAWLER_PAGE_LIMIT = 10000000


class BaseDDCrawlerProcess(CrawlProcess):
    default_docker_image = 'dd-crawler-hh'
    paths_cls = BaseDDPaths
    crawler_name = None

    def __init__(self, *,
                 root: Path=None,
                 max_workers: int=None,
                 login_credentials=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.paths = self.paths_cls(
            root or gen_job_path(self.id_, self.jobs_root))
        self.max_workers = max_workers
        self.page_limit = self.page_limit or DEFAULT_CRAWLER_PAGE_LIMIT
        self.login_credentials = login_credentials or []
        if self.login_credentials:
            # convert from service API format to dd-crawler format
            self.login_credentials = [{
                'url': c['url'],
                'login': c.get('login', c['key_values']['login']),
                'password': c.get('password', c['key_values']['password']),
                'id': c['id'],
            } for c in self.login_credentials]
        self._log_followers = {}  # type: Dict[Path, JsonLinesFollower]
        self._login_cred_ids = {}  # type: Dict[str, str]
        for c in self.login_credentials:
            self._login_cred_ids[get_domain(c['url'])] = c['id']
        self._login_state = {}  # type: Dict[str, str]

    def is_running(self):
        return is_running(self.paths.root)

    def stop(self, verbose=False):
        if verbose:
            self._compose_call('logs', '--tail', '30')
        self._compose_call('down', '-v')
        self.paths.pid.unlink()
        logging.info('Crawl "{}" stopped'.format(self.id_))

    def handle_login(self, *, url, login, password, cred_id):
        self._login_cred_ids[get_domain(url)] = cred_id
        self._scrapy_command('login', url, login, password)

    def _add_login_state_update(self, item, updates):
        cred_id = self._login_cred_ids.get(get_domain(item['url']))
        if cred_id is not None:
            state = 'success' if item['login_success'] else 'failed'
            if self._login_state.get(cred_id) != state:
                updates.setdefault('login_results', []).append((cred_id, state))
                self._login_state[cred_id] = state

    @property
    def external_links(self) -> str:
        """ External links in docker-compose format. """
        external_links = []
        if self.proxy_container:
            external_links.append('{}:proxy'.format(self.proxy_container))
        if self.test_server_container:
            external_links.extend(
                '{}:test-server-{}'.format(self.test_server_container, i + 1)
                for i in range(3))
        return json.dumps(external_links)

    @property
    def proxy(self):
        return 'http://proxy:8118' if self.proxy_container else ''

    def _scrapy_command(self, command, *args):
        # Find which crawler is still alive (some might finish earlier)
        ps_output = subprocess.check_output(
            ['docker-compose', 'ps'], cwd=str(self.paths.root))
        ps_output = ps_output.decode('ascii', 'replace')
        index = '1'
        for line in ps_output.split('\n'):
            m = re.match(r'[\da-f]+_crawler_(\d+)\s', line)
            if m:
                index = m.groups()[0]
                break
        self._compose_call(
            'exec', '-T', '--index', index,
            'crawler', 'scrapy', command, self.crawler_name, *args,
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
