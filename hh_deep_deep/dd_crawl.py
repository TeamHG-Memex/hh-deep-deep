import logging
from pathlib import Path
import multiprocessing
import subprocess
from typing import Optional, Tuple, List

from .crawl_utils import CrawlPaths, CrawlProcess, gen_job_path


class DDCrawlerPaths(CrawlPaths):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.link_clf = self.root.joinpath('Q.joblib')
        self.out = self.root.joinpath('out')
        self.redis_conf = self.root.joinpath('redis.conf')


class DDCrawlerProcess(CrawlProcess):
    jobs_root = Path('dd-jobs')
    default_docker_image = 'dd-crawler'

    def __init__(self, *,
                 page_clf_data: bytes,
                 link_clf_data: bytes,
                 root: Path=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.paths = DDCrawlerPaths(
            root or gen_job_path(self.id_, self.jobs_root))
        self.page_clf_data = page_clf_data
        self.link_clf_data = link_clf_data

    @classmethod
    def load_running(cls, root: Path, **kwargs) -> Optional['DDCrawlerProcess']:
        """ Initialize a process from a directory.
        """
        return None  # TODO

    def start(self):
        assert self.pid is None
        self.paths.mkdir()
        self.paths.id.write_text(self.id_)
        self.paths.page_clf.write_bytes(self.page_clf_data)
        self.paths.link_clf.write_bytes(self.link_clf_data)
        self.paths.seeds.write_text('\n'.join(url for url in self.seeds))
        cur_dir = Path(__file__).parent  # type: Path
        compose_templates = (
            cur_dir.joinpath('dd-crawler-compose.template.yml').read_text())
        self.paths.root.joinpath('docker-compose.yml').write_text(
            compose_templates.format(
                docker_image=self.docker_image,
                seeds=self.paths.seeds.name,
                page_clf=self.paths.page_clf.name,
                link_clf=self.paths.link_clf.name,
                redis_conf=self.paths.redis_conf.name,
                out=self.paths.out.name,
            ))
        redis_config = cur_dir.joinpath('redis.conf').read_text()
        self.paths.redis_conf.write_text(redis_config)
        self._compose_cmd('up', '-d')
        n_processes = multiprocessing.cpu_count()
        self._compose_cmd('scale', 'crawler={}'.format(n_processes))
        self.pid = self.id_

    def stop(self):
        if self.pid:
            self._compose_cmd('down', '-v')
            logging.info('Crawl {} stopped'.format(self.pid))
            self.pid = None
        else:
            logging.info('Can not stop crawl: it is not running')

    def _get_updates(self) -> Tuple[str, List[str]]:
        return 'TODO', []

    def _compose_cmd(self, *args):
        subprocess.check_call(
            ['docker-compose'] + list(args), cwd=str(self.paths.root))
