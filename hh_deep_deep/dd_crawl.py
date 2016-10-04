import logging
from pathlib import Path
import multiprocessing
import subprocess
from typing import Optional, Tuple, List

from .crawl_utils import CrawlPaths, CrawlProcess


class DDCrawlerProcess(CrawlProcess):
    jobs_root = Path('dd-jobs')
    default_docker_image = 'dd-crawler'

    def __init__(self, *,
                 page_clf_data: bytes,
                 link_clf_data: bytes,
                 **kwargs):
        super().__init__(**kwargs)
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
        compose_templates = (
            Path(__file__).parent.joinpath('dd-crawler-compose.template.yml')
            .read_text())
        self.paths.root.joinpath('docker-compose.yml').write_text(
            compose_templates.format(docker_image=self.docker_image))
        self.compose_cmd('up', '-d')
        n_processes = multiprocessing.cpu_count()
        self.compose_cmd('scale', 'scale', 'crawler={}'.format(n_processes))
        self.pid = self.id_

    def compose_cmd(self, *args):
        subprocess.check_call(
            ['docker-compose'] + args, cwd=str(self.paths.root))

    def stop(self):
        if self.pid:
            self.compose_cmd('down', '-v')
            self.pid = None
        else:
            logging.info('Can not stop crawl: it is not running')

    def _get_updates(self) -> Tuple[str, List[str]]:
        return 'TODO', []
