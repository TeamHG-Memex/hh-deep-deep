from pathlib import Path

from .crawl_utils import CrawlPaths, CrawlProcess


class DDCrawlerPaths(CrawlPaths):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.out = self.root.joinpath('out')
        self.redis_conf = self.root.joinpath('redis.conf')


class DeepCrawlerProcess(CrawlProcess):
    _jobs_root = Path('deep-jobs')
    default_docker_image = 'dd-crawler-hh'

    def __init__(self, *,
                 root: Path=None,
                 max_workers: int=None,
                 **kwargs):
        super().__init__(**kwargs)
        self.paths = DDCrawlerPaths(
            root or gen_job_path(self.id_, self.jobs_root))
        self.max_workers = max_workers
        self.page_limit = self.page_limit or 10000000
        self._log_followers = {}  # type: Dict[Path, JsonLinesFollower]
