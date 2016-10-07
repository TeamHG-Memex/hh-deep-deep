import csv
from pathlib import Path
import os
import tempfile

from hh_deep_deep.deepdeep_crawl import get_progress_from_item, get_sample_from_item
from hh_deep_deep.dd_crawl import get_last_csv_items


item = {
    't': 6596,
    'ts': 1475576323.389498,
    'todo': 831657,
    'eps-policy': None,
    'is_seed': False,
    'domains_open': 32958,
    'dropped': 0,
    'Q': 1.3928,
    'return': 3719.2005295396293,
    'processed': 6109,
    'crawled_domains': 50,
    'relevant_domains': 21,
    'reward': 0.8025,
    'enqueued': 837766,
    'domains_closed': 0,
    '_type': 'stats',
    'rss': 6742257664,
    'url': 'http://example.com',
}


def test_get_progress_from_item():
    progress = get_progress_from_item(item)
    assert progress == (
        '6,109 pages processed from 50 domains (21 relevant), '
        'average score 56.4, '
        '837,766 requests enqueued, 32,958 domains open.')

    progress = get_progress_from_item({})
    assert progress == (
        '0 pages processed from 0 domains (0 relevant), '
        'average score 0.0, '
        '0 requests enqueued, 0 domains open.')


def test_get_sample_from_item():
    sample = get_sample_from_item(item)
    assert sample == {'url': 'http://example.com', 'score': 80.25}


def test_get_last_csv_items():
    data = [['zero', '0']] * 10 + [['one', '1'], ['two,!', '2'], ['broken']]
    with tempfile.NamedTemporaryFile('wt', delete=False) as f:
        csv.writer(f).writerows(data)
    try:
        assert get_last_csv_items(Path(f.name), 1, exp_len=1) == [['broken']]
        assert get_last_csv_items(Path(f.name), 1, exp_len=2) == data[-2:-1]
        assert get_last_csv_items(Path(f.name), 2, exp_len=2) == data[-3:-1]
        assert get_last_csv_items(Path(f.name), 2, exp_len=3) == []
    finally:
        os.unlink(f.name)
