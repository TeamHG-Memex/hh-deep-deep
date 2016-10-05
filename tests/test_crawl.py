from hh_deep_deep.deepdeep_crawl import get_progress_from_item, get_sample_from_item


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
