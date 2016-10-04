from hh_deep_deep.crawl import get_updates_from_item


def test_get_updates_from_item():
    item = {
        'ts': 1475576323.389498,
        'todo': 831657,
        'eps - policy': None,
        'is_seed': False,
        'domains_open': 32958,
        'dropped': 0,
        'Q': 1.3928,
        'return': 3719.2005295396293,
        'processed': 6109,
        'reward': 0.8025,
        'enqueued': 837766,
        'domains_closed': 0,
        '_type': 'stats',
        'rss': 6742257664,
        'url': 'http://example.com',
    }
    progress, pages = get_updates_from_item(item)
    assert isinstance(progress, str)
    assert pages == [{'url': 'http://example.com', 'score': 80.25}]
