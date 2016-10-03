import logging


def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(module)s: %(message)s')


def log_ignore_exception(fn):
    try:
        name = fn.__name__
    except AttributeError:
        name = fn.__func__.__name__

    def deco(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            logging.error('Error in {}'.format(name), exc_info=e)

    return deco
