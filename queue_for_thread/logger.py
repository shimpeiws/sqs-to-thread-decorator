from logging import getLogger, StreamHandler, Formatter, INFO


class Logger:
    def __init__(self, **options):
        logger = getLogger(__name__)
        logger.setLevel(options.get('log_level', INFO))
        stream_handler = StreamHandler()
        stream_handler.setLevel(self.log_level)
        handler_format = Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        stream_handler.setFormatter(handler_format)
        logger.addHandler(stream_handler)
        return logger
