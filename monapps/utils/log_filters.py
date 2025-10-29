import logging


class OnlyLocalModulesFilter(logging.Filter):
    # to pass messages only from the modules
    def filter(self, record):
        return record.name.startswith("#") or record.levelno > logging.INFO
