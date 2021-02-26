from rq.registry import FailedJobRegistry


class CursorWrapper(object):
    def __init__(self, cursor):
        self.cursor = cursor

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self.cursor.rollback()
        self.cursor.close()


def get_failed_queue():
    return FailedJobRegistry()
