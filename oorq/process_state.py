"""Process-local lifecycle signals shared by tasks and persistent workers."""

_recycle_reason = None


def request_recycle(reason):
    global _recycle_reason
    if _recycle_reason is None:
        _recycle_reason = reason


def consume_recycle_reason():
    global _recycle_reason
    reason = _recycle_reason
    _recycle_reason = None
    return reason
