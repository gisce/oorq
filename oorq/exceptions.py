# -*- coding: utf-8 -*-

class OORQException(Exception):
    """Base OORQ Exception.
    """
    pass

class OORQNotIds(OORQException):
    """Not a standard function with ids parameter.
    """
    pass
