# -*- coding: utf8 -*-
"""OpenObject RQ.
"""
from __future__ import absolute_import
from rq import get_current_connection, get_current_job


if get_current_connection() and get_current_job():
    # importing osv.osv it tries to parse args we need to remove, because
    # there are a conflict bettween rqworker and openerp-server args.
    import sys
    sys.argv = sys.argv[:1]

# Only import this if we are in OpenERP
from . import ir_cron
from . import oorq
from . import xmlrpc
from .exceptions import *
