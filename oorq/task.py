# -*- coding: utf-8 -*-
import logging
logging.disable(logging.CRITICAL)
import os
import signal
import sys
import threading
import traceback
import release
import netsvc
import tools
tools.config['db_name'] = 'openerp'
tools.config['addons_path'] = '/Users/eduard/Projects/OpenERP/server/bin/addons'
import pooler
import osv
import workflow
import report
import service
import addons

def execute(db, uid, obj, method, *args, **kw):
    res = None
    opool = osv_pool()
    res = opool.execute(dbname, uid, obj, method, *args, **kw)
    return res