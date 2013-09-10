# -*- coding: utf8 -*-
"""OpenObject RQ.
"""
# Only import this if we are in OpenERP
try:
    import from openerp import netsvc
    import ir_cron
    import oorq
    from exceptions import *
except:
    pass
