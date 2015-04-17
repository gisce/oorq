# -*- coding: utf-8 -*-

from osv import osv
from rq import get_current_connection, get_current_job


class IrCron(osv.osv):
    _name = 'ir.cron'
    _inherit = 'ir.cron'

    def _poolJobs(self, db_name, check=False):
        """Check if we are a worker process.
        """
        if get_current_connection() and get_current_job():
            pass
        else:
            super(IrCron, self)._poolJobs(db_name, check)

IrCron()
