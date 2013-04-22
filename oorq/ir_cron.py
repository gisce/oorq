# -*- coding: utf-8 -*-

from osv import osv
from rq import get_current_job


class IrCron(osv.osv):
    _name = 'ir.cron'
    _inherit = 'ir.cron'

    def _poolJobs(self, db_name, check=False):
        """Check if we are a worker process.
        """
        if not get_current_job():
            super(IrCron, self)._poolJobs(db_name, check)

IrCron()
