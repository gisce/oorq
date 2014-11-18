# -*- coding: utf-8 -*-

import netsvc
from osv import osv
from rq import get_current_job


class IrCron(osv.osv):
    _name = 'ir.cron'
    _inherit = 'ir.cron'

    def _poolJobs(self, db_name, check=False):
        """Check if we are a worker process.
        """
        im_a_worker = netsvc.SERVICES.get('im_a_worker', False)
        if not get_current_job() and not im_a_worker:
            super(IrCron, self)._poolJobs(db_name, check)

IrCron()
