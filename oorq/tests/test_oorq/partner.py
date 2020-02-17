from osv import osv
from oorq.decorators import job, split_job
from oorq.decorators import create_jobs_group
from oorq.oorq import AsyncMode
from rq.job import Job


class ResPartner(osv.osv):
    _name = 'res.partner'
    _inherit = 'res.partner'

    def test_write_async(self, cursor, uid, ids, vals, context=None):
        self.write_async(cursor, uid, ids, vals, context)
        return True

    def test_write_split(self, cursor, uid, ids, vals, context=None):
        self.write_split(cursor, uid, ids, vals, context)
        return True

    def test_write_split_size(self, cursor, uid, ids, vals, context=None):
        self.write_split_size(cursor, uid, ids, vals, context)
        return True

    def test_dependency_job(self, cursor, uid, ids, vals, context=None):
        self.dependency_job(cursor, uid, ids, vals, context=context)
        return

    def test_dependency_job_on_commit(self, cursor, uid, ids, vals, context=None):
        self.dependency_job_on_commit(cursor, uid, ids, vals, context=context)
        return True

    def test_no_enqueue_on_rollback(self, cursor, uid, ids, vals, context=None):
        self.write_async(cursor, uid, ids, vals, context)
        raise osv.except_osv('Error', 'Test error!')

    @job(async=True, queue='default', on_commit=True)
    def write_async(self, cr, user, ids, vals, context=None):
        #TODO: process before updating resource
        res = super(ResPartner, self).write(cr, user, ids, vals, context)
        import time
        time.sleep(1)
        return res
    
    @split_job(n_chunks=4, isolated=True)
    def write_split(self, cursor, uid, ids, vals, context=None):
        if 5 in ids:
            raise Exception("We want to fail!")
        res = super(ResPartner, self).write(cursor, uid, ids, vals, context)
        return res

    @split_job(chunk_size=1)
    def write_split_size(self, cursor, uid, ids, vals, context=None):
        res = super(ResPartner, self).write(cursor, uid, ids, vals, context)
        return res

    @job(queue='dependency')
    def dependency_job(self, cursor, uid, ids, vals, context=None):
        print "First job"
        import time
        self.write_async(cursor, uid, ids, vals, context=context)
        print "I'm working and not affected for the subjob"
        time.sleep(10)
        return True

    @job(queue='dependency', on_commit=True)
    def dependency_job_on_commit(self, cursor, uid, ids, vals, context=None):
        print "First job"
        import time
        self.write_async(cursor, uid, ids, vals, context=context)
        print "I'm working and not affected for the subjob"
        time.sleep(5)
        return True

    def massive_write(self, cursor, uid, context=None):
        ids = self.search(cursor, uid, [])
        jobs_ids = []
        for p_id in ids:
            j = self.write_async(cursor, uid, [p_id], {'active': 1})
            jobs_ids.append(j.id)
        create_jobs_group(cursor.dbname, uid, 'Massive write', 'res.partner.massive.write', jobs_ids)
        return True

    def write_not_async(self, cursor, uid, ids, values, context=None):
        if context is None:
            context = {}

        with AsyncMode(mode='sync'):
            res = self.write_async(cursor, uid, ids, values, context)
            assert isinstance(res, Job)
        return res.result


ResPartner()
