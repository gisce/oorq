from osv import osv
from oorq.decorators import job, split_job

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

    @job(async=True, queue='default')
    def write_async(self, cr, user, ids, vals, context=None):
        #TODO: process before updating resource
        res = super(ResPartner, self).write(cr, user, ids, vals, context)
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

ResPartner()
