# RQ for OpenObject

Using [python-rq](http://www.python-rq.org) for OpenObject tasks.

Example to do a async write.

### Add the decorator to the function

```python
from osv import osv
from oorq.decorators import job

class ResPartner(osv.osv):
    _name = 'res.partner'
    _inherit = 'res.partner'

    @job(async=True)
    def write(self, cursor, user, ids, vals, context=None):
        res = super(ResPartner,
                    self).write(cursor, user, ids, vals, context)
        return res

ResPartner()
```

### Start the worker

```sh
$ rqworker --path ~/Projects/OpenERP/server/bin --path ~/Projects/OpenERP/server/bin/addons
```

**Do fun things :)**