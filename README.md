# RQ for OpenObject

Using [python-rq](http://python-rq.org) for OpenObject tasks.

## API compatibility

 * For OpenERP v5 the module versions are `v1.X.X` or below and branch is `api_v5` [![Build Status](https://travis-ci.org/gisce/oorq.png?branch=api_v5)](https://travis-ci.org/gisce/oorq)
 * For OpenERP v6 the module versions are `v2.X.X` and branch is `api_v6` [![Build Status](https://travis-ci.org/gisce/oorq.png?branch=api_v6)](https://travis-ci.org/gisce/oorq)
 * For OpenERP v7 the module versions are `v3.X.X` and branch is `api_v7` [![Build Status](https://travis-ci.org/gisce/oorq.png?branch=api_v7)](https://travis-ci.org/gisce/oorq)

## Example to do a async write.

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
$ export PYTHONPATH=~/Projects/OpenERP/server/bin:~/Projects/OpenERP/server/bin/addons
$ rq worker
```

**Do fun things :)**
