# RQ for OpenObject

Using [python-rq](http://python-rq.org/) for OpenObject tasks.

## API compatibility

 * For OpenERP v5 the module versions are `v1.X.X` or below and branch is `api_v5` [![Build Status](https://travis-ci.org/gisce/oorq.png?branch=api_v5)](https://travis-ci.org/gisce/oorq)
 * For OpenERP v6 the module versions are `v2.X.X` and branch is `api_v6` [![Build Status](https://travis-ci.org/gisce/oorq.png?branch=api_v6)](https://travis-ci.org/gisce/oorq)
 * For OpenERP v7 the module versions are `v3.X.X` and branch is `api_v7` [![Build Status](https://travis-ci.org/gisce/oorq.png?branch=api_v7)](https://travis-ci.org/gisce/oorq)

## DB compatibility (Tested)

- Redis (3, 5, 7, 8M)
- Valkey (8)

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
$ PYTHONPATH=~/Projects/OpenERP/server/bin:~/Projects/OpenERP/server/bin/addons rq worker
```

The default worker keeps RQ's process isolation and executes every job in a
forked work horse. The stable `oorq.worker.Worker` entry point can select a
persistent process from the ERP configuration after `tools.config.parse()`:

```ini
[options]
oorq_persistent_worker = true
```

The default is false. Accepted true values are `true`, `1`, `yes` and `on`;
accepted false values are `false`, `0`, `no` and `off` (case-insensitive).
Absent, empty and invalid values select `NoPersistentWorker`. As with other ERP
options, `OPENERP_OORQ_PERSISTENT_WORKER` overrides the configuration file.
Startup logs report the effective strategy.

Homogeneous queues whose jobs reliably clean up all SQL, ERP context, sudo and
logging state can also select the persistent implementation explicitly:

```sh
$ PYTHONPATH=~/Projects/OpenERP/server/bin:~/Projects/OpenERP/server/bin/addons \
    rq worker -w oorq.worker.PersistentWorker queue_name
```

`PersistentWorker` uses RQ's `SimpleWorker`, so consecutive jobs can reuse
in-memory ERP caches. A leaked transaction or process-global context can also
affect the next job. Keep the default `oorq.worker.Worker` for heterogeneous,
memory-heavy, untrusted or native-code jobs, and roll out persistent workers
gradually under a process supervisor.

For immediate rollback, set `oorq_persistent_worker = false` or bypass automatic
selection explicitly with `rq worker -w oorq.worker.NoPersistentWorker`. Use
`rq worker -w oorq.worker.PersistentWorker` only for diagnosis or an intentional
opt-in. Production activation remains blocked on the hardening, recycling and
observability work tracked in issue #144.

**Do fun things :)**
