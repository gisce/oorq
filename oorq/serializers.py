# coding=utf-8
import logging
from rq.job import Job
from six.moves import xmlrpc_client
from signals import APP_STARTED
from flask import current_app


logger = logging.getLogger('openerp.oorq')


def register_json_encoder(*args, **kwargs):

    class FlaskJsonEncoder(current_app.json_encoder):
        def default(self, o):
            if isinstance(o, Job):
                return o.id
            return super(FlaskJsonEncoder, self).default(o)

    logger.info('JSONEncoder registered')
    current_app.json_encoder = FlaskJsonEncoder


APP_STARTED.connect(register_json_encoder)


def dump_job(marshaller, value, write):
    write("<value><string>")
    write(value.id)
    write("</string></value>\n")


xmlrpc_client.Marshaller.dispatch[Job] = dump_job
