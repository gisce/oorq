# coding=utf-8
from rq.job import Job
from six.moves import xmlrpc_client

try:
    from flask import current_app
    from flask.json import JSONEncoder as JSONEncoderBase

    class JsonEncoder(JSONEncoderBase):
        def default(self, o):
            if isinstance(o, Job):
                return o.id
            return super(JsonEncoder, self).default(o)

    current_app.json_encoder = JsonEncoder
except RuntimeError:
    pass


def dump_job(marshaller, value, write):
    write("<value><string>")
    write(value.id)
    write("</string></value>\n")


xmlrpc_client.Marshaller.dispatch[Job] = dump_job
