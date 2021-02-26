# coding=utf-8
from six.moves import xmlrpc_client
from rq.job import Job


def dump_job(marshaller, value, write):
    write("<value><string>")
    write(value.id)
    write("</string></value>\n")


xmlrpc_client.Marshaller.dispatch[Job] = dump_job
