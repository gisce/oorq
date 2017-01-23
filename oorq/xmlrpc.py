# coding=utf-8
import xmlrpclib
from rq.job import Job


def dump_job(marshaller, value, write):
    write("<value><string>")
    write(value.id)
    write("</string></value>\n")


xmlrpclib.Marshaller.dispatch[Job] = dump_job
