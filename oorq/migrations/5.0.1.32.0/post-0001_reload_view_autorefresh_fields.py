# coding=utf-8
import logging
from tools import config
from oopgrade.oopgrade import MigrationHelper


logger = logging.getLogger('openerp.migration.' + __name__)


def up(cursor, installed_version):
    if not installed_version or config.updating_all:
        return
    mg = MigrationHelper(cursor, 'oorq')
    mg.update_xml_records(
        'oorq_view.xml',
        update_record_ids=[
            'view_oorq_worker_tree',
            'view_oorq_queue_tree',
            'view_oorq_registry_tree',
            'view_oorq_job_form',
            'view_oorq_job_tree',
            'view_oorq_jobs_group_form',
            'view_oorq_jobs_group_tree',
        ]
    )


def down(cursor, installed_version):
    pass


migrate = up
