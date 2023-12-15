# -*- coding: utf-8 -*-
{
    "name": "oorq",
    "version": "1.28.1",
    "depends": ["base"],
    "author": "Eduard Carreras",
    "category": "Base",
    "description": """
    This module provide :
      * Use python-rq (Redis Queue) to manage jobs
    """,
    "init_xml": [],
    'update_xml': ['oorq_view.xml', 'security/oorq.xml', 'security/ir.model.access.csv'],
    'demo_xml': [],
    'installable': True,
    'active': False,
}
