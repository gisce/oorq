from __future__ import absolute_import

from destral import testing


class TestTrackerConfig(testing.OOTestCaseWithCursor):
    def test_global_query_engine_is_forwarded_to_worker_tracker(self):
        from oorq.tasks import _webservice_tracker_kwargs

        kwargs = _webservice_tracker_kwargs(
            {'global_query_engine': 'auto'}, uid=1, obj='res.users'
        )

        self.assertEqual(kwargs['ooquery_strategy'], 'auto')
        self.assertEqual(kwargs['uid'], 1)
        self.assertEqual(kwargs['obj'], 'res.users')

    def test_disabled_global_query_engine_is_not_forwarded(self):
        from oorq.tasks import _webservice_tracker_kwargs

        kwargs = _webservice_tracker_kwargs(
            {'global_query_engine': False}, uid=1
        )

        self.assertNotIn('ooquery_strategy', kwargs)
