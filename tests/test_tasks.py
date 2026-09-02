from unittest import TestCase

from oorq.tasks import execution_user_context


class FakeContextStack(object):

    def __init__(self, context=None):
        self._stack = []
        if context is not None:
            self.push(context)

    @property
    def top(self):
        return self._stack[-1] if self._stack else None

    def push(self, context):
        self._stack.append(context)

    def pop(self):
        return self._stack.pop()


class TestExecutionUserContext(TestCase):

    def test_sets_user_and_preserves_existing_context(self):
        original_context = {'request_id': 'request-1'}
        stack = FakeContextStack(original_context)
        user = object()

        with execution_user_context(stack, user):
            self.assertIs(stack.top['user'], user)
            self.assertEqual(stack.top['request_id'], 'request-1')
            self.assertIsNot(stack.top, original_context)

        self.assertIs(stack.top, original_context)

    def test_removes_context_after_exception(self):
        stack = FakeContextStack()

        with self.assertRaises(RuntimeError):
            with execution_user_context(stack, object()):
                raise RuntimeError('job failed')

        self.assertIsNone(stack.top)

    def test_consecutive_jobs_do_not_share_users(self):
        stack = FakeContextStack()
        first_user = object()
        second_user = object()

        with execution_user_context(stack, first_user):
            self.assertIs(stack.top['user'], first_user)
        with execution_user_context(stack, second_user):
            self.assertIs(stack.top['user'], second_user)

        self.assertIsNone(stack.top)
