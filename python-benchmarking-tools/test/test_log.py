import io
import unittest
import unittest.mock

from haste.benchmarking import log


class TestLog(unittest.TestCase):

    @unittest.mock.patch('sys.stdout', new_callable=io.StringIO)
    def test_log(self, mock_stdout):
        log.log('topic', 'message')
        print(mock_stdout.getvalue())
        self.assertEqual('foo', mock_stdout.getvalue())
