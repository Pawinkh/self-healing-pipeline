import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import URLError
from qwen_classifier import QwenClassifier
from qwen_classifier.integration import capture_exception, run_job

ERROR='[UNRESOLVED_COLUMN.WITH_SUGGESTION] Column `crash_month` cannot be resolved'
GOOD=dict(failure_type='MISSING_COLUMN', table=None, column='crash_month', error_class='UNRESOLVED_COLUMN.WITH_SUGGESTION')
def reply(pred):
    return io.BytesIO(json.dumps(dict(done=True,message=dict(content=json.dumps(pred)))).encode())

class Tests(unittest.TestCase):
    @patch('qwen_classifier.classifier.urlopen')
    def test_valid(self, call):
        call.return_value=reply(GOOD)
        result=QwenClassifier().classify(ERROR,batch_id=1)
        self.assertEqual(result['column'],'crash_month')
        self.assertFalse(result['needs_review'])
        self.assertEqual(result['classification_status'],'OK')
    @patch('qwen_classifier.classifier.urlopen')
    def test_invalid(self, call):
        for prediction in (dict(GOOD,table='invented'),dict(GOOD,action='DROP'),
                           dict(GOOD,error_class='UNRESOLVED_COLUMN'),dict(GOOD,column=[])):
            call.return_value=reply(prediction)
            self.assertEqual(QwenClassifier().classify(ERROR)['classification_status'],'INVALID_OUTPUT')
    @patch('qwen_classifier.classifier.urlopen',side_effect=URLError('offline'))
    def test_offline(self,call):
        result=QwenClassifier().classify(ERROR)
        self.assertEqual(result['classification_status'],'UNAVAILABLE')
        self.assertTrue(result['needs_review'])
    @patch('qwen_classifier.classifier.urlopen')
    def test_bad_json(self,call):
        call.return_value=io.BytesIO(b'not json')
        self.assertEqual(QwenClassifier().classify(ERROR)['classification_status'],'INVALID_OUTPUT')
    @patch('qwen_classifier.classifier.urlopen')
    def test_unknown_and_truncated(self,call):
        call.return_value=reply(dict(GOOD,failure_type='UNKNOWN'))
        self.assertTrue(QwenClassifier().classify(ERROR)['needs_review'])
        call.return_value=reply(GOOD)
        self.assertTrue(QwenClassifier().classify(ERROR+'x'*20000)['needs_review'])
    @patch('qwen_classifier.integration.QwenClassifier')
    def test_disabled(self,client):
        with patch.dict(os.environ,QWEN_ENABLED='false'):
            self.assertIsNone(capture_exception(ValueError('x'),job_name='silver2'))
        client.assert_not_called()
    @patch('qwen_classifier.integration.QwenClassifier')
    def test_preserves_error_and_writes_incident(self,client):
        client.return_value.classify.return_value=dict(GOOD,classification_status='OK')
        original=RuntimeError('original')
        def fail(): raise original
        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(os.environ,QWEN_ENABLED='true',QWEN_INCIDENT_DIR=directory):
                with self.assertRaises(RuntimeError) as caught:
                    run_job(fail,job_name='silver2')
            self.assertIs(caught.exception,original)
            self.assertEqual(len(list(Path(directory).glob('*.json'))),1)
    def test_blank(self):
        with self.assertRaises(ValueError): QwenClassifier().classify('')
