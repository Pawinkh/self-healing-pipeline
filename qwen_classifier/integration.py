"""Optional triage handoff. No lookup, graph writes, or remediation."""
import hashlib
import json
import os
import sys
import traceback
import uuid
from datetime import datetime, timezone
from pathlib import Path
from .classifier import QwenClassifier


def capture_exception(exc, *, job_name, batch_id=None, table=None, dead_letter_path=None):
    if os.getenv('QWEN_ENABLED', 'false').lower() != 'true':
        return None
    try:
        trace = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        failure = QwenClassifier().classify(trace, job_name=job_name, batch_id=batch_id, table=table)
        failure.update(error_text=trace, dead_letter_path=dead_letter_path,
                       created_at=datetime.now(timezone.utc).isoformat())
        directory = Path(os.getenv('QWEN_INCIDENT_DIR', 'incidents'))
        directory.mkdir(parents=True, exist_ok=True)
        identity = json.dumps([job_name, batch_id, dead_letter_path, trace])
        key = hashlib.sha256(identity.encode()).hexdigest()
        temporary = directory / (key+'.'+uuid.uuid4().hex+'.tmp')
        temporary.write_text(json.dumps(failure, indent=2))
        temporary.replace(directory/(key+'.json'))
        print(f"[qwen] {job_name} batch={batch_id}: {failure['failure_type']} ({failure['classification_status']})", file=sys.stderr)
        return failure
    except Exception as recording_error:
        print(f'[qwen] Triage unavailable: {type(recording_error).__name__}', file=sys.stderr)
        return None


def run_job(main, *, job_name):
    try:
        return main()
    except Exception as exc:
        capture_exception(exc, job_name=job_name)
        raise
