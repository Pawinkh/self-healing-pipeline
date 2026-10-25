# Qwen exception classifier (IC2)

Python standard library only; Qwen runs locally using Ollama.
This module classifies unstructured exceptions. It does not implement lookup,
Neo4j writes, remediation, or classify already-structured quarantine failures.

## Quick demo (from project root)

```sh
ollama pull qwen3:4b-instruct
python3 -m qwen_classifier --error-file qwen_classifier/example_error.txt --job-name Silver2 --batch-id 17
python3 -m unittest discover -s tests -p 'test_qwen.py' -v
```

## Python interface

```python
from qwen_classifier import QwenClassifier
failure = QwenClassifier().classify(error_text, job_name='Silver2', batch_id=17)
```

Output: schema_version, source, failure_type, table, column, error_class,
job_name, batch_id, needs_review, classification_status, model, input_truncated.
Types: MISSING_COLUMN, TYPE_MISMATCH, PARSE_ERROR, CORRUPT_RECORD,
SCHEMA_MISMATCH, UNKNOWN. Missing identifiers are null.
OK means output passed structural/evidence checks, not proven semantic correctness.
UNAVAILABLE and INVALID_OUTPUT require review. No output authorizes a repair.
Long input is bounded to the first/last 8,000 characters and requires review.
An identifier appearing in input is necessary but not sufficient evidence that it
is the correct affected field; measure extraction accuracy separately.

## Pipeline integration

Silver2 calls Qwen after its existing dead-letter write succeeds. The raw batch
remains in the existing Delta dead-letter table. The classification and traceback
are stored in incidents/ with the dead-letter path and batch ID for handoff.
The combined run_layers entry point also classifies uncaught job exceptions and
re-raises the original error. Direct layer scripts have no general entry wrapper.
Classification failure does not change the existing batch/retry behavior.
Incident files are a handoff artifact; no recovery consumer or lookup is added.

Optional integration is disabled by default. To enable:

```sh
QWEN_ENABLED=true docker compose up --build
```

Compose mounts the package into both Spark drivers. Host Ollama must be reachable
at http://host.docker.internal:11434 (Docker Desktop); configure QWEN_BASE_URL for
your deployment. Do not expose Ollama publicly. Host CLI defaults to localhost.
Other settings: QWEN_MODEL, QWEN_INCIDENT_DIR. See .env.example for Docker values.

The Spark Dockerfile is unchanged: the package is provided by Compose's bind mount.
Smoke tests of local inference are not a full running Spark/Docker integration test.
