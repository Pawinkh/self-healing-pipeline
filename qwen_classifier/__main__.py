import argparse
import json
from pathlib import Path
from .classifier import QwenClassifier


def main():
    parser = argparse.ArgumentParser(description='Classify a Spark exception using local Qwen')
    parser.add_argument('--error-file', required=True, type=Path)
    parser.add_argument('--job-name')
    parser.add_argument('--batch-id', type=int)
    parser.add_argument('--table')
    args = parser.parse_args()
    result = QwenClassifier().classify(args.error_file.read_text(), job_name=args.job_name,
                                      batch_id=args.batch_id, table=args.table)
    print(json.dumps(result, indent=2))
    return 0 if result['classification_status']=='OK' else 2


if __name__ == '__main__':
    raise SystemExit(main())
