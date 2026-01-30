"""Main entry point for the flink-fingerprint project.

Usage:
    python main.py generator  - Start the data generator (publishes to Kafka)
    python main.py flink      - Run the Flink fingerprint processing job
    python main.py pandas     - Run the Pandas-based fingerprint job (simpler)
    python main.py both       - Run both generator and pandas processor together
"""

import argparse
import sys
import time
from concurrent.futures import ThreadPoolExecutor


def run_generator():
    """Start the data generator that publishes to Kafka."""
    from src.generator.data_generator import run_generator as start_generator

    print("=" * 60)
    print("SENSOR DATA GENERATOR")
    print("=" * 60)
    start_generator()


def run_flink_job():
    """Run the Flink fingerprint processing job."""
    from src.flink_jobs.fingerprint_job import run_fingerprint_job

    print("=" * 60)
    print("FLINK FINGERPRINT JOB")
    print("=" * 60)
    run_fingerprint_job()


def run_pandas_job():
    """Run the Pandas-based fingerprint processing job."""
    from src.flink_jobs.fingerprint_job import run_fingerprint_job_with_pandas

    print("=" * 60)
    print("PANDAS FINGERPRINT JOB")
    print("=" * 60)
    run_fingerprint_job_with_pandas()


def run_both(delay: int = 5):
    """Run both generator and processor together.

    Args:
        delay: Seconds to wait after starting generator before starting processor
    """
    print("=" * 60)
    print("STARTING COMBINED MODE")
    print("=" * 60)
    print(f"Starting generator first, then processor after {delay}s delay...")

    with ThreadPoolExecutor(max_workers=2) as executor:
        # Start generator
        gen_future = executor.submit(run_generator)

        # Wait for generator to connect to Kafka
        time.sleep(delay)

        # Start processor
        proc_future = executor.submit(run_pandas_job)

        try:
            while True:
                if gen_future.done() or proc_future.done():
                    break
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down...")


def main():
    """Main entry point with CLI argument parsing."""
    parser = argparse.ArgumentParser(
        description="PyFlink Streaming Fingerprint System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py generator   # Start data generator
  python main.py pandas      # Run Pandas fingerprint processor
  python main.py flink       # Run Flink fingerprint job (in Docker)
  python main.py both        # Run generator + Pandas processor

Before running:
  1. Start Docker services:
     docker-compose -f docker/docker-compose.yml up -d

  2. Wait for Kafka to be ready (~10 seconds)

  3. Run the desired command
        """,
    )

    parser.add_argument(
        "command",
        choices=["generator", "flink", "pandas", "both"],
        help="Component to run",
    )

    parser.add_argument(
        "--delay",
        type=int,
        default=5,
        help="Delay in seconds before starting processor in 'both' mode (default: 5)",
    )

    args = parser.parse_args()

    if args.command == "generator":
        run_generator()
    elif args.command == "flink":
        run_flink_job()
    elif args.command == "pandas":
        run_pandas_job()
    elif args.command == "both":
        run_both(args.delay)


if __name__ == "__main__":
    main()
