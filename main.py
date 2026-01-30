import argparse


def run_generator():
    from src.generator.data_generator import run_generator
    run_generator()


def run_flink_job():
    from src.flink_jobs.fingerprint_job import run_fingerprint_job
    run_fingerprint_job()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "mode",
        choices=["generator", "flink"],
        help="Run generator or flink job",
    )
    args = parser.parse_args()

    if args.mode == "generator":
        run_generator()
    elif args.mode == "flink":
        run_flink_job()


if __name__ == "__main__":
    main()
