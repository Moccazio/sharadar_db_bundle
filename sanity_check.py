#!/usr/bin/env python3
import argparse
import logging
import os
import sys
import traceback

import pandas as pd
from exchange_calendars import get_calendar
from sharadar.pipeline.engine import load_sharadar_bundle, make_pipeline_engine, symbols
from sharadar.pipeline.factors import MarketCap
from sharadar.util.run_algo import run_algorithm
from zipline.pipeline import Pipeline
from zipline.pipeline.data import USEquityPricing
from zipline.pipeline.filters import StaticAssets


def setup_logger(log_file: str) -> logging.Logger:
    logger = logging.getLogger("sharadar_sanity")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger


def _normalize_session(date: pd.Timestamp, cal, side: str) -> pd.Timestamp:
    date = pd.Timestamp(date).tz_localize(None).normalize()
    if cal.is_session(date):
        return date
    if side == "end":
        return cal.previous_close(date).tz_localize(None).normalize()
    return cal.next_open(date).tz_localize(None).normalize()


def check_pipeline(logger, pipe_engine, assets, start_date, end_date):
    pipe = Pipeline(
        columns={
            "close": USEquityPricing.close.latest,
            "volume": USEquityPricing.volume.latest,
            "mkt_cap": MarketCap(),
        },
        screen=StaticAssets(assets),
    )

    logger.info("Pipeline check 1/2: last date %s", end_date.date())
    result_last = pipe_engine.run_pipeline(pipe, end_date)
    if result_last.empty:
        raise RuntimeError("Pipeline returned empty result on last available date.")
    logger.info("Pipeline last-date rows: %d", len(result_last))

    logger.info("Pipeline check 2/2: range %s -> %s", start_date.date(), end_date.date())
    result_range = pipe_engine.run_pipeline(pipe, start_date, end_date)
    if result_range.empty:
        raise RuntimeError("Pipeline returned empty result on 1-year range.")

    min_dt = result_range.index.get_level_values(0).min()
    max_dt = result_range.index.get_level_values(0).max()
    logger.info("Pipeline range rows: %d, date span in output: %s -> %s", len(result_range), min_dt, max_dt)


def check_algorithm(logger, start_date, end_date):
    logger.info("Algorithm check: running from %s to %s", start_date.date(), end_date.date())

    def initialize(context):
        return None

    def handle_data(context, data):
        return None

    perf = run_algorithm(
        initialize=initialize,
        handle_data=handle_data,
        start=start_date,
        end=end_date,
        data_frequency="daily",
        bundle="sharadar",
        benchmark_symbol="SPY",
        metrics_set="default_daily",
    )

    if perf is None or len(perf) == 0:
        raise RuntimeError("Algorithm returned no performance rows.")

    logger.info("Algorithm performance rows: %d", len(perf))


def main() -> int:
    parser = argparse.ArgumentParser(description="Sanity checks for Sharadar bundle pipeline and algorithm.")
    parser.add_argument("--bundle", default="sharadar", help="Bundle name to load (default: sharadar)")
    parser.add_argument("--symbols", nargs="+", default=["AAPL", "SPY"], help="Symbols used in pipeline checks")
    parser.add_argument("--years", type=int, default=1, help="Range length in years ending at last available date")
    parser.add_argument(
        "--log-file",
        default=os.path.join(os.getcwd(), "sanity_check.log"),
        help="Path to log file",
    )
    args = parser.parse_args()

    logger = setup_logger(args.log_file)
    logger.info("Starting sanity check")
    logger.info("Log file: %s", args.log_file)

    try:
        bundle = load_sharadar_bundle(args.bundle)
        cal = get_calendar("XNYS", start=pd.Timestamp("2000-01-01 00:00:00"))

        raw_last = pd.Timestamp(bundle.equity_daily_bar_reader.last_available_dt)
        end_date = _normalize_session(raw_last, cal, side="end")
        start_date = _normalize_session(end_date - pd.DateOffset(years=args.years), cal, side="start")

        if start_date >= end_date:
            raise RuntimeError("Computed start_date is not before end_date.")

        logger.info("Bundle loaded: %s", args.bundle)
        logger.info("Date range: %s -> %s", start_date.date(), end_date.date())

        assets = symbols(args.symbols)
        pipe_engine = make_pipeline_engine(bundle)

        check_pipeline(logger, pipe_engine, assets, start_date, end_date)
        check_algorithm(logger, start_date, end_date)

        logger.info("SANITY CHECK PASSED")
        return 0
    except Exception:
        logger.error("SANITY CHECK FAILED")
        logger.error(traceback.format_exc())
        return 1


if __name__ == "__main__":
    raise SystemExit(main())