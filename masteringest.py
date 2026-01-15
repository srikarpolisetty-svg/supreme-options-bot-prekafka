from databasefunctions import master_ingest
import duckdb
import argparse
from datetime import datetime

DB_PATH = "/home/ubuntu/supreme-options-bot-prekafka/options_data.db"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True)
    args = parser.parse_args()

    print(f"[MASTER] start {datetime.now()} run_id={args.run_id}", flush=True)

    with duckdb.connect(DB_PATH) as con:

        # ============================================================
        # CREATE TABLES (ALL)
        # ============================================================

        # 10 MIN TABLES
        con.execute("""
            CREATE TABLE IF NOT EXISTS option_snapshots_raw (
                con_id BIGINT,
                snapshot_id TEXT,
                timestamp TIMESTAMP,
                symbol TEXT,
                strike DOUBLE,
                call_put TEXT,
                days_to_expiry INTEGER,
                expiration_date DATE,
                moneyness_bucket TEXT,
                bid DOUBLE,
                ask DOUBLE,
                mid DOUBLE,
                volume INTEGER,
                open_interest INTEGER,
                iv DOUBLE,
                spread DOUBLE,
                spread_pct DOUBLE,
                time_decay_bucket TEXT
            );
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS option_snapshots_enriched (
                con_id BIGINT,
                snapshot_id TEXT,
                timestamp TIMESTAMP,
                symbol TEXT,
                strike DOUBLE,
                call_put TEXT,
                days_to_expiry INTEGER,
                expiration_date DATE,
                moneyness_bucket TEXT,
                bid DOUBLE,
                ask DOUBLE,
                mid DOUBLE,
                volume INTEGER,
                open_interest INTEGER,
                iv DOUBLE,
                spread DOUBLE,
                spread_pct DOUBLE,
                time_decay_bucket TEXT,
                mid_z DOUBLE,
                volume_z DOUBLE,
                iv_z DOUBLE,
                opt_ret_10m DOUBLE,
                opt_ret_1h DOUBLE,
                opt_ret_eod DOUBLE,
                opt_ret_next_open DOUBLE,
                opt_ret_1d DOUBLE,
                opt_ret_exp DOUBLE
            );
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS option_snapshots_execution_signals (
                con_id BIGINT,
                snapshot_id TEXT,
                timestamp TIMESTAMP,
                symbol TEXT,
                strike DOUBLE,
                call_put TEXT,
                days_to_expiry INTEGER,
                expiration_date DATE,
                moneyness_bucket TEXT,
                bid DOUBLE,
                ask DOUBLE,
                mid DOUBLE,
                volume INTEGER,
                open_interest INTEGER,
                iv DOUBLE,
                spread DOUBLE,
                spread_pct DOUBLE,
                time_decay_bucket TEXT,
                mid_z DOUBLE,
                volume_z DOUBLE,
                iv_z DOUBLE,
                opt_ret_10m DOUBLE,
                opt_ret_1h DOUBLE,
                opt_ret_eod DOUBLE,
                opt_ret_next_open DOUBLE,
                opt_ret_1d DOUBLE,
                opt_ret_exp DOUBLE,
                atm_call_signal BOOLEAN,
                atm_put_signal BOOLEAN,
                otm1_call_signal BOOLEAN,
                otm1_put_signal BOOLEAN,
                otm2_call_signal BOOLEAN,
                otm2_put_signal BOOLEAN
            );
        """)

        # 5 WEEK TABLES
        con.execute("""
            CREATE TABLE IF NOT EXISTS option_snapshots_raw_5w (
                con_id BIGINT,
                snapshot_id TEXT,
                timestamp TIMESTAMP,
                symbol TEXT,
                strike DOUBLE,
                call_put TEXT,
                days_to_expiry INTEGER,
                expiration_date DATE,
                moneyness_bucket TEXT,
                bid DOUBLE,
                ask DOUBLE,
                mid DOUBLE,
                volume INTEGER,
                open_interest INTEGER,
                iv DOUBLE,
                spread DOUBLE,
                spread_pct DOUBLE,
                time_decay_bucket TEXT
            );
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS option_snapshots_enriched_5w (
                con_id BIGINT,
                snapshot_id TEXT,
                timestamp TIMESTAMP,
                symbol TEXT,
                strike DOUBLE,
                call_put TEXT,
                days_to_expiry INTEGER,
                expiration_date DATE,
                moneyness_bucket TEXT,
                bid DOUBLE,
                ask DOUBLE,
                mid DOUBLE,
                volume INTEGER,
                open_interest INTEGER,
                iv DOUBLE,
                spread DOUBLE,
                spread_pct DOUBLE,
                time_decay_bucket TEXT,
                mid_z DOUBLE,
                volume_z DOUBLE,
                iv_z DOUBLE,
                opt_ret_10m DOUBLE,
                opt_ret_1h DOUBLE,
                opt_ret_eod DOUBLE,
                opt_ret_next_open DOUBLE,
                opt_ret_1d DOUBLE,
                opt_ret_exp DOUBLE
            );
        """)

        con.execute("""
            CREATE TABLE IF NOT EXISTS option_snapshots_execution_signals_5w (
                con_id BIGINT,
                snapshot_id TEXT,
                timestamp TIMESTAMP,
                symbol TEXT,
                strike DOUBLE,
                call_put TEXT,
                days_to_expiry INTEGER,
                expiration_date DATE,
                moneyness_bucket TEXT,
                bid DOUBLE,
                ask DOUBLE,
                mid DOUBLE,
                volume INTEGER,
                open_interest INTEGER,
                iv DOUBLE,
                spread DOUBLE,
                spread_pct DOUBLE,
                time_decay_bucket TEXT,
                mid_z DOUBLE,
                volume_z DOUBLE,
                iv_z DOUBLE,
                opt_ret_10m DOUBLE,
                opt_ret_1h DOUBLE,
                opt_ret_eod DOUBLE,
                opt_ret_next_open DOUBLE,
                opt_ret_1d DOUBLE,
                opt_ret_exp DOUBLE,
                atm_call_signal BOOLEAN,
                atm_put_signal BOOLEAN,
                otm1_call_signal BOOLEAN,
                otm1_put_signal BOOLEAN,
                otm2_call_signal BOOLEAN,
                otm2_put_signal BOOLEAN
            );
        """)

        # ============================================================
        # INGEST
        # ============================================================

        master_ingest(run_id=args.run_id)

        # ============================================================
        # CLEANUP (ALL)
        # ============================================================

        # 10 MIN CLEANUP
        con.execute("""
            DELETE FROM option_snapshots_raw
            WHERE timestamp < NOW() - INTERVAL '3 days';
        """)

        con.execute("""
            DELETE FROM option_snapshots_enriched
            WHERE timestamp < NOW() - INTERVAL '3 days';
        """)

        con.execute("""
            DELETE FROM option_snapshots_execution_signals
            WHERE timestamp < NOW() - INTERVAL '3 days';
        """)

        # 5 WEEK CLEANUP
        con.execute("""
            DELETE FROM option_snapshots_raw_5w
            WHERE timestamp < NOW() - INTERVAL '35 days';
        """)

        con.execute("""
            DELETE FROM option_snapshots_enriched_5w
            WHERE timestamp < NOW() - INTERVAL '35 days';
        """)

        con.execute("""
            DELETE FROM option_snapshots_execution_signals_5w
            WHERE timestamp < NOW() - INTERVAL '35 days';
        """)
