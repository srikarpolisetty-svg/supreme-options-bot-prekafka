from fiveweekdatabase import master_ingest_5w
import duckdb
import argparse
from datetime import datetime
DB_PATH = "/home/ubuntu/supreme-options-bot/options_data.db"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--run_id", required=True)
    args = parser.parse_args()
    print(f"[MASTER] start {datetime.now()} run_id={args.run_id}", flush=True)
    with duckdb.connect(DB_PATH) as con:
        # 0) Ensure tables exist BEFORE ingest
        con.execute("""
            CREATE TABLE IF NOT EXISTS option_snapshots_raw_5w (
                snapshot_id TEXT,
                timestamp TIMESTAMP,
                symbol TEXT,
                option_symbol TEXT,
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
                snapshot_id TEXT,
                timestamp TIMESTAMP,
                symbol TEXT,
                option_symbol TEXT,
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
                snapshot_id TEXT,
                timestamp TIMESTAMP,
                symbol TEXT,
                option_symbol TEXT,
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

        # 1) Ingest (single writer phase)
        master_ingest_5w(run_id=args.run_id)

        # 2) Cleanup AFTER ingest
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
