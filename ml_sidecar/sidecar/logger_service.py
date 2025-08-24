import os
import logging
from pathlib import Path
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import zmq

# This file lives in ml_sidecar/sidecar/
ML_ROOT = Path(__file__).resolve().parents[1]

# Addresses & IO (env-overridable)
BUS_ADDR = os.getenv("BUS_BIND", os.getenv("LOG_BIND", "tcp://127.0.0.1:5557"))
DATA_DIR = Path(os.getenv("DATA_DIR", str(ML_ROOT / "data" / "ml")))
LOG_DIR  = Path(os.getenv("LOG_DIR",  str(ML_ROOT / "logs")))
LOG_FILE = Path(os.getenv("SIDECAR_LOG", str(LOG_DIR / "sidecar.log")))
ROW_GROUP = int(os.getenv("PARQUET_ROW_GROUP", "5000"))

# Ensure directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

# Logging config
logging.basicConfig(
    filename=str(LOG_FILE),
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logging.info("Sidecar logger starting. SUB connect to %s", BUS_ADDR)

# Buffers per event type (includes preflight_ping for preflight verification)
EVENT_TYPES = ["universe_snapshot", "feature_snapshot", "decision", "execution", "outcome", "preflight_ping"]
buffers: dict[str, list[dict]] = {k: [] for k in EVENT_TYPES}
last_session: dict[str, str] = {}

def _parts(session: str):
    y, m, d = session.split("-")
    return y, m, d

def _flush(event_type: str, session: str):
    """Flush one event buffer to a partitioned parquet file."""
    buf = buffers.get(event_type, [])
    if not buf:
        return
    try:
        y, m, d = _parts(session)
    except Exception:
        y, m, d = "unknown", "unknown", "unknown"

    out_dir = DATA_DIR / event_type / y / m / d
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{event_type}_{session}.parquet"

    try:
        table = pa.Table.from_pandas(pd.DataFrame(buf), preserve_index=False)
        if out_file.exists():
            try:
                old = pq.read_table(out_file)
                table = pa.concat_tables([old, table])
            except Exception as e:
                logging.warning("Concat existing failed for %s: %s", out_file, e)
        pq.write_table(table, out_file, compression="snappy", row_group_size=ROW_GROUP)
        logging.info("Flushed %s rows for %s -> %s", len(buf), event_type, out_file)
    except Exception as e:
        logging.error("Parquet write failed for %s (%s): %s", event_type, session, e)

    buffers[event_type].clear()

def _flush_all(final_session: str | None = None):
    for et, buf in buffers.items():
        if buf:
            s = final_session or buf[-1].get("session") or datetime.now().date().isoformat()
            _flush(et, s)

def main():
    # ZeroMQ SUB socket (connect to the PUB address)
    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.SUB)
    sock.connect(BUS_ADDR)
    sock.setsockopt_string(zmq.SUBSCRIBE, "")
    logging.info("Listening (SUB) on %s", BUS_ADDR)

    while True:
        try:
            msg = sock.recv_json()
        except KeyboardInterrupt:
            break
        except Exception as e:
            logging.warning("recv_json error: %s", e)
            continue

        et = msg.get("event")
        if et not in buffers:
            logging.warning("Unknown event type: %s", et)
            continue

        buffers[et].append(msg)

        # Determine session (default to today's date)
        session = msg.get("session") or datetime.now().date().isoformat()
        prev = last_session.get(et)

        # Rollover flush if the session changed
        if prev and prev != session:
            _flush(et, prev)
        last_session[et] = session

                # Immediate flush for preflight to let preflight verify quickly
        if et == "preflight_ping":
            _flush(et, session)
            # Force filesystem sync so preflight.py can see it right away
            try:
                today = datetime.now().date()
                pq_file = DATA_DIR / et / f"{today.year:04d}" / f"{today.month:02d}" / f"{today.day:02d}" / f"{et}_{today.isoformat()}.parquet"
                if pq_file.exists():
                    with open(pq_file, "rb+") as f:
                        os.fsync(f.fileno())
            except Exception as e:
                logging.warning("fsync failed for preflight_ping: %s", e)
            continue

        # Periodic flush for larger buffers
        if len(buffers[et]) >= 1000:
            _flush(et, session)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass
    finally:
        try:
            _flush_all()
        except Exception:
            logging.exception("Error during final flush")
        logging.info("Sidecar logger stopped.")
