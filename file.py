import pathlib

BATCH_DIR = pathlib.Path("batch_downloads")

def list_batch_dirs():
    print("[DEBUG] BATCH_DIR =", BATCH_DIR.resolve())
    if not BATCH_DIR.exists():
        print("[DEBUG] BATCH_DIR missing")
        return
    dirs = [p for p in BATCH_DIR.iterdir() if p.is_dir()]
    print("[DEBUG] job_dirs =", len(dirs))
    for d in sorted(dirs)[-30:]:
        zsts = list(d.rglob("*.dbn.zst"))
        print("   ", d.name, "dbn.zst =", len(zsts))

list_batch_dirs()
raise RuntimeError("Set STATS_BATCH_JOB_ID manually from one of the folders above.")
