#!/usr/bin/env python3
"""
MassIVE/GNPS Dataset Downloader
Downloads mzML/mzXML files from one or more MassIVE datasets over FTP,
falling back to RAW files when no mzML/mzXML files are found.

FTP layout on massive-ftp.ucsd.edu:
    /v01/ … /v12/              <- global server version dirs
        MSV000095785/
            ccms_peak/
                *.mzML
            raw/
                *.RAW

Files whose full FTP path contains any exclude keyword (neg, qc, blank,
blanc, control) are silently skipped.

Usage:
    # Single dataset
    python massive_downloader.py MSV000095785
    python massive_downloader.py MSV000095785 --output ./data --dry-run

    # Batch (one accession per line; blank lines and # comments ignored)
    python massive_downloader.py --file ids.txt --output ./data

Resume:
    Completed datasets are appended to <o>/completed.log.
    Re-running the same command skips datasets already in that file.
    Use --no-resume to force re-downloading everything.

Output:
    Each dataset is streamed directly into <o>/<accession>.tar.zst.
    Archive members are stored below a top-level <accession>/ directory.
"""

import argparse
import compression.zstd as zstd
import datetime
import ftplib
import tarfile
import sys
import re
import time
from pathlib import Path
from typing import Optional

FTP_HOST        = "massive-ftp.ucsd.edu"
TARGET_EXTS     = {".mzml", ".mzxml"}
RAW_TARGET_EXTS = {".raw"}
VERSION_RE      = re.compile(r"^v\d+$", re.IGNORECASE)
COMPLETED_LOG   = "completed.log"
ARCHIVE_SUFFIX  = ".tar.zst"
ZSTD_LEVEL      = 19

# Files/folders whose path (case-insensitive) contains one of these words
# as a whole word (not embedded inside another word) are excluded.
EXCLUDE_WORDS  = ["neg", "qc", "blank", "blanc", "control"]
_EXCLUDE_RE    = re.compile(
    "(" + "|".join(re.escape(w) for w in EXCLUDE_WORDS) + ")",
    re.IGNORECASE,
)


def is_excluded(remote_path: str) -> bool:
    return bool(_EXCLUDE_RE.search(remote_path))


# ---------------------------------------------------------------------------
# Progress bar (single updating line per file)
# ---------------------------------------------------------------------------

def _fmt_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if abs(n) < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} TB"


def _fmt_eta(secs: float) -> str:
    if secs < 0 or secs > 86400:
        return "--:--"
    m, s = divmod(int(secs), 60)
    h, m = divmod(m, 60)
    return f"{h:02d}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


def _bar(frac: float, width: int = 24) -> str:
    filled = int(max(0.0, min(1.0, frac)) * width)
    return "█" * filled + "░" * (width - filled)


class Progress:
    """
    Single updating line per file.
    Uses \r to overwrite in place on stderr (always visible, never mixed with
    stdout pipe output). Updates are throttled to at most once per 0.2 s.
    """
    _INTERVAL = 0.2

    def __init__(self, label: str, total: int) -> None:
        self.label      = (label[:50] + "…") if len(label) > 51 else label
        self.total      = total
        self._done      = 0
        self._t0        = time.monotonic()
        self._last_draw = 0.0

    def _render(self) -> str:
        elapsed = time.monotonic() - self._t0 or 1e-6
        speed   = self._done / elapsed
        if self.total <= 0:
            return (
                f"  ↓ {self.label}  "
                f"{_fmt_bytes(self._done)}  "
                f"{_fmt_bytes(int(speed))}/s"
            )
        frac    = self._done / self.total if self.total > 0 else 0.0
        eta     = (self.total - self._done) / speed if speed > 0 and self.total > 0 else -1
        pct     = f"{frac*100:5.1f}%"
        return (
            f"  ↓ {self.label}  "
            f"{_bar(frac)}  {pct}  "
            f"{_fmt_bytes(self._done)}/{_fmt_bytes(self.total)}  "
            f"{_fmt_bytes(int(speed))}/s  ETA {_fmt_eta(eta)}"
        )

    def update(self, chunk: int) -> None:
        self._done += chunk
        now = time.monotonic()
        if now - self._last_draw >= self._INTERVAL:
            sys.stdout.write(f"\r{self._render()}")
            sys.stdout.flush()
            self._last_draw = now

    def finish(self, ok: bool) -> None:
        elapsed = time.monotonic() - self._t0 or 1e-6
        avg     = _fmt_bytes(int(self._done / elapsed))
        icon    = "✓" if ok else "✗"
        line    = f"  {icon} {self.label}  {_fmt_bytes(self._done)}  avg {avg}/s"
        sys.stdout.write(f"\r{line}\n")
        sys.stdout.flush()


# ---------------------------------------------------------------------------
# FTP helpers
# ---------------------------------------------------------------------------


def connect_anon(timeout: int = 60, retries: int = 4) -> ftplib.FTP:
    for attempt in range(1, retries + 1):
        print(f"[FTP] Connecting to ftps://{FTP_HOST}/ (attempt {attempt}/{retries}, timeout={timeout}s) …")
        try:
            ftp = ftplib.FTP_TLS(FTP_HOST, timeout=timeout)
            ftp.login()
            ftp.prot_p()   # <-- secure the data channel too
            print(f"[FTP] Connected OK  →  ftps://{FTP_HOST}/")
            return ftp
        except Exception as exc:
            label = type(exc).__name__
            print(f"[FTP] Connection FAILED: {label}: {exc}")
            if attempt == retries:
                print(f"[FTP] Giving up after {retries} attempts.")
                raise
            wait = 10 * attempt
            print(f"[FTP] Retrying in {wait}s …")
            time.sleep(wait)
    raise RuntimeError("connect_anon: unreachable")


def list_dir(ftp: ftplib.FTP, path: str) -> list[tuple[str, dict]]:
    """MLSD preferred; NLST + CWD-probe fallback."""
    try:
        return list(ftp.mlsd(path))
    except (ftplib.error_perm, ftplib.error_reply):
        pass
    try:
        names = ftp.nlst(path)
    except ftplib.error_perm:
        return []
    saved  = ftp.pwd()
    result = []
    for full in names:
        base = full.split("/")[-1]
        try:
            ftp.cwd(full); ftp.cwd(saved)
            result.append((base, {"type": "dir"}))
        except ftplib.error_perm:
            result.append((base, {"type": "file"}))
    return result


def dir_exists(ftp: ftplib.FTP, path: str) -> bool:
    try:
        ftp.cwd(path); ftp.cwd("/")
        return True
    except ftplib.error_perm:
        return False


# ---------------------------------------------------------------------------
# Locate dataset
# ---------------------------------------------------------------------------

_global_versions: list[str] = []


def get_global_versions(ftp: ftplib.FTP) -> list[str]:
    global _global_versions
    if _global_versions:
        return _global_versions
    print(f"[INFO] Scanning FTP root: ftp://{FTP_HOST}/")
    versions = sorted(
        name for name, facts in list_dir(ftp, "/")
        if facts.get("type", "").lower() in ("dir", "cdir")
        and VERSION_RE.match(name)
    )
    if not versions:
        sys.exit("[ERROR] No vNN directories at FTP root — server layout may have changed.")
    print(f"[INFO] Version dirs: {versions}")
    _global_versions = versions
    return versions


def find_dataset_path(ftp: ftplib.FTP, accession: str) -> Optional[str]:
    """Return FTP path of the newest copy of the dataset, or None."""
    versions = get_global_versions(ftp)
    found_in = []
    print(f"[INFO] Locating {accession} …")
    for gv in versions:
        candidate = f"/{gv}/{accession}"
        print(f"[INFO]   Checking ftp://{FTP_HOST}{candidate}/")
        if dir_exists(ftp, candidate):
            found_in.append(gv)
    if not found_in:
        print(f"[WARN] {accession} not found — skipping.")
        return None
    latest = f"/{found_in[-1]}/{accession}"
    if len(found_in) > 1:
        print(f"[INFO]   Copies in: {found_in}  →  using {found_in[-1]}")
    print(f"[INFO]   Path: ftp://{FTP_HOST}{latest}/")
    return latest


# ---------------------------------------------------------------------------
# FTP walk + keyword filter
# ---------------------------------------------------------------------------

def walk_ftp(ftp: ftplib.FTP, remote_root: str,
             target_exts: set[str] = TARGET_EXTS) -> tuple[list[str], int]:
    """
    Recursively collect paths matching target_exts under remote_root.
    Returns (kept_paths, n_excluded).
    """
    kept:     list[str] = []
    excluded: int       = 0
    queue = [remote_root]

    while queue:
        current = queue.pop()
        print(f"[INFO]   Listing ftp://{FTP_HOST}{current}/")
        try:
            entries = list_dir(ftp, current)
        except ftplib.error_perm as exc:
            print(f"[WARN] Cannot list '{current}': {exc}")
            continue

        for name, facts in entries:
            ftype = facts.get("type", "").lower()
            full  = f"{current}/{name}"

            if ftype in ("dir", "cdir"):
                if is_excluded(full):
                    print(f"[INFO]   Skipping dir  [{_matching_keyword(full)}]: …/{name}/")
                    excluded += 1
                else:
                    queue.append(full)
            elif ftype == "file":
                if Path(name).suffix.lower() in target_exts:
                    if is_excluded(full):
                        print(f"[INFO]   Skipping file [{_matching_keyword(full)}]: …/{name}")
                        excluded += 1
                    else:
                        kept.append(full)
            else:
                if Path(name).suffix.lower() in target_exts:
                    if is_excluded(full):
                        excluded += 1
                    else:
                        kept.append(full)
                elif "." not in name and not is_excluded(full):
                    queue.append(full)

    return kept, excluded


def collect_target_files(ftp: ftplib.FTP, dataset_path: str,
                         ignore_raw: bool = True) -> tuple[list[str], str, int]:
    target_files, n_excluded = walk_ftp(ftp, dataset_path, TARGET_EXTS)
    if target_files or ignore_raw:
        return target_files, "mzML/mzXML", n_excluded

    print("[INFO] No mzML/mzXML files found; checking RAW files instead.")
    raw_files, raw_excluded = walk_ftp(ftp, dataset_path, RAW_TARGET_EXTS)
    return raw_files, "RAW", raw_excluded


def _matching_keyword(path: str) -> str:
    """Return the first keyword that matched in path, for display."""
    m = _EXCLUDE_RE.search(path)
    return m.group(0).lower() if m else "?"


# ---------------------------------------------------------------------------
# Download (sequential, with live progress bar)
# ---------------------------------------------------------------------------

CHUNK = 256 * 1024   # 256 KB — balances progress granularity vs syscall overhead


def _archive_member_name(accession: str, dataset_path: str, remote_path: str) -> str:
    return f"{accession}/{strip_prefix(remote_path, dataset_path)}"


def _add_archive_root(archive: tarfile.TarFile, accession: str) -> None:
    root = tarfile.TarInfo(f"{accession}/")
    root.type = tarfile.DIRTYPE
    root.mode = 0o755
    root.mtime = int(time.time())
    archive.addfile(root)


def _is_broken_pipe(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "broken pipe" in msg or "connection reset" in msg or "timed out" in msg or "eof" in msg


def download_file(ftp: ftplib.FTP, remote_path: str, local_path: Path,
                  retries: int = 3) -> tuple[ftplib.FTP, bool, str]:
    local_path.parent.mkdir(parents=True, exist_ok=True)

    for attempt in range(1, retries + 1):
        progress = Progress(local_path.name, -1)

        try:
            with open(local_path, "wb") as fh:
                def _cb(data: bytes) -> None:
                    fh.write(data)
                    progress.update(len(data))

                ftp.retrbinary(f"RETR {remote_path}", _cb, blocksize=CHUNK)

            progress.finish(ok=True)
            return ftp, True, "OK"

        except Exception as exc:
            progress.finish(ok=False)
            local_path.unlink(missing_ok=True)

            if _is_broken_pipe(exc) and attempt < retries:
                wait = 5 * attempt
                print(f"  [WARN] Connection lost: {type(exc).__name__}: {exc}")
                print(f"  [WARN] File was: ftp://{FTP_HOST}{remote_path}")
                print(f"  [WARN] Reconnecting in {wait}s (attempt {attempt}/{retries}) …")
                time.sleep(wait)
                try: ftp.close()
                except Exception: pass
                ftp = connect_anon()
            else:
                return ftp, False, f"FAILED — {exc}"

    return ftp, False, "FAILED — max retries exceeded"


def _remove_empty_parents(path: Path, stop: Path) -> None:
    current = path.parent
    while True:
        try:
            current.rmdir()
        except OSError:
            break
        if current == stop:
            break
        current = current.parent


def write_archive(ftp: ftplib.FTP, target_files: list[str], dataset_path: str,
                  accession: str, archive_path: Path,
                  delay: float = 0.0) -> ftplib.FTP:
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    part_path = archive_path.with_name(f"{archive_path.name}.part")
    output_dir = archive_path.parent / accession
    part_path.unlink(missing_ok=True)

    try:
        with zstd.open(part_path, "wb", level=ZSTD_LEVEL) as compressed:
            with tarfile.open(fileobj=compressed, mode="w|") as archive:
                _add_archive_root(archive, accession)
                total = len(target_files)

                for i, remote_path in enumerate(target_files, 1):
                    rel = strip_prefix(remote_path, dataset_path)
                    sys.stdout.write(f"\n  [{i}/{total}] {rel}\r")
                    sys.stdout.flush()

                    local_path = output_dir / rel
                    ftp, success, msg = download_file(ftp, remote_path, local_path)
                    if not success:
                        raise RuntimeError(f"{rel}: {msg}")

                    try:
                        archive.add(local_path, arcname=_archive_member_name(accession, dataset_path, remote_path))
                    finally:
                        local_path.unlink(missing_ok=True)
                        _remove_empty_parents(local_path, output_dir)

                    if delay > 0 and i < total:
                        time.sleep(delay)

        part_path.replace(archive_path)
        return ftp
    except Exception:
        part_path.unlink(missing_ok=True)
        raise


def download_archive(ftp: ftplib.FTP, target_files: list[str], dataset_path: str,
                     accession: str, archive_path: Path, delay: float,
                     retries: int = 3) -> tuple[ftplib.FTP, bool, str]:
    for attempt in range(1, retries + 1):
        try:
            ftp = write_archive(ftp, target_files, dataset_path, accession, archive_path, delay)
            return ftp, True, "OK"
        except Exception as exc:
            if _is_broken_pipe(exc) and attempt < retries:
                wait = 5 * attempt
                print(f"  [WARN] Connection lost: {type(exc).__name__}: {exc}")
                print(f"  [WARN] Restarting archive in {wait}s (attempt {attempt}/{retries}) …")
                time.sleep(wait)
                try: ftp.close()
                except Exception: pass
                ftp = connect_anon()
            else:
                return ftp, False, f"FAILED — {exc}"

    return ftp, False, "FAILED — max retries exceeded"


def strip_prefix(path: str, prefix: str) -> str:
    return path[len(prefix):].lstrip("/")


# ---------------------------------------------------------------------------
# Resume log
# ---------------------------------------------------------------------------

def load_completed(log_path: Path) -> set[str]:
    if not log_path.exists():
        return set()
    out = set()
    for line in log_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            out.add(line.upper())
    return out


def mark_completed(log_path: Path, accession: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "a") as f:
        f.write(accession + "\n")


# ---------------------------------------------------------------------------
# Input parsing
# ---------------------------------------------------------------------------

def parse_accessions(args) -> list[str]:
    raw: list[str] = []
    selected_file_lines = 0
    if args.accession:
        raw.append(args.accession)
    if args.file:
        p = Path(args.file)
        if not p.exists():
            sys.exit(f"[ERROR] ID file not found: {p}")
        from_line = getattr(args, "from_line", None)
        to_line = getattr(args, "to_line", None)
        if from_line is not None and from_line < 1:
            sys.exit("[ERROR] --from-line must be >= 1.")
        if to_line is not None and to_line < 1:
            sys.exit("[ERROR] --to-line must be >= 1.")
        if from_line is not None and to_line is not None and from_line > to_line:
            sys.exit("[ERROR] --from-line cannot be greater than --to-line.")

        for line_no, line in enumerate(p.read_text().splitlines(), 1):
            if from_line is not None and line_no < from_line:
                continue
            if to_line is not None and line_no > to_line:
                continue
            selected_file_lines += 1
            line = line.strip()
            if line and not line.startswith("#"):
                raw.append(line)

        if not args.accession and selected_file_lines == 0:
            if from_line is not None or to_line is not None:
                range_label = f"{from_line or 1}..{to_line or 'EOF'}"
                sys.exit(f"[ERROR] No accessions found in selected file lines: {range_label}")
            sys.exit(f"[ERROR] No accessions found in file: {p}")

    seen, result = set(), []
    for acc in raw:
        acc = acc.upper()
        if not re.match(r"^MSV\d{9}$", acc):
            print(f"[WARN] Invalid accession '{acc}' — skipping.")
            continue
        if acc not in seen:
            seen.add(acc)
            result.append(acc)

    if not result:
        sys.exit("[ERROR] No valid accessions provided.")
    return result


# ---------------------------------------------------------------------------
# Per-dataset orchestration
# ---------------------------------------------------------------------------

def download_dataset(accession: str, output_root: Path,
                     dry_run: bool, delay: float, ignore_raw: bool) -> bool:
    archive_path = output_root / f"{accession}{ARCHIVE_SUFFIX}"
    print(f"\n{'='*60}")
    print(f"[INFO] Dataset : {accession}")
    print(f"[INFO] FTP URL : ftp://{FTP_HOST}/{accession}/")
    print(f"[INFO] Browse  : https://massive.ucsd.edu/ProteoSAFe/dataset.jsp?task={accession}")
    print(f"[INFO] Output  : {archive_path}")

    ftp          = connect_anon()
    dataset_path = find_dataset_path(ftp, accession)
    if dataset_path is None:
        ftp.quit()
        return False

    print(f"[INFO] Walking ftp://{FTP_HOST}{dataset_path}/ …")
    target_files, file_kind, n_excluded = collect_target_files(
        ftp, dataset_path, ignore_raw=ignore_raw
    )

    if n_excluded:
        print(f"[INFO] Excluded {n_excluded} path(s) matching: {EXCLUDE_WORDS}")
    if not target_files:
        if ignore_raw:
            print("[INFO] No mzML/mzXML files to download (RAW fallback disabled).")
        else:
            print("[INFO] No mzML/mzXML or RAW files to download (all excluded or unavailable).")
        ftp.quit()
        return True

    total = len(target_files)
    print(f"[INFO] {total} {file_kind} file(s) queued for archive.")

    if dry_run:
        print("\n=== DRY RUN ===")
        for rp in sorted(target_files):
            print(f"  {accession}/{strip_prefix(rp, dataset_path)}")
        ftp.quit()
        return True

    ftp, success, msg = download_archive(
        ftp, target_files, dataset_path, accession, archive_path, delay
    )
    ftp.quit()
    if success:
        print(f"\n[INFO] {accession}: archived {total}/{total} file(s) to {archive_path}.")
    else:
        print(f"  [✗] {msg}")
        print(f"\n[INFO] {accession}: archive failed.")
    return success


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Archive mzML/mzXML files from MassIVE/GNPS datasets, with optional RAW fallback.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python massive_downloader.py MSV000095785\n"
            "  python massive_downloader.py MSV000095785 --output ./data\n"
            "  python massive_downloader.py MSV000095785 --no-ignore-raw --output ./data\n"
            "  python massive_downloader.py --file ids.txt --output ./data\n"
            "  python massive_downloader.py --file ids.txt --from-line 2501 --to-line 3000 --output ./data\n"
            "  python massive_downloader.py --file ids.txt --dry-run\n"
        ),
    )
    parser.add_argument("accession", nargs="?", default=None,
                        help="Single MassIVE accession, e.g. MSV000095785")
    parser.add_argument("--file", "-f", metavar="FILE",
                        help="Text file of accessions (one per line; # = comment)")
    parser.add_argument("--from-line", type=int, default=None,
                        help="Read --file starting at this 1-based line number (inclusive)")
    parser.add_argument("--to-line", type=int, default=None,
                        help="Read --file through this 1-based line number (inclusive)")
    parser.add_argument("--ignore-raw", action=argparse.BooleanOptionalAction, default=True,
                        help="Ignore RAW fallback when no mzML/mzXML files are found [default: true]")
    parser.add_argument("--output", "-o", default=".",
                        help="Root output dir — archives land in <o>/<accession>.tar.zst [default: .]")
    parser.add_argument("--dry-run", "-n", action="store_true",
                        help="List files that would be downloaded without fetching them")
    parser.add_argument("--delay", "-d", type=float, default=1.0,
                        help="Seconds between file downloads [default: 1.0]")
    parser.add_argument("--no-resume", action="store_true",
                        help="Ignore completed.log and re-download all datasets")
    args = parser.parse_args()

    if not args.accession and not args.file:
        parser.error("Provide a single accession or --file with a list of accessions.")
    if (args.from_line is not None or args.to_line is not None) and not args.file:
        parser.error("--from-line/--to-line require --file.")
    if args.from_line is not None and args.from_line < 1:
        parser.error("--from-line must be >= 1.")
    if args.to_line is not None and args.to_line < 1:
        parser.error("--to-line must be >= 1.")
    if args.from_line is not None and args.to_line is not None and args.from_line > args.to_line:
        parser.error("--from-line cannot be greater than --to-line.")

    accessions  = parse_accessions(args)
    output_root = Path(args.output).resolve()
    log_path    = output_root / COMPLETED_LOG

    completed = set() if args.no_resume or args.dry_run else load_completed(log_path)
    if completed:
        before     = len(accessions)
        accessions = [a for a in accessions if a not in completed]
        n_skip     = before - len(accessions)
        if n_skip:
            print(f"[INFO] Skipping {n_skip} already-completed dataset(s) "
                  f"(--no-resume to override).")

    if not accessions:
        print("[INFO] Nothing to do.")
        sys.exit(0)

    n_total = len(accessions)
    print(f"[INFO] {n_total} dataset(s) to process.")
    if not args.dry_run:
        print(f"[INFO] Resume log: {log_path}")

    succeeded, failed, failed_list = 0, 0, []

    for i, accession in enumerate(accessions, 1):
        ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"\n[{i}/{n_total}]  {accession}  {ts}")
        print(f"  Manual check: ftp://{FTP_HOST}/{accession}/")
        print(f"  Dataset page: https://massive.ucsd.edu/ProteoSAFe/dataset.jsp?task={accession}")
        try:
            ok = download_dataset(
                accession, output_root, args.dry_run, args.delay, args.ignore_raw
            )
        except Exception as exc:
            print(f"[ERROR] {accession}: {type(exc).__name__}: {exc}")
            ok = False

        if ok:
            succeeded += 1
            if not args.dry_run:
                mark_completed(log_path, accession)
        else:
            failed += 1
            failed_list.append(accession)

    print(f"\n{'='*60}")
    print(f"[DONE] {succeeded}/{n_total} dataset(s) completed successfully.")
    if failed_list:
        print(f"[WARN] {failed} failed: {', '.join(failed_list)}")
        print("       Re-run to retry; completed datasets will be skipped automatically.")
        sys.exit(1)


if __name__ == "__main__":
    main()
