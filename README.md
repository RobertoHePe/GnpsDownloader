# MassIVE/GNPS Downloader

Download MassIVE/GNPS dataset files over FTP and store each dataset as a
compressed tar archive.

## Usage

Download one dataset:

```bash
python3 massive_downloader.py MSV000095785 --output ./data
```

Read dataset IDs from a file:

```bash
python3 massive_downloader.py --file massive_ids_after_2500.txt --output ./data
```

Process only part of an ID file, using 1-based inclusive line numbers:

```bash
python3 massive_downloader.py --file massive_ids_after_2500.txt --from-line 2501 --to-line 3000 --output ./data
```

Preview what would be downloaded:

```bash
python3 massive_downloader.py MSV000095785 --dry-run
```

## Output

Each dataset is written to:

```text
<output>/<MSV_ID>.tar.zst
```

Files inside the archive keep the MassIVE dataset as the top folder:

```text
MSV000095785/path/from/dataset/file.mzML
```

Temporary downloaded files are deleted after they are added to the archive.

## RAW Fallback

By default, RAW files are ignored. The downloader only archives mzML/mzXML files.

To download `.raw` / `.RAW` / `.Raw` files when no mzML/mzXML files are found:

```bash
python3 massive_downloader.py MSV000095785 --no-ignore-raw --output ./data
```

## Resume

Completed dataset IDs are appended to:

```text
<output>/completed.log
```

Re-running the same command skips completed datasets. Use `--no-resume` to
process them again.

## Filtering

Paths containing these words are skipped:

```text
neg, qc, blank, blanc, control
```

