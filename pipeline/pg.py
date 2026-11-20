"""Replace one work day of a report table in PostgreSQL (create the table on first use)."""
from __future__ import annotations

import io

import polars as pl

PG = {pl.Int64: "bigint", pl.Int32: "integer", pl.UInt32: "bigint", pl.Float64: "double precision",
      pl.Boolean: "boolean", pl.Utf8: "text"}


def replace_day(url: str, table: str, df: pl.DataFrame, day: str) -> int:
    import psycopg2

    cols = ", ".join(f'"{c}" {PG.get(t, "text")}' for c, t in df.schema.items())
    buf = io.StringIO()
    df.write_csv(buf, include_header=False, null_value="\\N")
    buf.seek(0)
    with psycopg2.connect(url) as conn, conn.cursor() as cur:
        cur.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({cols})')
        cur.execute(f'DELETE FROM "{table}" WHERE work_date = %s', (day,))
        cur.copy_expert(f'COPY "{table}" ({", ".join(chr(34) + c + chr(34) for c in df.columns)}) '
                        f"FROM STDIN WITH (FORMAT csv, NULL '\\N')", buf)
    return df.height
