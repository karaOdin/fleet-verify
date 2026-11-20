"""GPS positions: one normalised table for every source (Traccar API, Kafka lake, CSV export)."""
from __future__ import annotations

import polars as pl

SCHEMA = {"device_id": pl.Int64, "t": pl.Datetime("us"), "s": pl.Int64,
          "lat": pl.Float64, "lon": pl.Float64, "kmh": pl.Float64}


def normalise(raw: pl.DataFrame, tz: str) -> pl.DataFrame:
    """raw columns: device_id, fix_time_utc (ISO string or datetime), lat, lon, speed_kn.
    Output: local wall-clock time `t`, seconds since local midnight `s`, speed in km/h."""
    ft = pl.col("fix_time_utc")
    if raw.schema["fix_time_utc"] == pl.Utf8:
        ft = ft.str.to_datetime(time_zone="UTC")
    else:
        ft = ft.dt.replace_time_zone("UTC") if raw.schema["fix_time_utc"].time_zone is None else ft
    t = ft.dt.convert_time_zone(tz).dt.replace_time_zone(None).dt.cast_time_unit("us")
    return (
        raw.select(
            pl.col("device_id").cast(pl.Int64),
            t.alias("t"),
            pl.col("lat").cast(pl.Float64),
            pl.col("lon").cast(pl.Float64),
            (pl.col("speed_kn").cast(pl.Float64) * 1.852).alias("kmh"),
        )
        .with_columns((pl.col("t").dt.hour().cast(pl.Int64) * 3600
                       + pl.col("t").dt.minute().cast(pl.Int64) * 60
                       + pl.col("t").dt.second().cast(pl.Int64)).alias("s"))
        .sort(["device_id", "t"], maintain_order=True)
        .select(list(SCHEMA))
    )


def with_xy(df: pl.DataFrame, plane) -> pl.DataFrame:
    xy = plane.xy(df["lat"].to_numpy(), df["lon"].to_numpy())
    return df.with_columns(pl.Series("x", xy[:, 0]), pl.Series("y", xy[:, 1]))
