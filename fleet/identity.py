"""Entity resolution: which GPS device really drove each bus today?

Three sources describe the same bus and can disagree:
  - the fleet registry (CloudRoutes): bus -> line, and the Traccar device configured for it;
  - the road papers: the bus number, its line, and the departure time of every trip;
  - the GPS: which line each device actually drove (route fit), and when it left each terminal.

Names are not trusted. A link is accepted on behaviour: the share of the bus's
declared trips that the device made within a few minutes at both ends.

Verdicts (one row per bus):
  confirmed                   registered device drove the line and matches the papers
  confirmed_route_only        registered device drove the paper's line, but the written times do not match it
  matched_other_device        papers match another device far better (needs review)
  contested_device            two buses' papers point to the same device (needs review)
  weak_match                  registered device runs but matches the papers poorly (needs review)
  no_gps_data                 registered device sent nothing usable today
  off_network                 registered device drives none of our lines (name shared with another fleet)
  insufficient_gps            registered device sent too little today to tell (e.g. stopped reporting mid-morning)
  no_device                   no device registered for this bus
  running_without_paper       device drove the line but the bus has no paper today
  inactive                    no paper and no GPS
The output also says which device to use for verification (`resolved_device_id`).
"""
from __future__ import annotations

import polars as pl


def trip_scores(papers: pl.DataFrame, trips: pl.DataFrame, pairs: pl.DataFrame, tol_s: int, short_s: int) -> pl.DataFrame:
    """For each (bus, device) pair: share of the bus's declared trips that the
    device made within `tol_s` at both ends (departure only for a stub whose
    written arrival is not a real time).

    Whole-trip agreement is used, not departures alone: on a busy line any bus
    leaves a terminal within ten minutes of almost any time, so departures
    alone cannot tell two buses of the same line apart."""
    decl = papers.drop_nulls("dep").select("bus", "dep", "arr",
                                          ((pl.col("arr") - pl.col("dep")) <= short_s).alias("short"))
    decl = decl.with_row_index("k")
    gps = trips.filter(pl.col("kind") == "trip").select("device_id", pl.col("dep").alias("g_dep"), pl.col("arr").alias("g_arr"))
    x = (decl.join(pairs, on="bus")
         .join(gps, on="device_id", how="left")
         .with_columns((((pl.col("g_dep") - pl.col("dep")).abs() <= tol_s)
                        & (pl.col("short") | ((pl.col("g_arr") - pl.col("arr")).abs() <= tol_s))).fill_null(False).alias("hit"))
         .group_by(["bus", "device_id", "k"]).agg(pl.col("hit").any()))
    return x.group_by(["bus", "device_id"]).agg(pl.col("hit").mean().alias("score"), pl.len().alias("declared"))


def resolve(registry: pl.DataFrame, devices: pl.DataFrame, best: pl.DataFrame,
            trips: pl.DataFrame, papers: pl.DataFrame, cfg: dict) -> pl.DataFrame:
    ic = cfg["identity"]
    tol = ic["trip_tol_min"] * 60
    names = devices.select(pl.col("id").alias("device_id"), pl.col("name").alias("device_name"),
                           pl.col("status").alias("device_status"))
    paper_buses = papers.group_by("bus").agg(
        pl.col("line").unique().sort().str.join("/").alias("paper_lines"),
        pl.col("paper_id").n_unique().alias("papers"),
        pl.col("dep").is_not_null().sum().alias("declared"))
    gps_dev = best.select("device_id", pl.col("line").alias("gps_line"), pl.col("f").alias("route_fit"), "on_network")
    with_trips = trips.filter(pl.col("kind") == "trip").group_by("device_id").len("gps_trips")

    buses = (registry.select("bus", pl.col("line").alias("registry_line"), pl.col("traccar_device_id").alias("reg_id"))
             .join(paper_buses, on="bus", how="full", coalesce=True)
             .join(gps_dev.rename({"device_id": "reg_id", "gps_line": "reg_gps_line", "route_fit": "reg_fit",
                                   "on_network": "reg_on_network"}), on="reg_id", how="left")
             .join(with_trips.rename({"device_id": "reg_id", "gps_trips": "reg_gps_trips"}), on="reg_id", how="left"))

    # candidate devices: the registered one, plus every device that drove one of the paper lines
    on_net = gps_dev.filter("on_network")
    cand = (buses.filter(pl.col("papers").is_not_null())
            .select("bus", pl.col("paper_lines").str.split("/").alias("pl"))
            .explode("pl", empty_as_null=True)
            .join(on_net.select("device_id", pl.col("gps_line").alias("pl")), on="pl")
            .select("bus", "device_id"))
    reg_pairs = buses.filter(pl.col("papers").is_not_null() & pl.col("reg_id").is_not_null()).select("bus", pl.col("reg_id").alias("device_id"))
    pairs = pl.concat([cand, reg_pairs]).unique()
    sc = trip_scores(papers, trips, pairs, tol, cfg["match"]["short_trip_min"] * 60)

    reg_score = sc.rename({"device_id": "reg_id", "score": "reg_score"}).drop("declared")
    # best device for each bus other than its registered one; a device can serve only one bus,
    # so when two buses want the same device the higher score keeps it
    # A device registered to another bus that has its own papers is not available.
    owned = buses.filter(pl.col("papers").is_not_null() & pl.col("reg_id").is_not_null()).select(pl.col("reg_id").alias("device_id"), pl.col("bus").alias("owner"))
    ranked = (sc.join(buses.select("bus", "reg_id"), on="bus")
                .filter(pl.col("device_id") != pl.col("reg_id").fill_null(-1))
                .join(owned, on="device_id", how="left").filter(pl.col("owner").is_null())
                .sort(["bus", "score"], descending=[False, True])
                .with_columns(pl.int_range(pl.len()).over("bus").alias("r")))
    # The best other device must also stand clearly above the next one; otherwise the
    # papers fit several buses of the line equally and say nothing about identity.
    alt = (ranked.filter(pl.col("r") == 0).select("bus", pl.col("device_id").alias("alt_id"), pl.col("score").alias("alt_score"))
           .join(ranked.filter(pl.col("r") == 1).select("bus", pl.col("score").alias("alt_next")), on="bus", how="left")
           .with_columns(pl.col("alt_next").fill_null(0.0)))

    b = (buses.join(reg_score, on=["bus", "reg_id"], how="left").join(alt, on="bus", how="left")
         .with_columns(pl.col("reg_score").fill_null(0.0), pl.col("alt_score").fill_null(0.0)))

    has_paper = pl.col("papers").is_not_null()
    reg_runs = pl.col("reg_gps_trips").fill_null(0) > 0
    reg_ok = reg_runs & pl.col("reg_on_network").fill_null(False)
    alt_wins = ((pl.col("alt_score") >= ic["alt_min"]) & ((pl.col("alt_score") - pl.col("reg_score")) >= ic["alt_margin"])
                & ((pl.col("alt_score") - pl.col("alt_next")) >= ic["unique_margin"]))
    reg_on_paper_line = pl.col("paper_lines").str.contains(pl.col("reg_gps_line").fill_null("~"))
    b = b.with_columns(
        pl.when(~has_paper & reg_ok).then(pl.lit("running_without_paper"))
          .when(~has_paper).then(pl.lit("inactive"))
          .when(reg_ok & (pl.col("reg_score") >= ic["confirm"])).then(pl.lit("confirmed"))
          .when(alt_wins).then(pl.lit("matched_other_device"))
          .when(reg_ok & reg_on_paper_line).then(pl.lit("confirmed_route_only"))
          .when(reg_ok).then(pl.lit("weak_match"))
          .when(pl.col("reg_fit").is_not_null() & (pl.col("reg_fit") < ic["off_network_max_f"])).then(pl.lit("off_network"))
          .when(pl.col("reg_fit").is_not_null()).then(pl.lit("insufficient_gps"))
          .when(pl.col("reg_id").is_not_null()).then(pl.lit("no_gps_data"))
          .otherwise(pl.lit("no_device")).alias("verdict"))
    # off_network only when the device drove but not on our lines; a device that is on-network but made no trip has no usable data
    b = b.with_columns(
        pl.when((pl.col("verdict") == "off_network") & pl.col("reg_on_network").fill_null(False))
          .then(pl.lit("no_gps_data")).otherwise(pl.col("verdict")).alias("verdict"))
    b = b.with_columns(
        pl.when(pl.col("verdict").is_in(["confirmed", "confirmed_route_only", "weak_match", "running_without_paper"])).then(pl.col("reg_id"))
          .when(pl.col("verdict") == "matched_other_device").then(pl.col("alt_id"))
          .otherwise(None).alias("resolved_device_id"),
        pl.col("verdict").is_in(["matched_other_device", "weak_match", "off_network", "insufficient_gps"]).alias("needs_review"))
    # two buses claiming the same device: the higher score keeps it, a tie leaves both unresolved
    claims = b.filter(pl.col("verdict") == "matched_other_device").select("bus", "alt_id", "alt_score")
    claims = claims.with_columns(pl.col("alt_score").rank("min", descending=True).over("alt_id").alias("cr"),
                                 (pl.col("alt_score") == pl.col("alt_score").max().over("alt_id")).sum().over("alt_id").alias("ntop"))
    losers = claims.filter((pl.col("cr") > 1) | (pl.col("ntop") > 1)).select("bus", pl.lit(True).alias("lost"))
    b = b.join(losers, on="bus", how="left").with_columns(
        pl.when(pl.col("lost").fill_null(False)).then(pl.lit("contested_device")).otherwise(pl.col("verdict")).alias("verdict"))
    b = b.with_columns(
        pl.when(pl.col("verdict") == "contested_device").then(None).otherwise(pl.col("resolved_device_id")).alias("resolved_device_id"),
        (pl.col("needs_review") | (pl.col("verdict") == "contested_device")).alias("needs_review"))
    # a device whose registered bus has no paper, but whose trips match another bus's paper
    used = b.filter(pl.col("verdict") == "matched_other_device").select(pl.col("alt_id").alias("reg_id"), pl.col("bus").alias("used_by"))
    b = b.join(used, on="reg_id", how="left").with_columns(
        pl.when(pl.col("used_by").is_not_null()).then(pl.lit("device_used_by_other_bus")).otherwise(pl.col("verdict")).alias("verdict"),
        pl.when(pl.col("used_by").is_not_null()).then(None).otherwise(pl.col("resolved_device_id")).alias("resolved_device_id"))
    b = (b.join(names.rename({"device_id": "reg_id", "device_name": "registered_device", "device_status": "registered_status"}), on="reg_id", how="left")
          .join(names.rename({"device_id": "alt_id", "device_name": "alt_device"}).drop("device_status"), on="alt_id", how="left")
          .join(gps_dev.select(pl.col("device_id").alias("resolved_device_id"), pl.col("gps_line").alias("resolved_line")), on="resolved_device_id", how="left")
          .with_columns((pl.col("paper_lines").is_not_null() & pl.col("resolved_line").is_not_null()
                         & ~pl.col("paper_lines").str.contains(pl.col("resolved_line").fill_null("~"))).alias("paper_line_differs")))
    return b.select(
        "bus", "registry_line", "paper_lines", "papers", "declared",
        pl.col("reg_id").alias("registered_device_id"), "registered_device", "registered_status",
        pl.col("reg_gps_line").alias("registered_gps_line"), pl.col("reg_fit").round(2).alias("registered_route_fit"),
        pl.col("reg_score").round(2).alias("registered_trip_match"),
        "alt_device", pl.col("alt_score").round(2).alias("alt_trip_match"), pl.col("alt_next").round(2).alias("alt_runner_up"),
        "resolved_device_id", "resolved_line", "verdict", "needs_review", "paper_line_differs", "used_by",
    ).sort("bus")
