from aiohttp import web
import logging
import datetime

routes = web.RouteTableDef()

logger = logging.getLogger("platform_link.api")

##  datetime.datetime.now(tz=tz).replace(hour=0,minute=0,second=0,microsecond=0) - datetime.timedelta(days=1)


@routes.post("/api/inc/{stat}")
async def increment_stat(request):
    stat_name = request.match_info["stat"]
    if request.can_read_body and request.content_type == "application/json":
        request_data = await request.json()
        increment_amount = request_data.get("increment", 1)
    else:
        increment_amount = 1

    timestamp_now = int(datetime.datetime.now(datetime.timezone.utc).timestamp())

    db = request.config_dict["DB"]
    await db.execute(
        """
        INSERT INTO usage_records (stat, value, timestamp)
        VALUES(:stat, :value, :timestamp);
        """,
        {
            "stat": stat_name,
            "value": increment_amount,
            "timestamp": timestamp_now,
        },
    )
    await db.commit()

    threshold = int(timestamp_now / 3600) * 3600

    async with db.execute(
        """
        SELECT SUM(value) as total 
        FROM usage_records  
        WHERE stat = :stat
        AND timestamp > :last_hour_threshold;
        """,
        {"stat": stat_name, "last_hour_threshold": threshold},
    ) as cursor:
        row = await cursor.fetchone()
        total = row["total"]
        data = {"total": total}

    logger.debug(f"{stat_name} increased by {increment_amount} to {total}")
    return web.json_response(data)


@routes.get("/api/get/{stat}")
async def get_stat(request):
    stat_name = request.match_info["stat"]

    db = request.config_dict["DB"]
    async with db.execute(
        """
        SELECT SUM(value) as total 
        FROM usage_records 
        WHERE stat = :stat;
        """,
        {"stat": stat_name},
    ) as cursor:
        row = await cursor.fetchone()
        if row is None:
            return web.json_response({"error": "not found"})
        else:
            total = row["total"] if row["total"] is not None else 0
            data = {"total": total}
            return web.json_response(data)


@routes.get("/api/all")
async def get_all_stats(request):
    raw_from_threshold = request.query.get("from")
    from_threshold = (
        datetime.datetime.fromisoformat(raw_from_threshold)
        if raw_from_threshold
        else None
    )

    raw_to_threshold = request.query.get("to")
    to_threshold = (
        datetime.datetime.fromisoformat(raw_to_threshold) if raw_to_threshold else None
    )

    filter_clause = ""
    args = {}

    if from_threshold is not None and to_threshold is None:
        filter_clause = "WHERE hour_bucket_ts > :from_threshold"
        args = {
            "from_threshold": int(from_threshold.timestamp()),
        }

    if from_threshold is None and to_threshold is not None:
        filter_clause = "WHERE hour_bucket_ts < :to_threshold"
        args = {
            "to_threshold": int(to_threshold.timestamp()),
        }

    if from_threshold is not None and to_threshold is not None:
        filter_clause = "WHERE hour_bucket_ts BETWEEN :from_threshold and :to_threshold"
        args = {
            "from_threshold": int(from_threshold.timestamp()),
            "to_threshold": int(to_threshold.timestamp()),
        }

    db = request.config_dict["DB"]
    raw_data = {}
    async with db.execute(
        f"""
        SELECT stat, total, hour_bucket_ts 
        FROM usage_aggregated 
        {filter_clause}
        """,
        args
    ) as cursor:
        async for row in cursor:
            if row["stat"] not in raw_data:
                raw_data[row["stat"]] = {}
            raw_data[row["stat"]][row["hour_bucket_ts"]] = row["total"]

    data = [
        {"metric": k, "data": [{"period": ik, "count": iv} for ik, iv in v.items()]}
        for k, v in raw_data.items()
    ]
    return web.json_response(data)


@routes.post("/api/gather")
async def gather_stats(request):
    db = request.config_dict["DB"]

    threshold = (
        int(datetime.datetime.now(datetime.timezone.utc).timestamp() / 3600) * 3600
    )

    await db.execute(
        """
        INSERT INTO usage_aggregated (stat, total, hour_bucket_ts)
        Select stat, SUM(value) as sum , floor(timestamp/3600)*3600 as tbucket from usage_records
        WHERE timestamp < :prev_hour_threshold
        GROUP By stat, tbucket
        ON CONFLICT
        DO UPDATE
        SET total = excluded.total + usage_aggregated.total
        """,
        {"prev_hour_threshold": threshold},
    )
    await db.execute(
        """
        DELETE FROM usage_records
        WHERE timestamp < :prev_hour_threshold
        """,
        {"prev_hour_threshold": threshold},
    )
    async with db.execute(
        """
        SELECT changes() as removed
        """
    ) as cursor:
        row = await cursor.fetchone()
        total_removed = row["removed"]
    await db.commit()
    logger.info(f"Gathered {total_removed} records")
    return web.json_response({"gathered": total_removed})


@routes.post("/api/prune")
async def prune_stats(request):

    request_data = await request.json()
    threshold = datetime.datetime.fromisoformat(request_data["threshold"])

    db = request.config_dict["DB"]
    await db.execute(
        """
        DELETE FROM usage_records
        WHERE timestamp < :timestamp
        """,
        {"timestamp": int(threshold.timestamp())},
    )
    async with db.execute(
        """
        SELECT changes() as removed
        """
    ) as cursor:
        row = await cursor.fetchone()
        total_removed = row["removed"]
    await db.commit()
    logger.info(f"Prune removed {total_removed} records before threshold {threshold}")
    return web.Response(status=200)
