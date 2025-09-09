import aiohttp
import aiohttp.client_exceptions
import logging
import aiohttp_jinja2
import json
import upload_stats
import base64
import datetime
import functools
import time
import os
import traceback
from upload_stats import solution_type_key

routes = aiohttp.web.RouteTableDef()

logger = logging.getLogger("platform_link.ui")

exchange_token_key = aiohttp.web.AppKey("exchange_token", str)
streams_list_cache_key = aiohttp.web.AppKey("streams_list_cache", list[str])
# exchange_token_body_key = aiohttp.web.AppKey("exchange_token_body", str)

LINKED_COMPANY_FILE = "./data/linked_company"
DEBUG_MODE = os.getenv("DEBUG","").upper() == "TRUE"

def decode_jwt_payload(jwt):
    if isinstance(jwt, str):
        jwt = bytes(jwt, "utf-8")

    b64_header, b64_payload, b64_jwt_signature = jwt.split(b".")
    return json.loads(
        base64.urlsafe_b64decode(b64_payload + b"==")
    )  # add extra == padding as they are stripped in generation


@routes.get("/")
@aiohttp_jinja2.template("dashboard.jinja2")
async def home_page(request):

    __dt = -1 * (time.timezone if (time.localtime().tm_isdst == 0) else time.altzone)
    tz = datetime.timezone(datetime.timedelta(seconds=__dt))

    now = datetime.datetime.now(tz=tz)

    params = {
        "query": f'sum by (service_module, level) (count_over_time({{level=~"warn|warning|error|critical"}}[1h]))',
        "start": (now - datetime.timedelta(days=1)).isoformat(),
        "end": now.isoformat(),
        "direction": "forward",
        "step": "1h",
    }
    logger.debug(f"log quantity params: {params}")
    results = []
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "http://loki.docker.local:3100/loki/api/v1/query_range", params=params
            ) as resp:
                if resp.status == 200:
                    resp_body = await resp.json()
                    raw_results = resp_body["data"]["result"]
                    # merged_logs = functools.reduce(reduce_logs, raw_results, [])
                    # merged_logs.sort(key=lambda x: x[0])
                    results = [
                        {
                            "metric": entry["metric"],
                            "data": [
                                (
                                    datetime.datetime.fromtimestamp(int(value_entry[0])),
                                    value_entry[1],
                                )
                                for value_entry in entry["values"]
                            ],
                        }
                        for entry in raw_results
                    ]
                else:
                    logger.error(
                        f"Log query returned status:{resp.status} for url: {resp.request_info.url}"
                    )
    except aiohttp.client_exceptions.ClientError:
        pass

    now_rounded = now.replace(hour=now.hour + 1, minute=0, second=0, microsecond=0)

    labels = [now_rounded - datetime.timedelta(hours=x) for x in range(26)]
    labels.reverse()
    return {"results": results, "labels": labels}


@routes.get("/logs")
@aiohttp_jinja2.template("logs.jinja2")
async def logs_page(request):
    levels = ["debug", "info", "warning", "error", "critical", "not set"]
    level_map = {
        "debug": "debug",
        "info": "info",
        "warning": "warn|warning",
        "error": "error",
        "critical": "critical",
        "not set": "not_set",
    }
    # streams = request.app.get(streams_list_cache_key)
    streams = []

    # if streams is None:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(
                "http://loki.docker.local:3100/loki/api/v1/series",
            ) as resp:
                if resp.status == 200:
                    resp_body = await resp.json()
                    try:
                        streams_dict = {
                            entry["service_module"]: entry["shoestring_function"]
                            for entry in resp_body["data"]
                            if entry.get("shoestring_solution")
                            == request.app[solution_type_key]
                        }
                        streams = [(k, v) for k, v in streams_dict.items()]
                        streams.sort(
                            key=lambda x: (1 if x[1] == "infrastructure" else 0, x[0])
                        )
                        # request.app[streams_list_cache_key] = streams
                    except:
                        logger.error("Something went wrong getting series")
                        logger.error(traceback.format_exc())
    except aiohttp.client_exceptions.ClientError:
        pass

    checked_stream = request.query.get("stream")
    if checked_stream is None and len(streams)>0:
        checked_stream = streams[0][0]

    checked_levels = request.query.getall("level", ["warning", "error", "critical"])
    end_date = datetime.date.today()
    dates = [end_date - datetime.timedelta(days=x) for x in range(7)]
    raw_date = request.query.get("date")
    selected_date = datetime.date.fromisoformat(raw_date) if raw_date else dates[0]
    selected_time = request.query.get("time", "00:00")

    __dt = -1 * (time.timezone if (time.localtime().tm_isdst == 0) else time.altzone)
    tz = datetime.timezone(datetime.timedelta(seconds=__dt))

    start_time = datetime.datetime.combine(
        selected_date,
        datetime.time.fromisoformat(selected_time),
        tzinfo=tz,
    )
    logs = []
        
    if checked_stream is not None:
        # TODO: could use sort metic aggregation query to sort at source instead of sorting here
        params = {
            "query": f'{{service_module="{checked_stream}",level=~"{"|".join([level_map[level] for level in checked_levels])}"}}',
            "start": start_time.isoformat(),
            "end": (start_time + datetime.timedelta(days=1)).isoformat(),
            "direction": "forward",
            "limit": 1000,
        }
        logger.debug(f"log query params: {params}")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    "http://loki.docker.local:3100/loki/api/v1/query_range", params=params
                ) as resp:
                    if resp.status == 200:
                        resp_body = await resp.json()
                        raw_logs = resp_body["data"]["result"]
                        merged_logs = functools.reduce(reduce_logs, raw_logs, [])
                        merged_logs.sort(key=lambda x: x[0])
                        logs = [
                            (
                                datetime.datetime.fromtimestamp(int(entry[0]) / 1000000000),
                                entry[1],
                            )
                            for entry in merged_logs
                        ]
                    else:
                        logger.error(
                            f"Log query returned status:{resp.status} for url: {resp.request_info.url}"
                        )
        except aiohttp.client_exceptions.ClientError:
            pass

    return {
        "streams": streams,
        "levels": levels,
        "checked_stream": checked_stream,
        "checked_levels": checked_levels,
        "dates": dates,
        "selected_date": selected_date,
        "selected_time": selected_time,
        "logs": logs,
    }


def reduce_logs(acc, entry):
    acc.extend(entry["values"])
    return acc


@routes.get("/report")
@aiohttp_jinja2.template("report_issue.jinja2")
async def home_page(request):
    return {"text": "hello in template"}


@routes.get("/stats")
@routes.post("/stats")
@aiohttp_jinja2.template("transmitted_stats.jinja2")
async def stats_page(request):
    if request.method == "POST":
        form_data = await request.post()
        await upload_stats.update_uploaded_metrics(
            request.app,
            form_data.get("running", False) == "on",
            form_data.get("usage", False) == "on",
        )

    upload_logs = []

    db = request.config_dict["DB"]
    async with db.execute(
        """
        SELECT type, payload, timestamp 
        FROM sent_reports
        ORDER BY
            timestamp DESC;
        """
    ) as cursor:
        async for row in cursor:
            upload_logs.append(
                {
                    "type": row["type"],
                    "payload": row["payload"],
                    "timestamp": datetime.datetime.fromtimestamp(row["timestamp"]),
                }
            )

    logger.debug(upload_logs)

    return {
        "running": request.app[upload_stats.upload_running_key],
        "usage": request.app[upload_stats.upload_usage_key],
        "upload_logs": upload_logs,
    }


@routes.get("/link")
@aiohttp_jinja2.template("link.jinja2")
async def link_page(request):
    output = {"status": "new"}

    try:
        payload = decode_jwt_payload(request.app[upload_stats.id_token_key])
        output["linked_to"] = {"org": payload["org"], "dep": payload["dep"]}
        output["status"] = "linked"
    except:
        # ignore if failed to access
        pass

    return output


@routes.post("/link")
@aiohttp_jinja2.template("link.jinja2")
async def link_page_post(request):
    linked_to = None

    try:
        payload = decode_jwt_payload(request.app[upload_stats.id_token_key])
        linked_to = {"org": payload["org"], "dep": payload["dep"]}
    except:
        # ignore if failed to access
        pass

    data = await request.post()
    link_code = data["link_code"]
    try:
        id_token = request.app[upload_stats.id_token_key]
    except KeyError:
        return {
            "linked_to": linked_to,
            "link_code": link_code,
            "error": "Error: Couldn't access anonymous registration for this deployment. If you've just started this solution for the first time, try again in a few minutes.",
            "status": "error",
        }

    body = {"code": link_code, "id_token": id_token}

    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://platform.digitalshoestring.net/api/deployments/link-flow/associate",
            json=body,
            ssl=not DEBUG_MODE,
        ) as resp:
            if resp.status == 200:
                resp_body = await resp.json()
                try:
                    exchange_token = resp_body["exchange_token"]
                    request.app[exchange_token_key] = exchange_token
                    exchange_token_body = decode_jwt_payload(exchange_token)
                    # request.app[exchange_token_body_key] = exchange_token_body

                    return {
                        "conf_code": resp_body["confirm_code"],
                        "org_name": exchange_token_body["org"],
                        "deployment_name": exchange_token_body["dep"],
                        "status": "confirm",
                    }
                except ValueError:
                    return {
                        "linked_to": linked_to,
                        "link_code": link_code,
                        "error": "Error: server response did not have the expected fields",
                        "status": "error",
                    }
            else:
                resp_body = await resp.json()
                return {
                    "linked_to": linked_to,
                    "link_code": link_code,
                    "error": resp_body.get(
                        "detail",
                        f"Error: Got status code {resp.status} -- {json.dumps(resp_body)}.",
                    ),
                    "status": "error",
                }


@routes.post("/link/exchange")
async def exchange_ajax(request):
    exchange_token = request.app[exchange_token_key]
    body = {"exchange_token": exchange_token}

    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://platform.digitalshoestring.net/api/deployments/link-flow/exchange",
            json=body,
            ssl=not DEBUG_MODE,
        ) as resp:
            if resp.status == 200:
                resp_body = await resp.json()
                new_id_token = resp_body["new_id_token"]
                await upload_stats.update_id_token(request.app, new_id_token)
                return aiohttp.web.Response(status=200)
            else:
                resp_body = await resp.json()
                return aiohttp.web.json_response(resp_body, status=resp.status)


@routes.post("/link/cancel")
@aiohttp_jinja2.template("link_cancel.jinja2")
async def cancel_link_page(request):
    exchange_token = request.app[exchange_token_key]
    body = {"exchange_token": exchange_token}

    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://platform.digitalshoestring.net/api/deployments/link-flow/cancel-exchange",
            json=body,
            ssl=not DEBUG_MODE,
        ) as resp:
            if resp.status == 200:
                return {"result": "ok"}
            else:
                resp_body = await resp.json()
                return {
                    "result": "error",
                    "error": resp_body.get(
                        "detail",
                        f"Error: Got status code {resp.status} -- {json.dumps(resp_body)}.",
                    ),
                }
