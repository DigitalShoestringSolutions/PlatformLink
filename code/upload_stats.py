import aiohttp
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio
import logging
import traceback
import datetime
import json
import os
from database import get_setting, set_setting

try:
    import tomllib
except ImportError:
    import tomli as tomllib

logger = logging.getLogger("platform_link.upload_stats")


URL_BASE = "https://platform.digitalshoestring.net/api/stats/deployments/v1"
CONFIG_FILENAME = "./config/config.toml"
ID_FILENAME = "./data/id_token"

DEBUG_MODE = os.getenv("DEBUG", "").upper() == "TRUE"

graceful_exit_key = aiohttp.web.AppKey("graceful_exit_key", str)
id_token_key = aiohttp.web.AppKey("id_token", str)
solution_type_key = aiohttp.web.AppKey("solution_type", str)
scheduler_key = aiohttp.web.AppKey("scheduler", AsyncIOScheduler)

upload_running_key = aiohttp.web.AppKey("upload_running", bool)
upload_usage_key = aiohttp.web.AppKey("upload_usage", bool)


class IrrecoverableProblem(Exception):
    pass


async def task(app):
    logger.info("Stats sender started as background task")
    while True:
        try:
            ''' 
            This ID token is a JWT.
            Before linking with a company it contains:
            {
                "type":"anon",
                "id": "<anonymous id as UUID>",
                "iat": <issue timestamp>
            },
            After linking it contains:
            {
                "type":"link",
                "id": "<linked id as UUID>",
                "company": "<Company Name at time of link>",
                "iat": <issue timestamp>
            },
            '''
            id_token = await get_id_token()
            config = get_config()
            if id_token is None:
                id_token = await register_anonymous_deployment(app,config)

            app[solution_type_key] = config["solution_type"]
            app[id_token_key] = id_token

            db = app["DB"]
            upload_running_setting = await get_setting(db, "upload_running")
            logger.debug(f"upload_running_setting: {upload_running_setting}")
            if upload_running_setting is not None:
                app[upload_running_key] = upload_running_setting=="True"
            else: 
                app[upload_running_key] = config["default_transmitted_stats"]["running"]

            upload_usage_setting = await get_setting(db, "upload_usage")
            logger.debug(f"upload_usage_setting: {upload_usage_setting}")
            if upload_usage_setting is not None:
                app[upload_usage_key] = upload_usage_setting=="True"
            else: 
                app[upload_usage_key] = config["default_transmitted_stats"]["usage"]

            scheduler = AsyncIOScheduler()

            app[scheduler_key] = scheduler

            scheduler.add_job(  # Runs once immediately
                submit_started_report,
                args=[app],
                id="started",
            )

            scheduler.add_job(  # Runs once immediately
                submit_usage_report,
                args=[app],
                id="startup_report_usage",
            )

            scheduler.add_job(
                submit_heartbeat_report,
                args=[app],
                trigger="interval",
                hours=3,
                jitter=60,  # seconds
                id="heartbeat",
                coalesce=True,
                misfire_grace_time=10800,  # 3h in seconds
            )

            scheduler.add_job(
                submit_usage_report,
                args=[app],
                trigger="interval",
                hours=24,
                jitter=60,  # seconds
                id="report_usage",
                coalesce=True,
                misfire_grace_time=84600,  # 24h in seconds
            )

            scheduler.add_job(
                db_cleanup,
                args=[app],
                trigger="interval",
                hours=24,
                jitter=60,  # seconds
                id="db_cleanup",
                coalesce=True,
                misfire_grace_time=84600,  # 24h in seconds
            )

            scheduler.start()

            logger.info("Scheduler set up. Main task waiting till exit signal")
            await app[graceful_exit_key]
            scheduler.shutdown()
            await on_exit(app)
            logger.info("Graceful exit complete")
            break
        except IrrecoverableProblem as e:
            logger.critical(f"Something went badly wrong: {e}")
            logger.critical("The stats uploader is now stopping - it will try again on next boot")
            break
        except asyncio.CancelledError:
            logger.debug("Asyncio task cancelled")
            await on_exit(app)
            break
        except:
            logger.error(
                f"Something unexpected went very wrong: {traceback.format_exc()} - Uploader terminating"
            )


async def get_id_token():
    retry = 0
    while retry < 3:
        logger.debug(f"Fetching id_token from file. Attempt ({retry+1})")
        try:
            with open(ID_FILENAME, "r") as fp:
                id_token = fp.read()
                logger.info("id_token read from file")
                return id_token

        except FileNotFoundError:
            logger.info(f"id file {ID_FILENAME} not found: {traceback.format_exc()}")
            return None
        except:
            logger.error(
                f"Unable to open ID file {ID_FILENAME}: {traceback.format_exc()}"
            )
            retry += 1
            await asyncio.sleep(2)

    logger.critical("Ran out of retries while trying to read ID file")
    raise IrrecoverableProblem("Anonymous ID file exists, but the application can't read it")


async def update_id_token(app,new_id_token):
    app[id_token_key] = new_id_token
    await write_id_token(new_id_token)
    # scheduler = app[scheduler_key]
    # for job in scheduler.get_jobs():
    #     job.modify(args=[new_id_token])

async def update_uploaded_metrics(app,new_running,new_usage):
    # update app keys
    prev_running = app[upload_running_key]
    prev_usage = app[upload_usage_key]
    app[upload_running_key] = new_running
    app[upload_usage_key] = new_usage
    # save to db
    db = app["DB"]
    await set_setting(db, "upload_running", "True" if new_running else "False")
    await set_setting(db, "upload_usage", "True" if new_usage else "False")

    # notify platform of stops
    if prev_running and not new_running:
        pass
    if prev_usage and not new_usage:
        pass

async def write_id_token(id_token):
    retry = 0
    while retry < 3:
        logger.debug(f"Writing id_token to file. Attempt ({retry+1})")
        try:
            with open(ID_FILENAME, "w") as fp:
                fp.write(id_token)
                logger.info("Successfully saved id_token to file")
                return
        except:
            logger.error(
                f"Unable to open id file {ID_FILENAME}: {traceback.format_exc()}"
            )
            retry += 1
            await asyncio.sleep(2)

    raise IrrecoverableProblem("Ran out of retries while trying to write to ID file")


async def register_anonymous_deployment(app,config):
    payload = {
        "solution_type": config["solution_type"],
        "solution_version": config["solution_version"],
    }
    logger.info(f"Registering Deployment >> {payload}")
    
    db = app["DB"]

    async with aiohttp.ClientSession() as session:
        retry = 0
        while retry < 3:
            logger.debug(f"Submitting registration. Attempt ({retry+1})")
            try:
                async with session.post(
                    f"{URL_BASE}/register",
                    json=payload,
                    ssl= not DEBUG_MODE
                ) as resp:
                    if resp.status == 201:
                        resp_body = await resp.json()
                        anonymous_id_token = resp_body["token"]
                        await write_id_token(anonymous_id_token)
                        timestamp_now = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
                        await db.execute(
                            """
                            INSERT INTO sent_reports (type, payload, timestamp)
                            VALUES(:type, :payload, :timestamp);
                            """,
                            {
                                "type": "register",
                                "payload": json.dumps(payload),
                                "timestamp": timestamp_now,
                            },
                        )
                        await db.commit()
                        return anonymous_id_token
                    else:
                        logger.error(f"Registration failed: http response {resp.status}")
            except:
                logger.error(
                    f"Registration failed: {traceback.format_exc()} - will retry"
                )
            finally:
                await asyncio.sleep(60)
                retry += 1
        raise IrrecoverableProblem("Unable to register deployment - stopping for now - will retry on next restart")

def get_config():
    try:
        with open(CONFIG_FILENAME, "rb") as f:
            config = tomllib.load(f)
            return config
    except FileNotFoundError:
        raise IrrecoverableProblem(f"Expected config file at {CONFIG_FILENAME}. File Not Found - unable to load config.")


async def submit_started_report(app):
    if app.get(upload_running_key) == True:
        report = {"event": "started"}
        await submit_running_report(app, report)
    else:
        logger.debug(
            f"Did not submit started report because upload_running set to {app.get(upload_running_key)}"
        )


async def submit_heartbeat_report(app):
    if app.get(upload_running_key) == True:
        report = {"event": "heartbeat"}
        await submit_running_report(app, report)
    else:
        logger.debug(
            f"Did not submit heartbeat report because upload_running set to {app.get(upload_running_key)}"
        )


async def submit_stopped_report(app):
    if app.get(upload_running_key) == True:
        report = {"event": "stopped"}
        await submit_running_report(app, report)
    else:
        logger.debug(
            f"Did not submit stopped report because upload_running set to {app.get(upload_running_key)}"
        )

async def submit_running_report(app, report):
    id_token = app[id_token_key]
    db = app["DB"]
    headers = {"Authorization": f"Token {id_token}"}
    retry = 0
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=10),
        headers=headers
    ) as session:
        while retry < 3:
            logger.debug(f"Submitting running report. Attempt ({retry+1})")
            try:
                async with session.post(
                    f"{URL_BASE}/report/running", json=report, ssl=not DEBUG_MODE
                ) as resp:
                    if resp.status == 200:
                        logger.info(f"Running report submission complete")
                        timestamp_now = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
                        await db.execute(
                            """
                            INSERT INTO sent_reports (type, payload, timestamp)
                            VALUES(:type, :payload, :timestamp);
                            """,
                            {
                                "type": "running",
                                "payload": json.dumps(report),
                                "timestamp": timestamp_now,
                            }
                        )
                        await db.commit()
                        return
                    else:
                        logger.warning(
                            f"Report transmission failed with code: {resp.status}. Will retry."
                        )
            except aiohttp.ClientConnectionError:
                logger.error("Unable to connect to target")
            except asyncio.CancelledError:
                raise #prevent suppressing error on scheduler shutdown or task cancel
            except:
                logger.error(
                    f"Running report transmission failed: {traceback.format_exc()}"
                )
            finally:
                await asyncio.sleep(60)
                retry += 1

        logger.error(
            f"Ran out of retries while trying to send running report - will try again at next time interval."
        )

last_run = None


async def submit_usage_report(app):
    global last_run
    if app.get(upload_usage_key) == True:
        id_token = app[id_token_key]
        db = app["DB"]
        now_floor_1h = datetime.datetime.now(tz=datetime.timezone.utc).replace(minute=0,second=0,microsecond=0)
        now_floor_1h_minus_24h = now_floor_1h - datetime.timedelta(days=1)

        timespan_from = last_run if last_run is not None else now_floor_1h_minus_24h

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10)
        ) as session:
            try:
                async with session.post(
                    "http://localhost/api/gather",
                ) as resp:
                    pass
                async with session.get(
                    "http://localhost/api/all",
                    params={
                        "from": timespan_from.isoformat(),
                        "to": now_floor_1h.isoformat(),
                    },
                ) as resp:
                    report = await resp.json()
                    logger.debug(f"Usage: {report}")
                    last_run = now_floor_1h

            except aiohttp.ClientConnectionError:
                logger.error("Unable to connect to internal target")
                raise
            except asyncio.CancelledError:
                raise #prevent suppressing error on scheduler shutdown or task cancel
            except:
                logger.error(f"Usage data gathering failed: {traceback.format_exc()}")
                raise

        headers = {"Authorization": f"Token {id_token}"}
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10), headers=headers
        ) as session:
            retry = 0
            while retry < 3:
                logger.debug(f"Submitting usage report. Attempt ({retry+1})")
                try:
                    async with session.post(
                        f"{URL_BASE}/report/usage",
                        json=report,
                        ssl=not DEBUG_MODE,
                    ) as resp:
                        if resp.status == 200:
                            logger.info(f"Usage report submission complete")
                            timestamp_now = int(datetime.datetime.now(datetime.timezone.utc).timestamp())
                            await db.execute(
                                """
                                INSERT INTO sent_reports (type, payload, timestamp)
                                VALUES(:type, :payload, :timestamp);
                                """,
                                {
                                    "type": "usage",
                                    "payload": json.dumps(report),
                                    "timestamp": timestamp_now,
                                },
                            )
                            await db.commit()
                            return
                        else:
                            logger.warning(
                                f"Report transmission failed with code: {resp.status}. Will retry."
                            )
                            logger.debug(f"Error Returned: {resp.content}")
                except aiohttp.ClientConnectionError:
                    logger.error("Unable to connect to target")
                except asyncio.CancelledError:
                    raise #prevent suppressing error on scheduler shutdown or task cancel
                except:
                    logger.error(
                        f"Usage report transmission failed: {traceback.format_exc()}"
                    )
                finally:
                    await asyncio.sleep(60)
                    retry += 1

            logger.error(
                f"Ran out of retries while trying to send usage report - will try again at next time interval."
            )
    else:
        logger.debug(
            f"Did not submit usage report because upload_usage set to {app.get(upload_usage_key)}"
        )


async def db_cleanup(app):
    db = app["DB"]
    today = datetime.datetime.now(tz=datetime.timezone.utc).replace(hour=0,minute=0,second=0,microsecond=0)
    week_ago = today - datetime.timedelta(days=7)
    threshold = week_ago.timestamp()
    await db.execute(
        """
        DELETE from sent_reports
        WHERE timestamp < :threshold
        """,
        {"threshold": threshold},
    )
    async with db.execute(
        """
        SELECT changes() as removed
        """
    ) as cursor:
        row = await cursor.fetchone()
        total_removed = row["removed"]
    await db.commit()
    logger.info(f"Removed {total_removed} old entries from the upload log")

async def on_exit(app):
    logger.debug("Handling Exit")
    await submit_stopped_report(app)
    logger.info("Sent stop report")
