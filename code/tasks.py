from aiohttp import web
import asyncio
import logging
import upload_stats


logger = logging.getLogger("platform_link.tasks")

send_stats_bg_task_key = web.AppKey("send_stats_bg_task_key", asyncio.Task[None])

async def create_background_task(app: web.Application):
    logger.info("starting bg task")
    app[send_stats_bg_task_key] = asyncio.create_task(upload_stats.task(app))
    app[upload_stats.graceful_exit_key] = asyncio.get_running_loop().create_future()
    app.on_shutdown.append(graceful_exit)

    yield

    app[send_stats_bg_task_key].cancel()
    await app[send_stats_bg_task_key]


async def graceful_exit(app):
    exit_future = app[upload_stats.graceful_exit_key]
    exit_future.set_result(True)
