from aiohttp import web
import aiohttp_jinja2
import jinja2
import logging

import tasks
import database

import pages.api
import pages.alert_relay
import pages.ui

import asyncio


# Set log level
logging.basicConfig(level=logging.DEBUG)
logging.getLogger("aiosqlite").setLevel(logging.INFO)
logging.getLogger("aiohttp").setLevel(logging.INFO)
logger = logging.getLogger("platform_link")


async def start_external_app():
    external_app = web.Application()    # available externally on port 80
    # set up templating
    aiohttp_jinja2.setup(external_app, loader=jinja2.FileSystemLoader("./templates"))
    # Routes
    external_app.add_routes(pages.api.routes)
    external_app.add_routes(pages.ui.routes)
    # Background Tasks
    external_app.cleanup_ctx.append(database.init)

    runner = web.AppRunner(external_app)
    await runner.setup()
    site = web.TCPSite(runner, port=80)
    await site.start()
    logger.info("--------------External Started--------------")

async def start_internal_app():
    internal_app = web.Application()  # available to other containers at port 8080
    # Routes
    internal_app.add_routes(pages.api.routes)
    internal_app.add_routes(pages.alert_relay.routes)
    # Background Tasks
    internal_app.cleanup_ctx.append(database.init)
    internal_app.cleanup_ctx.append(tasks.create_background_task)

    runner = web.AppRunner(internal_app)
    await runner.setup()
    site = web.TCPSite(runner, port=81)
    await site.start()
    logger.info("--------------Internal Started--------------")


async def main():
    await start_external_app()
    await start_internal_app()
    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
