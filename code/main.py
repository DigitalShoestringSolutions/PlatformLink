from aiohttp import web, ClientSession
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

INTERNAL_ONLY = ["alert"]
INTERNAL_API_URL = "http://localhost:81"

async def proxy_to_internal(request):
    path = request.match_info["path"]
    # /alert
    path_segments = path.split("/")
    if path_segments[0] not in INTERNAL_ONLY:
        logger.critical(path)
        internal_url = f"{INTERNAL_API_URL}{request.rel_url}"
        async with ClientSession() as session:
            try:
                # Replicate the original request to the internal API
                # This handles the HTTP method (GET, POST, etc.), headers, and body
                async with session.request(
                    method=request.method,
                    url=internal_url,
                    headers=request.headers,
                    data=request.content, # Pass the incoming request body
                ) as resp:
                    # Read the entire response body into memory
                    body = await resp.read()

                    # Create a simple Response object with the full body
                    return web.Response(
                        status=resp.status,
                        reason=resp.reason,
                        headers=resp.headers,
                        body=body
                    )
            except Exception as e:
                # Handle any errors during the proxy call
                return web.Response(status=500, text=f"Proxy error: {e}")
    else:
        raise web.HTTPNotFound()


async def start_external_app():
    external_app = web.Application()  # available externally on port 80
    # Routes
    external_app.router.add_route("*", "/{path:.*}", proxy_to_internal)
    # Run
    runner = web.AppRunner(external_app)
    await runner.setup()
    site = web.TCPSite(runner, port=80)
    await site.start()
    logger.info("--------------External Started--------------")


async def start_internal_app():
    internal_app = web.Application()  # available to other containers at port 8080
    # set up templating
    aiohttp_jinja2.setup(internal_app, loader=jinja2.FileSystemLoader("./templates"))
    # Routes
    internal_app.add_routes(pages.api.routes)
    internal_app.add_routes(pages.ui.routes)
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
