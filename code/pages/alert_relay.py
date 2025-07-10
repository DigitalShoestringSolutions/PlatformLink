from aiohttp import web
import aiohttp
import logging
import datetime
import os
from upload_stats import id_token_key

routes = web.RouteTableDef()

logger = logging.getLogger("platform_link.alerts")
DEBUG_MODE = os.getenv("DEBUG", "").upper() == "TRUE"

@routes.get("/test")
async def test(request):
    return web.json_response({})

@routes.post("/alert/{tag}")
async def increment_stat(request):
    tag = request.match_info["tag"]
    if request.can_read_body and request.content_type == "application/json":
        body = await request.json()
    else:
        body = {}

    id_token = request.app[id_token_key]

    if id_token is None:    # not linked to platform
        return web.json_response({"reason":"not_linked"},status=404)

    headers = {"Authorization": f"Token {id_token}"}
    async with aiohttp.ClientSession(headers=headers) as session:
        async with session.post(
            f"https://platform.digitalshoestring.net/api/alerts/email/send/{tag}",
            json=body,
            ssl=not DEBUG_MODE,
        ) as resp:
            logger.info(f"{tag} - {resp.status}")
            try:
                resp_body = await resp.json()
                return web.json_response(resp_body, status=resp.status)
            except:
                return web.json_response({}, status=resp.status)
