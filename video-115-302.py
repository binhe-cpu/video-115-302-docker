#!/usr/bin/env python3
# encoding: utf-8

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 2)
__doc__ = """115 302 服务（仅针对视频）

请在脚本同一目录下，创建一个 115-cookies.txt 文件，并写入 cookies
如果没有，则会自动创建，并要求你扫码，默认自动绑定到 harmony 端（即 115 鸿蒙版）

此程序用于请求视频文件的直链，支持两种调用方式

1. 以视频的文件名（仅仅是文件名，而不是完整路径）获取直链

    http://localhost:8000/video.mp4

2. 以 pickcode 获取直链（这种方式可以获取任何文件的直链，不限于视频）

    http://localhost:8000?pickcode=xxxxx

程序启动过后，会周期性地拉取一些指定目录下的所有视频文件的 名字 和 pickcode，并保存到缓存中。
但如果文件名不在缓存中，则第 1 种调用方式会返回 404 响应。因此如果要用文件名来获取缓存，请先等缓存第一次构建完，再进行使用。
🚨 请确保那些指定目录下的视频文件的名字各不相同。"""
__requirements__ = ["blacksheep", "p115client"]

from argparse import ArgumentParser, RawTextHelpFormatter

parser = ArgumentParser(formatter_class=RawTextHelpFormatter, description=__doc__)
parser.add_argument("-c", "--cids", metavar="cid", default=["0"], nargs="*", help="待拉取的目录 id，可以传多个，如果不传，默认是根目录")
parser.add_argument("-i", "--interval", default=30, type=float, help="一批拉取完成后，等待的间隔时间，默认是 30 秒钟")
parser.add_argument("-f", "--store-file", help="缓存到文件的路径，如果不提供，则在内存中（程序关闭后销毁）")
parser.add_argument("-H", "--host", default="0.0.0.0", help="ip 或 hostname，默认值：'0.0.0.0'")
parser.add_argument("-p", "--port", default=8000, type=int, help="端口号，默认值：8000")
parser.add_argument("-v", "--version", action="store_true", help="输出版本号")

args = parser.parse_args()
if args.version:
    print(".".join(map(str, __version__)))
    raise SystemExit(0)

try:
    from p115client import P115Client
    from blacksheep import Application, Request, json, route, redirect, text
    from blacksheep.server.remotes.forwarding import ForwardedHeadersMiddleware
except ImportError:
    from sys import executable
    from subprocess import run
    run([executable, "-m", "pip", "install", "-U", *__requirements__], check=True)
    from p115client import P115Client
    from blacksheep import Application, Request, json, route, redirect, text
    from blacksheep.server.remotes.forwarding import ForwardedHeadersMiddleware

import logging

from asyncio import create_task, sleep
from collections.abc import Collection, MutableMapping
from pathlib import Path
from time import time

cids = args.cids
interval = args.interval

app = Application()

logger = getattr(app, "logger")
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("[\x1b[1m%(asctime)s\x1b[0m] (\x1b[1;36m%(levelname)s\x1b[0m) \x1b[5;31m➜\x1b[0m %(message)s"))
logger.addHandler(handler)

# 用来保存【视频名称】对应的【pickcode】
if args.store_file:
    from shelve import open as open_shelve
    NAME_TO_PICKCODE: MutableMapping[str, str] = open_shelve(args.store_file, )
else:
    NAME_TO_PICKCODE = {}
# 用来保存【目录 id】对应的【目录里面最近一条视频文件的更新时间】
MAX_MTIME_MAP: dict[str, str] = {}


async def load_videos(client: P115Client, cid: int | str = 0, /) -> int:
    cid = str(cid)
    last_max_mtime = MAX_MTIME_MAP.get(cid, "0")
    page_size = 10_000 if last_max_mtime == "0" else 32
    payload = {
        "asc": 0, "cid": cid, "count_folders": 0, "cur": 0, "limit": page_size, 
        "o": "user_utime", "offset": 0, "show_dir": 0, "type": 4, 
    }
    resp = await client.fs_files(payload, async_=True)
    if (not resp["state"] or 
        cid != "0" and resp["path"][-1]["cid"] != cid or 
        resp["count"] == 0):
        return 0
    this_max_mtime = resp["data"][0]["te"]
    if last_max_mtime >= this_max_mtime:
        return 0
    count = 0
    payload["limit"] = 10_000
    while True:
        for info in resp["data"]:
            mtime = info["te"]
            if mtime <= last_max_mtime:
                MAX_MTIME_MAP[cid] = this_max_mtime
                return count
            NAME_TO_PICKCODE[info["n"]] = info["pc"]
            count += 1
        payload["offset"] += len(resp["data"]) # type: ignore
        if payload["offset"] >= resp["count"]:
            break
        resp = await client.fs_files(payload, async_=True)
        if (not resp["state"] or 
            cid != "0" and resp["path"][-1]["cid"] != cid or 
            payload["offset"] != resp["offset"]):
            break
    MAX_MTIME_MAP[cid] = this_max_mtime
    return count


@app.on_middlewares_configuration
def configure_forwarded_headers(app):
    app.middlewares.insert(0, ForwardedHeadersMiddleware(accept_only_proxied_requests=False))


async def batch_load_videos(app: Application, /):
    client = app.services.resolve(P115Client)
    while True:
        start = time()
        for cid in cids:
            this_start = time()
            try:
                count = await load_videos(client, cid)
                logger.info(f"successfully loaded cid={cid}, {count} items, {time() - this_start:.6f} seconds")
            except:
                logger.exception(f"error occurred while loading cid={cid}")
        if interval >= 0:
            await sleep(interval)


@app.lifespan
async def register_p115client():
    client = P115Client(
        Path(__file__).parent / "115-cookies.txt", 
        app="harmony", 
        check_for_relogin=True, 
    )
    async with client.async_session:
        app.services.register(P115Client, instance=client)
        yield


@app.on_start
async def start_batch_load_videos(app: Application):
    create_task(batch_load_videos(app))


@route("/{path:name}", methods=["GET", "HEAD"])
async def index(
    request: Request, 
    client: P115Client, 
    name: str, 
    pickcode: str = "", 
):
    if not pickcode:
        try:
            pickcode = NAME_TO_PICKCODE[name]
        except KeyError:
            return text("not found", 404)
    user_agent = (request.get_first_header(b"User-agent") or b"").decode("utf-8")
    resp = client.download_url_app(pickcode, headers={"User-Agent": user_agent})
    if not resp["state"]:
        return json(resp, 404)
    return redirect(next(iter(resp["data"].values()))["url"]["url"])


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        app=app, 
        host=args.host, 
        port=args.port, 
        proxy_headers=True, 
        forwarded_allow_ips="*", 
    )

