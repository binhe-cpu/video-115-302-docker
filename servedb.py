#!/usr/bin/env python3
# encoding: utf-8

from __future__ import annotations

__author__ = "ChenyangGao <https://chenyanggao.github.io>"
__version__ = (0, 0, 0)
__all__ = ["make_application"]
__doc__ = """\
115 数据库 WebDAV 服务，请先用 updatedb.py 采集数据
"""
__requirements__ = ["flask", "Flask-Compress", "p115client", "path_predicate", "pyyaml", "urllib3", "urllib3_request", "werkzeug", "wsgidav"]

if __name__ == "__main__":
    from argparse import ArgumentParser, RawTextHelpFormatter

    parser = ArgumentParser(formatter_class=RawTextHelpFormatter, description=__doc__)
    parser.add_argument("dbfile", help="数据库路径")
    parser.add_argument("-c", "--config-path", help="""webdav 配置文件路径，采用 yaml 格式，如需样板文件，请阅读：

    https://wsgidav.readthedocs.io/en/latest/user_guide_configure.html#sample-wsgidav-yaml

""")
    parser.add_argument("-cp", "--cookies-path", default="", help="cookies 文件保存路径，默认是此脚本同一目录下的 115-cookies.txt")
    parser.add_argument("-p1", "--predicate", help="断言，当断言的结果为 True 时，文件或目录会被显示")
    parser.add_argument(
        "-t1", "--predicate-type", default="ignore", 
        choices=("ignore", "ignore-file", "expr", "lambda", "stmt", "module", "file", "re"), 
        help="""断言类型，默认值为 'ignore'
    - ignore       （默认值）gitignore 配置文本（有多个时用空格隔开），在文件路径上执行模式匹配，匹配成功则断言为 False
                   NOTE: https://git-scm.com/docs/gitignore#_pattern_format
    - ignore-file  接受一个文件路径，包含 gitignore 的配置文本（一行一个），在文件路径上执行模式匹配，匹配成功则断言为 False
                   NOTE: https://git-scm.com/docs/gitignore#_pattern_format
    - expr         表达式，会注入一个名为 path 的 p115.P115Path 对象
    - lambda       lambda 函数，接受一个 p115.P115Path 对象作为参数
    - stmt         语句，当且仅当不抛出异常，则视为 True，会注入一个名为 path 的 p115.P115Path 对象
    - module       模块，运行后需要在它的全局命名空间中生成一个 check 或 predicate 函数用于断言，接受一个 p115.P115Path 对象作为参数
    - file         文件路径，运行后需要在它的全局命名空间中生成一个 check 或 predicate 函数用于断言，接受一个 p115.P115Path 对象作为参数
    - re           正则表达式，模式匹配，如果文件的名字匹配此模式，则断言为 True
""")
    parser.add_argument("-p2", "--strm-predicate", help="strm 断言（优先级高于 -p1/--predicate），当断言的结果为 True 时，文件会被显示为带有 .strm 后缀的文本文件，打开后是链接")
    parser.add_argument(
        "-t2", "--strm-predicate-type", default="filter", 
        choices=("filter", "filter-file", "expr", "lambda", "stmt", "module", "file", "re"), 
        help="""断言类型，默认值为 'filter'
    - filter       （默认值）gitignore 配置文本（有多个时用空格隔开），在文件路径上执行模式匹配，匹配成功则断言为 True
                   请参考：https://git-scm.com/docs/gitignore#_pattern_format
    - filter-file  接受一个文件路径，包含 gitignore 的配置文本（一行一个），在文件路径上执行模式匹配，匹配成功则断言为 True
                   请参考：https://git-scm.com/docs/gitignore#_pattern_format
    - expr         表达式，会注入一个名为 path 的 p115.P115Path 对象
    - lambda       lambda 函数，接受一个 p115.P115Path 对象作为参数
    - stmt         语句，当且仅当不抛出异常，则视为 True，会注入一个名为 path 的 p115.P115Path 对象
    - module       模块，运行后需要在它的全局命名空间中生成一个 check 或 predicate 函数用于断言，接受一个 p115.P115Path 对象作为参数
    - file         文件路径，运行后需要在它的全局命名空间中生成一个 check 或 predicate 函数用于断言，接受一个 p115.P115Path 对象作为参数
    - re           正则表达式，模式匹配，如果文件的名字匹配此模式，则断言为 True
""")
    parser.add_argument("-fs", "--fast-strm", action="store_true", help="""快速实现 媒体筛选 和 虚拟 strm，此命令优先级较高，相当于命令行指定

    --strm-predicate-type expr \\
    --strm-predicate '(
        path.media_type.startswith(("video/", "audio/")) and
        path.suffix.lower() != ".ass"
    )' \\
    --predicate-type expr \\
    --predicate '(
        path.is_dir() or
        path.media_type.startswith("image/") or
        path.suffix.lower() in (".nfo", ".ass", ".ssa", ".srt", ".idx", ".sub", ".txt", ".vtt", ".smi")
    )'
""")
    parser.add_argument("-H", "--host", default="0.0.0.0", help="ip 或 hostname，默认值：'0.0.0.0'")
    parser.add_argument("-P", "--port", default=9000, type=int, help="端口号，默认值：9000")
    parser.add_argument("-d", "--debug", action="store_true", help="启用 debug 模式，当文件变动时自动重启 + 输出详细的错误信息")
    parser.add_argument("-v", "--version", action="store_true", help="输出版本号")

    args = parser.parse_args()
    if args.version:
        print(".".join(map(str, __version__)))
        raise SystemExit(0)

try:
    from flask import redirect, request, Flask
    from flask_compress import Compress
    from path_predicate import MappingPath, make_predicate
    from p115client import P115Client
    from urllib3.poolmanager import PoolManager
    from urllib3_request import request as urllib3_request
    from werkzeug.middleware.dispatcher import DispatcherMiddleware
    from wsgidav.wsgidav_app import WsgiDAVApp
    from wsgidav.dav_error import DAVError
    from wsgidav.dav_provider import DAVCollection, DAVNonCollection, DAVProvider
    from wsgidav.server.server_cli import SUPPORTED_SERVERS
    from yaml import load, Loader
except ImportError:
    from sys import executable
    from subprocess import run
    run([executable, "-m", "pip", "install", "-U", *__requirements__], check=True)
    from flask import redirect, request, Flask
    from flask_compress import Compress # type: ignore
    from path_predicate import MappingPath, make_predicate
    from p115client import P115Client
    from urllib3.poolmanager import PoolManager
    from urllib3_request import request as urllib3_request
    from werkzeug.middleware.dispatcher import DispatcherMiddleware
    from wsgidav.wsgidav_app import WsgiDAVApp # type: ignore
    from wsgidav.dav_error import DAVError # type: ignore
    from wsgidav.dav_provider import DAVCollection, DAVNonCollection, DAVProvider # type: ignore
    from wsgidav.server.server_cli import SUPPORTED_SERVERS # type: ignore
    from yaml import load, Loader

from collections.abc import Callable, Mapping, ItemsView
from functools import cached_property, partial
from io import BytesIO
from pathlib import Path
from posixpath import dirname, splitext
from sqlite3 import connect, Connection, OperationalError
from threading import Lock
from typing import Literal


transtab = {c: f"%{c:02x}" for c in b"#%/?"}
translate = str.translate


class LRUDict(dict):

    def __init__(self, /, maxsize: int = 0):
        self.maxsize = maxsize

    def __setitem__(self, key, value, /):
        self.pop(key, None)
        super().__setitem__(key, value)
        self.clean()

    def clean(self, /):
        if (maxsize := self.maxsize) > 0:
            pop = self.pop
            while len(self) > maxsize:
                try:
                    pop(next(iter(self)), None)
                except RuntimeError:
                    pass

    def setdefault(self, key, default=None, /):
        value = super().setdefault(key, default)
        self.clean()
        return value

    def update(self, iterable=None, /, **pairs):
        pop = self.pop
        setitem = self.__setitem__
        if iterable:
            if isinstance(iterable, Mapping):
                try:
                    iterable = iterable.items()
                except (AttributeError, TypeError):
                    iterable = ItemsView(iterable)
            for key, val in iterable:
                pop(key, None)
                setitem(key, val)
        if pairs:
            for key, val in pairs.items():
                pop(key, None)
                setitem(key, val)
        self.clean()


def make_application(
    dbfile: str | Path, 
    config_path: str | Path = "", 
    cookies_path: str | Path = "", 
    predicate: None | Callable = None, 
    strm_predicate: None | Callable = None, 
) -> DispatcherMiddleware:
    if config_path:
        config = load(open(config_path, encoding="utf-8"), Loader=Loader)
    else:
        config = {"simple_dc": {"user_mapping": {"*": True}}}
    if cookies_path:
        cookies_path = Path(cookies_path)
    else:
        cookies_path = Path(__file__).parent / "115-cookies.txt"
    client = P115Client(cookies_path, app="harmony", check_for_relogin=True)
    urlopen = partial(urllib3_request, pool=PoolManager(num_pools=50))

    CON: Connection
    FIELDS = ("id", "name", "path", "ctime", "mtime", "size", "pickcode", "is_dir")
    ROOT = {"id": 0, "name": "", "path": "/", "ctime": 0, "mtime": 0, "size": 0, "pickcode": "", "is_dir": 1}
    STRM_CACHE: LRUDict = LRUDict(65536)
    WRITE_LOCK = Lock()

    class DavPathBase:

        def __getattr__(self, attr, /):
            try:
                return self.attr[attr]
            except KeyError as e:
                raise AttributeError(attr) from e

        @cached_property
        def creationdate(self, /) -> float:
            return self.ctime

        @cached_property
        def ctime(self, /) -> float:
            return self.attr["ctime"]

        @cached_property
        def mtime(self, /) -> float:
            return self.attr["mtime"]

        @cached_property
        def name(self, /) -> str:
            return self.attr["name"]

        def get_creation_date(self, /) -> float:
            return self.ctime

        def get_display_name(self, /) -> str:
            return self.name

        def get_etag(self, /) -> str:
            return "%s-%s-%s" % (
                self.attr["pickcode"], 
                self.mtime, 
                self.size, 
            )

        def get_last_modified(self, /) -> float:
            return self.mtime

        def is_link(self, /) -> bool:
            return False

        def support_etag(self, /) -> bool:
            return True

        def support_modified(self, /) -> bool:
            return True

    class FileResource(DavPathBase, DAVNonCollection):

        def __init__(
            self, 
            /, 
            path: str, 
            environ: dict, 
            attr: dict, 
            is_strm: bool = False, 
        ):
            super().__init__(path, environ)
            self.attr = attr
            self.is_strm = is_strm
            if is_strm:
                STRM_CACHE[path] = self

        @cached_property
        def origin(self, /) -> str:
            return f"{self.environ['wsgi.url_scheme']}://{self.environ['HTTP_HOST']}"

        @cached_property
        def size(self, /) -> int:
            if self.is_strm:
                return len(self.strm_data)
            return self.attr["size"]

        @cached_property
        def strm_data(self, /) -> bytes:
            attr = self.attr
            name = translate(attr["name"], transtab)
            return bytes(f"{self.origin}/{name}?pickcode={attr['pickcode']}", "utf-8")

        @cached_property
        def url(self, /) -> str:
            scheme = self.environ["wsgi.url_scheme"]
            host = self.environ["HTTP_HOST"]
            return f"{scheme}://{host}?pickcode={self.attr['pickcode']}"

        def get_content(self, /):
            if self.is_strm:
                return BytesIO(self.strm_data)
            fid = self.attr["id"]
            try:
                return CON.blobopen("data", "data", fid, readonly=True, name="file")
            except (OperationalError, SystemError):
                pass
            if self.attr["size"] >= 1024 * 64:
                raise DAVError(302, add_headers=[("Location", self.url)])
            CON.execute("""\
INSERT INTO file.data(id, data) VALUES(?, zeroblob(?)) 
ON CONFLICT(id) DO UPDATE SET data=excluded.data;""", (fid, self.attr["size"]))
            CON.commit()
            try:
                data = urlopen(self.url).read()
                with WRITE_LOCK:
                    with CON.blobopen("data", "data", fid, name="file") as fdst:
                        fdst.write(data)
                return CON.blobopen("data", "data", fid, readonly=True, name="file")
            except:
                CON.execute("DELETE FROM file WHERE id=?", (fid,))
                CON.commit()
                raise

        def get_content_length(self, /) -> int:
            return self.size

        def support_content_length(self, /) -> bool:
            return True

        def support_ranges(self, /) -> bool:
            return True

    class FolderResource(DavPathBase, DAVCollection):

        def __init__(
            self, 
            /, 
            path: str, 
            environ: dict, 
            attr: dict, 
        ):
            if not path.endswith("/"):
                path += "/"
            super().__init__(path, environ)
            self.attr = attr

        @cached_property
        def children(self, /) -> dict[str, FileResource | FolderResource]:
            sql = """\
SELECT id, name, path, ctime, mtime, size, pickcode, is_dir
FROM data
WHERE parent_id = :id AND name NOT IN ('', '.', '..') AND name NOT LIKE '%/%';
"""
            children: dict[str, FileResource | FolderResource] = {}
            environ = self.environ
            for r in CON.execute(sql, self.attr):
                attr = dict(zip(FIELDS, r))
                is_strm = False
                name = attr["name"]
                path = attr["path"]
                if not attr["is_dir"] and strm_predicate and strm_predicate(MappingPath(attr)):
                    name = splitext(name)[0] + ".strm"
                    path = splitext(path)[0] + ".strm"
                    is_strm = True
                elif predicate and not predicate(MappingPath(attr)):
                    continue
                if attr["is_dir"]:
                    children[name] = FolderResource(path, environ, attr)
                else:
                    children[name] = FileResource(path, environ, attr, is_strm=is_strm)
            return children

        def get_descendants(
            self, 
            /, 
            collections: bool = True, 
            resources: bool = True, 
            depth_first: bool = False, 
            depth: Literal["0", "1", "infinity"] = "infinity", 
            add_self: bool = False, 
        ) -> list[FileResource | FolderResource]:
            descendants: list[FileResource | FolderResource] = []
            push = descendants.append
            if collections and add_self:
                push(self)
            if depth == "0":
                return descendants
            elif depth == "1":
                for item in self.children.values():
                    if item.attr["is_dir"]:
                        if collections:
                            push(item)
                    elif resources:
                        push(item)
                return descendants
            sql = """\
SELECT id, name, path, ctime, mtime, size, pickcode, is_dir
FROM data
WHERE path LIKE ? || '%' AND name NOT IN ('', '.', '..') AND name NOT LIKE '%/%'"""
            if collections and resources:
                pass
            elif collections:
                sql += " AND is_dir"
            elif resources:
                sql += " AND NOT is_dir"
            else:
                return descendants
            if depth_first:
                sql += "\nORDER BY path"
            else:
                sql += "\nORDER BY dirname(path)"
            environ = self.environ
            for r in CON.execute(sql, (self.path,)):
                attr = dict(zip(FIELDS, r))
                is_strm = False
                path = attr["path"]
                if not attr["is_dir"] and strm_predicate and strm_predicate(MappingPath(attr)):
                    path = splitext(path)[0] + ".strm"
                    is_strm = True
                elif predicate and not predicate(MappingPath(attr)):
                    continue
                if attr["is_dir"]:
                    push(FolderResource(path, environ, attr))
                else:
                    push(FileResource(path, environ, attr, is_strm=is_strm))
            return descendants

        def get_member(self, /, name: str) -> FileResource | FolderResource:
            if res := self.children.get(name):
                return res
            raise DAVError(404, self.path + name)

        def get_member_list(self, /) -> list[FileResource | FolderResource]:
            return list(self.children.values())

        def get_member_names(self, /) -> list[str]:
            return list(self.children.keys())

        def get_property_value(self, /, name: str):
            if name == "{DAV:}getcontentlength":
                return 0
            elif name == "{DAV:}iscollection":
                return True
            return super().get_property_value(name)

    class ServeDBProvider(DAVProvider):

        def __init__(self, /, dbfile: str | Path):
            nonlocal CON
            CON = connect(dbfile, check_same_thread=False)
            CON.create_function("dirname", 1, dirname)
            dbfile = CON.execute("SELECT file FROM pragma_database_list() WHERE name='main';").fetchone()[0]
            head, suffix = splitext(dbfile)
            CON.execute("ATTACH DATABASE ? AS file;", (f"{head}-file{suffix}",))
            CON.execute("""\
CREATE TABLE IF NOT EXISTS file.data (
    id INTEGER NOT NULL PRIMARY KEY,
    data BLOB,
    temp_path TEXT
);""")

        def __del__(self, /):
            try:
                CON.close()
            except AttributeError:
                pass

        def get_resource_inst(
            self, 
            /, 
            path: str, 
            environ: dict, 
        ) -> FolderResource | FileResource:
            if strm := STRM_CACHE.get(path):
                return strm
            if path in ("/", ""):
                return FolderResource("/", environ, ROOT)
            path = path.removesuffix("/")
            sql = "SELECT id, name, path, ctime, mtime, size, pickcode, is_dir FROM data WHERE path = ? LIMIT 1"
            cur = CON.execute(sql, (path,))
            record = cur.fetchone()
            if not record:
                raise DAVError(404, path)
            attr = dict(zip(FIELDS, record))
            is_strm = False
            if not attr["is_dir"] and strm_predicate and strm_predicate(MappingPath(attr)):
                is_strm = True
                path = splitext(path)[0] + ".strm"
            elif predicate and not predicate(MappingPath(attr)):
                raise DAVError(404, path)
            if attr["is_dir"]:
                return FolderResource(path, environ, attr)
            else:
                return FileResource(path, environ, attr, is_strm=is_strm)

        def is_readonly(self, /) -> bool:
            return True

    flask_app = Flask(__name__)
    Compress(flask_app)

    @flask_app.route("/", methods=["GET", "HEAD"])
    def index():
        if pickcode := request.args.get("pickcode"):
            resp = client.download_url_app(
                pickcode, 
                headers={"User-Agent": request.headers.get("User-Agent") or ""}, 
                request=urlopen, 
            )
            return redirect(next(iter(resp["data"].values()))["url"]["url"])
        else:
            return redirect("/d")

    @flask_app.route("/", methods=[
        "POST", "PUT", "DELETE", "CONNECT", "OPTIONS", 
        "TRACE", "PATCH", "MKCOL", "COPY", "MOVE", "PROPFIND", 
        "PROPPATCH", "LOCK", "UNLOCK", "REPORT", "ACL", 
    ])
    def redirect_to_dav():
        return redirect("/d")

    @flask_app.route("/<path:path>", methods=["GET", "HEAD"])
    def resolve_path(path: str):
        if pickcode := request.args.get("pickcode"):
            return redirect(f"/?pickcode={pickcode}")
        else:
            return redirect(f"/d/{path}")

    @flask_app.route("/<path:path>", methods=[
        "POST", "PUT", "DELETE", "CONNECT", "OPTIONS", 
        "TRACE", "PATCH", "MKCOL", "COPY", "MOVE", "PROPFIND", 
        "PROPPATCH", "LOCK", "UNLOCK", "REPORT", "ACL", 
    ])
    def resolve_path_to_dav(path: str):
        return redirect(f"/d/{path}")

    config.update({
        "host": "0.0.0.0", 
        "host": 0, 
        "mount_path": "/d", 
        "provider_mapping": {"/": ServeDBProvider(dbfile)}, 
    })
    wsgidav_app = WsgiDAVApp(config)
    return DispatcherMiddleware(flask_app, {"/d": wsgidav_app})


if __name__ == "__main__":
    import re
    from werkzeug.serving import run_simple

    if args.fast_strm:
        predicate = make_predicate("""(
    path.is_dir() or
    path.media_type.startswith("image/") or
    path.suffix.lower() in (".nfo", ".ass", ".ssa", ".srt", ".idx", ".sub", ".txt", ".vtt", ".smi")
)""", type="expr")
    elif predicate := args.predicate or None:
        predicate = make_predicate(predicate, {"re": re}, type=args.predicate_type)
    if args.fast_strm:
        strm_predicate = make_predicate("""(
    path.media_type.startswith(("video/", "audio/")) and
    path.suffix.lower() != ".ass"
)""", type="expr")
    elif strm_predicate := args.strm_predicate or None:
        strm_predicate = make_predicate(strm_predicate, {"re": re}, type=args.strm_predicate_type)

    app = make_application(
        args.dbfile, 
        config_path=args.config_path, 
        cookies_path=args.cookies_path, 
        predicate=predicate, 
        strm_predicate=strm_predicate, 
    )
    run_simple(
        hostname=args.host, 
        port=args.port, 
        application=app, 
        use_reloader=args.debug, 
        use_debugger=args.debug, 
        use_evalex=args.debug, 
        threaded=True, 
    )
