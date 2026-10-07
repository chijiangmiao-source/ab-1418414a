"""束线联锁监看服务入口。"""
from __future__ import annotations

import argparse
import os
import signal

from .app import build_server


def main() -> None:
    parser = argparse.ArgumentParser(description="束线联锁监看服务")
    parser.add_argument("--host", default=os.environ.get("HOST", "0.0.0.0"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", "8080")))
    parser.add_argument("--db", default=os.environ.get("DB_PATH", "/data/interlock.db"))
    args = parser.parse_args()

    if args.db != ":memory:":
        os.makedirs(os.path.dirname(os.path.abspath(args.db)), exist_ok=True)
    httpd = build_server(args.host, args.port, args.db)
    print(f"interlock monitor listening on http://{args.host}:{args.port} (db={args.db})")

    def _shutdown(*_):
        httpd.shutdown()

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)
    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
