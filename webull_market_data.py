"""
Webull OpenAPI - Tick data + Level 2 (order book depth) example,
extended with Nasdaq-100 index/futures data.

Requires:
    pip install webull-openapi-python-sdk

Credentials:
    Set these as environment variables (do NOT hardcode them in the script):
        WEBULL_APP_KEY      -> your App Key   (what you called "app id")
        WEBULL_APP_SECRET   -> your App Secret
        WEBULL_REGION       -> "us" (or "au", "hk" etc. depending on your account)
        WEBULL_ENV          -> "prod" or "sandbox"

    On Linux/macOS:
        export WEBULL_APP_KEY="your_app_key"
        export WEBULL_APP_SECRET="your_app_secret"
        export WEBULL_REGION="us"
        export WEBULL_ENV="prod"

    On Windows (PowerShell):
        $env:WEBULL_APP_KEY="your_app_key"
        $env:WEBULL_APP_SECRET="your_app_secret"
        $env:WEBULL_REGION="us"
        $env:WEBULL_ENV="prod"

Important - Nasdaq "index" data:
    Webull's OpenAPI has no raw index category (there is no way to pull the
    Nasdaq Composite / ^IXIC tick-by-tick). The Category enum only covers:
    US_STOCK, US_OPTION, HK_STOCK, US_ETF, HK_ETF, CN_STOCK, US_CRYPTO,
    US_FUTURES, US_EVENT, HK_FUTURES. The two realistic ways to track "the
    Nasdaq" through this API are:
      1. QQQ (Invesco Nasdaq-100 ETF) as a liquid index proxy -> Category.US_ETF
      2. Nasdaq-100 futures (NQ / Micro NQ) -> Category.US_FUTURES

Important - futures data:
    - Level 2 / order-book-depth data for STOCKS requires an active
      "OpenAPI Advanced Quotes" subscription (Advanced Quotes Center ->
      OpenAPI Advanced Quotes on the developer portal).
    - FUTURES market data requires its own, separate paid market-data
      subscription. As of Webull's current docs this futures subscription
      module is still "under active development" on their end, so the
      futures calls below may return 403 even if your stock quote
      subscription is active - that's expected, not a bug in this script.
    - Futures symbols use Webull's own convention: "<root>main" for the
      continuously-rolled front-month contract (e.g. "NQmain", "MNQmain"),
      or a dated contract code (e.g. "NQZ6" for the Dec 2026 contract).
      Use Instrument.get_futures_instrument(code="NQ", ...) if you want to
      list all live contracts for a product instead of guessing the code.
"""

import os
import sys
import threading
import time

try:
    from dotenv import load_dotenv
    load_dotenv()  # reads a local ".env" file, if present, into os.environ
except ImportError:
    pass  # dotenv is optional; env vars can also be set directly in the shell

from webull.core.client import ApiClient
from webull.data.common.category import Category
from webull.data.common.subscribe_type import SubscribeType
from webull.data.data_client import DataClient
from webull.data.data_streaming_client import DataStreamingClient

# ---- 1. Load credentials from environment -------------------------------

APP_KEY = os.environ.get("WEBULL_APP_KEY")
APP_SECRET = os.environ.get("WEBULL_APP_SECRET")
REGION = os.environ.get("WEBULL_REGION", "us")
ENV = os.environ.get("WEBULL_ENV", "prod")  # "prod" or "sandbox"

if not APP_KEY or not APP_SECRET:
    sys.exit(
        "Missing credentials. Set WEBULL_APP_KEY and WEBULL_APP_SECRET "
        "as environment variables before running this script."
    )

# Endpoints differ between production and the sandbox/test environment.
HTTP_ENDPOINTS = {
    "prod": "api.webull.com",
    "sandbox": "api.sandbox.webull.com",
}
MQTT_ENDPOINTS = {
    "prod": "data-api.webull.com",
    "sandbox": "data-api.sandbox.webull.com",
}

HTTP_HOST = HTTP_ENDPOINTS[ENV]
MQTT_HOST = MQTT_ENDPOINTS[ENV]

SYMBOLS = ["AAPL"]                 # regular stocks, change as needed
INDEX_PROXY_SYMBOLS = ["QQQ"]      # Nasdaq-100 ETF proxy (Category.US_ETF)
FUTURES_SYMBOLS = ["NQmain", "MNQmain"]  # E-mini / Micro E-mini Nasdaq-100 (Category.US_FUTURES)


# ---- 2a. On-demand HTTP call: order book depth (Level 2) for stocks -----

def fetch_order_book_depth():
    """
    Pulls a one-off snapshot of bid/ask depth via HTTP for regular stocks.
    Requires the OpenAPI Advanced Quotes subscription for Level 2.

    Note: the depth/quotes endpoint lives under `market_data.get_quotes`,
    not a separate `quote` client -- there is no `get_order_book` method
    in the SDK.
    """
    api_client = ApiClient(APP_KEY, APP_SECRET, REGION)
    api_client.add_endpoint(REGION, HTTP_HOST)

    data_client = DataClient(api_client)

    for symbol in SYMBOLS:
        try:
            res = data_client.market_data.get_quotes(
                symbol=symbol,
                category=Category.US_STOCK.name,
                depth=10,  # Level 2, 10 price levels (US stocks support up to 50)
            )
            if res.status_code == 200:
                print(f"[{symbol}] order book depth:")
                print(res.json())
            else:
                print(f"[{symbol}] depth request failed: HTTP {res.status_code} - {res.text}")
        except Exception as exc:
            print(f"[{symbol}] depth request failed: {exc}")


# ---- 2b. On-demand HTTP call: Nasdaq-100 proxy (QQQ) snapshot -----------

def fetch_nasdaq_index_proxy_snapshot():
    """
    Pulls a real-time snapshot for QQQ, used as a proxy for "the Nasdaq"
    since the OpenAPI has no dedicated index category.
    """
    api_client = ApiClient(APP_KEY, APP_SECRET, REGION)
    api_client.add_endpoint(REGION, HTTP_HOST)

    data_client = DataClient(api_client)

    for symbol in INDEX_PROXY_SYMBOLS:
        try:
            res = data_client.market_data.get_quotes(
                symbol=symbol,
                category=Category.US_ETF.name,
                depth=10,
            )
            if res.status_code == 200:
                print(f"[{symbol}] Nasdaq-100 proxy snapshot:")
                print(res.json())
            else:
                print(f"[{symbol}] proxy request failed: HTTP {res.status_code} - {res.text}")
        except Exception as exc:
            print(f"[{symbol}] proxy request failed: {exc}")


# ---- 2c. On-demand HTTP call: Nasdaq futures (NQ / MNQ) ------------------

def fetch_nasdaq_futures_data():
    """
    Pulls a real-time futures snapshot and Level-2 depth for the Nasdaq-100
    futures. Needs the separate futures market-data subscription; a 403
    here most likely means that subscription isn't active/available yet
    on your account, not a code problem.
    """
    api_client = ApiClient(APP_KEY, APP_SECRET, REGION)
    api_client.add_endpoint(REGION, HTTP_HOST)

    data_client = DataClient(api_client)

    try:
        res = data_client.futures_market_data.get_futures_snapshot(
            symbols=FUTURES_SYMBOLS,
            category=Category.US_FUTURES.name,
        )
        if res.status_code == 200:
            print("[NQ/MNQ] futures snapshot:")
            print(res.json())
        else:
            print(f"[NQ/MNQ] snapshot request failed: HTTP {res.status_code} - {res.text}")
    except Exception as exc:
        print(f"[NQ/MNQ] snapshot request failed: {exc}")

    for symbol in FUTURES_SYMBOLS:
        try:
            res = data_client.futures_market_data.get_futures_depth(
                symbol=symbol,
                category=Category.US_FUTURES.name,
                depth=10,
            )
            if res.status_code == 200:
                print(f"[{symbol}] futures order book depth:")
                print(res.json())
            else:
                print(f"[{symbol}] depth request failed: HTTP {res.status_code} - {res.text}")
        except Exception as exc:
            print(f"[{symbol}] depth request failed: {exc}")


# ---- 3. Real-time streaming: tick + quote + snapshot via MQTT -----------

def stream_realtime_data(duration_seconds=60):
    """
    Opens a live MQTT stream and prints tick / quote / snapshot events
    as they arrive, for `duration_seconds`. Subscribes to stocks, the
    Nasdaq-100 ETF proxy, and the Nasdaq-100 futures in parallel.

    Note: DataStreamingClient does not expose a plain `connect()` you call
    yourself -- the documented entry point is `connect_and_loop_forever()`,
    which blocks the calling thread until the connection ends. To cap the
    stream at `duration_seconds`, we schedule `disconnect()` on a background
    timer; calling disconnect() causes connect_and_loop_forever() to return.
    """
    session_id = f"session_{int(time.time())}"

    streaming_client = DataStreamingClient(
        APP_KEY,
        APP_SECRET,
        REGION,
        session_id,
        http_host=HTTP_HOST,
        mqtt_host=MQTT_HOST,
    )

    def on_connect(client, api_client, session_id):
        print("Connected, session:", session_id)

        # Regular stocks
        client.subscribe(
            SYMBOLS,
            Category.US_STOCK.name,
            [
                SubscribeType.TICK.name,
                SubscribeType.QUOTE.name,
                SubscribeType.SNAPSHOT.name,
            ],
        )

        # Nasdaq-100 proxy (QQQ)
        client.subscribe(
            INDEX_PROXY_SYMBOLS,
            Category.US_ETF.name,
            [
                SubscribeType.TICK.name,
                SubscribeType.QUOTE.name,
                SubscribeType.SNAPSHOT.name,
            ],
        )

        # Nasdaq-100 futures (NQ / MNQ) -- may fail to subscribe if the
        # futures market-data subscription isn't active on your account.
        client.subscribe(
            FUTURES_SYMBOLS,
            Category.US_FUTURES.name,
            [
                SubscribeType.TICK.name,
                SubscribeType.QUOTE.name,
                SubscribeType.SNAPSHOT.name,
            ],
        )

    def on_subscribe(client, api_client, session_id):
        print("Subscribed successfully.")

    def on_message(client, topic, quotes):
        print(f"[{topic}] {quotes}")

    streaming_client.on_connect_success = on_connect
    streaming_client.on_subscribe_success = on_subscribe
    streaming_client.on_quotes_message = on_message

    timer = threading.Timer(duration_seconds, streaming_client.disconnect)
    timer.daemon = True
    timer.start()

    print(f"Streaming for {duration_seconds}s... Ctrl+C to stop early.")
    try:
        streaming_client.connect_and_loop_forever()
    except KeyboardInterrupt:
        streaming_client.disconnect()
    finally:
        timer.cancel()
        print("Stream closed.")


if __name__ == "__main__":
    print("=== Order book depth (Level 2 snapshot) - stocks ===")
    fetch_order_book_depth()

    print("\n=== Nasdaq-100 proxy (QQQ) snapshot ===")
    fetch_nasdaq_index_proxy_snapshot()

    print("\n=== Nasdaq-100 futures (NQ / MNQ) snapshot + depth ===")
    fetch_nasdaq_futures_data()

    print("\n=== Real-time tick/quote/snapshot stream (stocks + QQQ + NQ/MNQ) ===")
    stream_realtime_data(duration_seconds=60)
