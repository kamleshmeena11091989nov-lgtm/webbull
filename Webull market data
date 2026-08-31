"""
Webull OpenAPI - Tick data + Level 2 (order book depth) example.

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

Important:
    - Level 2 / order-book-depth data requires an active "OpenAPI Advanced Quotes"
      subscription purchased separately at the Webull developer portal
      (Advanced Quotes Center -> OpenAPI Advanced Quotes). A subscription bought
      in the mobile app or desktop platform does NOT apply to the API.
    - Without that subscription, depth/tick calls below will return a 403 error.
"""

import os
import sys
import time

try:
    from dotenv import load_dotenv
    load_dotenv()  # reads a local ".env" file, if present, into os.environ
except ImportError:
    pass  # dotenv is optional; env vars can also be set directly in the shell

from webull.core.client import ApiClient
from webull.data.common.category import Category
from webull.data.common.subscribe_type import SubscribeType
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
    "prod": "mqtt.webull.com",
    "sandbox": "mqtt.sandbox.webull.com",
}

HTTP_HOST = HTTP_ENDPOINTS[ENV]
MQTT_HOST = MQTT_ENDPOINTS[ENV]

SYMBOLS = ["AAPL"]  # change to whatever tickers you want


# ---- 2. On-demand HTTP call: order book depth (Level 2) -----------------

def fetch_order_book_depth():
    """
    Pulls a one-off snapshot of bid/ask depth via HTTP.
    Requires the OpenAPI Advanced Quotes subscription for Level 2.
    """
    api_client = ApiClient(APP_KEY, APP_SECRET, REGION)
    api_client.add_endpoint(REGION, HTTP_HOST)

    # The exact market-data client class/method names can change between SDK
    # versions -- check `webull.data` in your installed package if this
    # import path doesn't match, e.g.:
    #   from webull.data.data_client import DataClient
    from webull.data.data_client import DataClient

    data_client = DataClient(api_client)

    for symbol in SYMBOLS:
        try:
            resp = data_client.quote.get_order_book(
                symbol=symbol,
                category=Category.US_STOCK.name,
                depth=10,  # number of price levels to request
            )
            print(f"[{symbol}] order book depth:")
            print(resp.json() if hasattr(resp, "json") else resp)
        except Exception as exc:
            print(f"[{symbol}] depth request failed: {exc}")


# ---- 3. Real-time streaming: tick + quote + snapshot via MQTT -----------

def stream_realtime_data(duration_seconds=60):
    """
    Opens a live MQTT stream and prints tick / quote / snapshot events
    as they arrive, for `duration_seconds`.
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
        client.subscribe(
            SYMBOLS,
            Category.US_STOCK.name,
            [
                SubscribeType.TICK.name,      # raw tick-by-tick trades
                SubscribeType.QUOTE.name,     # best bid/ask (level 1)
                SubscribeType.SNAPSHOT.name,  # OHLC + volume snapshot
            ],
        )

    def on_subscribe(client, api_client, session_id):
        print("Subscribed successfully.")

    def on_message(client, topic, quotes):
        print(f"[{topic}] {quotes}")

    streaming_client.on_connect_success = on_connect
    streaming_client.on_subscribe_success = on_subscribe
    streaming_client.on_quotes_message = on_message

    streaming_client.connect()

    print(f"Streaming for {duration_seconds}s... Ctrl+C to stop early.")
    try:
        time.sleep(duration_seconds)
    except KeyboardInterrupt:
        pass
    finally:
        streaming_client.loop_stop()
        print("Stream closed.")


if __name__ == "__main__":
    print("=== Order book depth (Level 2 snapshot) ===")
    fetch_order_book_depth()

    print("\n=== Real-time tick/quote/snapshot stream ===")
    stream_realtime_data(duration_seconds=60)
