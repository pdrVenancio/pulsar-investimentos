import asyncio
import json
import os
from datetime import datetime, timezone
from typing import Any

import pulsar
import yfinance as yf


PULSAR_URL = os.getenv("PULSAR_URL", "pulsar://localhost:6650")
DESIRED_ASSETS_TOPIC = "persistent://public/default/desired-assets"
RAW_OPPORTUNITIES_TOPIC = "persistent://public/default/raw-opportunities"
POLL_INTERVAL_SECONDS = 60


active_assets: dict[str, set[str]] = {}
polling_tasks: dict[str, asyncio.Task] = {}


async def fetch_last_price(asset: str) -> float:
    # Como o yFinance realiza operações bloqueantes, ele é executado em uma thread 
    # separada para não bloquear o loop de eventos do asyncio.
    def read_price() -> float:
        ticker = yf.Ticker(asset)
        price = ticker.fast_info["lastPrice"]
        return float(price)

    return await asyncio.to_thread(read_price)


async def poll_asset(asset: str, producer: pulsar.Producer) -> None:
    try:
        while True:
            try:
                price = await fetch_last_price(asset)
                # O componente de ingestão apenas recebe e publica os dados brutos 
                # do mercado. Ele não avalia regras como "maior ou igual" (gte) ou 
                # "menor ou igual" (lte). Essa lógica de comparação pertence à Pulsar 
                # Function.
                payload = {
                    "asset": asset,
                    "price": price,
                    "timestamp": datetime.now(timezone.utc)
                    .isoformat()
                    .replace("+00:00", "Z"),
                    "source": "yfinance-ingestor",
                }
                producer.send(json.dumps(payload).encode("utf-8"))
                print(f"Published raw quote: {payload}", flush=True)
            except Exception as exc:
                print(f"Failed to poll {asset}: {exc}", flush=True)

            await asyncio.sleep(POLL_INTERVAL_SECONDS)
    except asyncio.CancelledError:
        print(f"Stopped polling {asset}", flush=True)
        raise


def decode_message(message: pulsar.Message) -> dict[str, Any]:
    return json.loads(message.data().decode("utf-8"))


def ensure_polling(asset: str, producer: pulsar.Producer) -> None:
    task = polling_tasks.get(asset)
    if task is None or task.done():
        # Existe apenas uma tarefa de consulta (polling) para cada ativo, independentemente 
        # de quantos clientes estejam inscritos nesse ativo.
        polling_tasks[asset] = asyncio.create_task(poll_asset(asset, producer))
        print(f"Started polling {asset}", flush=True)


def stop_polling(asset: str) -> None:
    task = polling_tasks.pop(asset, None)
    if task is not None:
        task.cancel()


async def handle_asset_demand(payload: dict[str, Any], producer: pulsar.Producer) -> None:
    action = payload.get("action")
    asset = payload.get("asset")
    client_id = payload.get("client_id")

    if not asset or not client_id:
        print(f"Ignoring malformed demand message: {payload}", flush=True)
        return

    if action == "subscribe":
        subscribers = active_assets.setdefault(asset, set())
        subscribers.add(client_id)
        # Diversos clientes podem utilizar a mesma rotina de consulta periódica de um ativo.
        ensure_polling(asset, producer)
        return

    if action == "unsubscribe":
        subscribers = active_assets.get(asset)
        if subscribers is None:
            return

        subscribers.discard(client_id)
        if not subscribers:
            # A consulta periódica ao ativo é interrompida quando o último cliente deixa de estar inscrito nele.
            active_assets.pop(asset, None)
            stop_polling(asset)
        return

    print(f"Ignoring unknown demand action: {payload}", flush=True)


async def consume_asset_demands(
    consumer: pulsar.Consumer,
    producer: pulsar.Producer,
) -> None:
    while True:
        message = await asyncio.to_thread(consumer.receive)
        try:
            payload = decode_message(message)
            await handle_asset_demand(payload, producer)
            consumer.acknowledge(message)
        except Exception as exc:
            print(f"Failed to process demand message: {exc}", flush=True)
            consumer.negative_acknowledge(message)


async def main() -> None:
    client = pulsar.Client(PULSAR_URL)
    producer = client.create_producer(RAW_OPPORTUNITIES_TOPIC)
    consumer = client.subscribe(
        DESIRED_ASSETS_TOPIC,
        subscription_name="ingestor-desired-assets",
    )

    try:
        await consume_asset_demands(consumer, producer)
    finally:
        for task in polling_tasks.values():
            task.cancel()

        if polling_tasks:
            await asyncio.gather(*polling_tasks.values(), return_exceptions=True)

        consumer.close()
        producer.close()
        client.close()


if __name__ == "__main__":
    asyncio.run(main())
