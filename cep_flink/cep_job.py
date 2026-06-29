"""
CEP worker for recurring market patterns.

The project uses Apache Pulsar topics directly. Earlier versions of this file
tried to reach Pulsar through the Kafka protocol, but the default Pulsar image
does not ship with the Kafka protocol handler enabled. This worker therefore
uses the native Pulsar client while keeping the CEP state machine in this
service.
"""

import json
import os
import signal
import threading
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Any

import pulsar


PULSAR_URL = os.getenv("PULSAR_URL", "pulsar://pulsar:6650")
RAW_OPPORTUNITIES_TOPIC = os.getenv(
    "RAW_OPPORTUNITIES_TOPIC",
    "persistent://public/default/raw-opportunities",
)
CEP_SUBSCRIPTIONS_TOPIC = os.getenv(
    "CEP_SUBSCRIPTIONS_TOPIC",
    "persistent://public/default/cep-subscriptions",
)
CEP_ALERTS_TOPIC = os.getenv(
    "CEP_ALERTS_TOPIC",
    "persistent://public/default/alerts-cep",
)
GROUP_ID = os.getenv("GROUP_ID", "cep-worker")


class CEPState:
    def __init__(self) -> None:
        # RLock (reentrante) porque process_quote adquire o lock e pode chamar
        # _match_subscription, que não precisa re-adquirir, mas protege contra
        # a thread de configuração modificar _subscriptions ao mesmo tempo.
        self._lock = threading.RLock()
        self._subscriptions: dict[str, dict[str, Any]] = {}
        # deque com maxlen=200 garante consumo de memória fixo por ativo,
        # independente de quantas cotações chegarem ao longo do tempo.
        self._history: dict[str, deque[tuple[float, float]]] = defaultdict(
            lambda: deque(maxlen=200)
        )

    def upsert_subscription(self, payload: dict[str, Any]) -> None:
        subscription_id = payload.get("subscription_id")
        asset = payload.get("asset")
        pattern = payload.get("pattern")
        if not subscription_id or not asset or not pattern:
            print(f"[CEP] Ignoring malformed subscription: {payload}", flush=True)
            return

        with self._lock:
            self._subscriptions[subscription_id] = {
                "subscription_id": subscription_id,
                "client_id": payload.get("client_id", subscription_id),
                "asset": asset,
                "pattern": pattern,
                "count": int(payload.get("count") or 3),
                "pct": float(payload.get("pct") or 2.0),
                "window_secs": int(payload.get("window_secs") or 300),
            }
        print(f"[CEP] Registered filter {subscription_id}: {asset} {pattern}", flush=True)

    def remove_subscription(self, payload: dict[str, Any]) -> None:
        subscription_id = payload.get("subscription_id") or payload.get("client_id")
        if not subscription_id:
            print(f"[CEP] Ignoring malformed unsubscribe: {payload}", flush=True)
            return

        with self._lock:
            removed = self._subscriptions.pop(subscription_id, None)
        if removed:
            print(f"[CEP] Removed filter {subscription_id}", flush=True)

    def process_quote(self, payload: dict[str, Any]) -> list[dict[str, Any]]:
        asset = payload.get("asset")
        price = payload.get("price")
        if not asset or price is None:
            return []

        try:
            numeric_price = float(price)
        except (TypeError, ValueError):
            return []

        event_ts = parse_timestamp(payload.get("timestamp"))
        with self._lock:
            # Acrescenta a cotação ao histórico do ativo antes de checar padrões,
            # para que a cotação atual já faça parte da janela de comparação.
            self._history[asset].append((event_ts, numeric_price))
            # Copia subscriptions e histórico enquanto segura o lock para não
            # bloquear a thread de configuração durante o matching (que pode ser lento).
            subscriptions = [
                dict(subscription)
                for subscription in self._subscriptions.values()
                if subscription["asset"] == asset
            ]
            history = list(self._history[asset])

        alerts = []
        for subscription in subscriptions:
            alert = self._match_subscription(subscription, history, payload, numeric_price)
            if alert:
                alerts.append(alert)
        return alerts

    def _match_subscription(
        self,
        subscription: dict[str, Any],
        history: list[tuple[float, float]],
        quote: dict[str, Any],
        price: float,
    ) -> dict[str, Any] | None:
        # Despacha para a função de matching correta com base no padrão cadastrado.
        # Adicionar um novo padrão = implementar uma nova função match_* aqui.
        pattern = subscription["pattern"]
        if pattern == "consecutive_drops":
            return match_consecutive(subscription, history, quote, price, direction="down")
        if pattern == "consecutive_rises":
            return match_consecutive(subscription, history, quote, price, direction="up")
        if pattern == "pct_drop_window":
            return match_pct_drop(subscription, history, quote, price)
        return None


def parse_timestamp(value: Any) -> float:
    if not value:
        return datetime.now(timezone.utc).timestamp()
    try:
        normalized = str(value).replace("Z", "+00:00")
        return datetime.fromisoformat(normalized).timestamp()
    except ValueError:
        return datetime.now(timezone.utc).timestamp()


def base_alert(subscription: dict[str, Any], quote: dict[str, Any], price: float) -> dict[str, Any]:
    return {
        "asset": subscription["asset"],
        "client_id": subscription["client_id"],
        "subscription_id": subscription["subscription_id"],
        "last_price": price,
        "triggered_at": quote.get("timestamp"),
        "source": "cep-worker",
    }


def match_consecutive(
    subscription: dict[str, Any],
    history: list[tuple[float, float]],
    quote: dict[str, Any],
    price: float,
    *,
    direction: str,
) -> dict[str, Any] | None:
    count = subscription["count"]
    # Aguarda histórico suficiente antes de tentar detectar o padrão.
    if len(history) < count:
        return None

    # Pega somente as últimas `count` cotações e verifica se cada uma é
    # estritamente menor (down) ou maior (up) que a anterior.
    prices = [entry_price for _, entry_price in history[-count:]]
    if direction == "down":
        matched = all(prices[i] < prices[i - 1] for i in range(1, len(prices)))
        pattern = "consecutive_drops"
    else:
        matched = all(prices[i] > prices[i - 1] for i in range(1, len(prices)))
        pattern = "consecutive_rises"

    if not matched:
        return None

    return {
        **base_alert(subscription, quote, price),
        "pattern": pattern,
        "count": count,
        "prices": prices,
    }


def match_pct_drop(
    subscription: dict[str, Any],
    history: list[tuple[float, float]],
    quote: dict[str, Any],
    price: float,
) -> dict[str, Any] | None:
    window_secs = subscription["window_secs"]
    pct = subscription["pct"]
    now_ts = history[-1][0]
    # Define o limite inferior da janela de tempo e filtra o histórico.
    cutoff = now_ts - window_secs
    window_prices = [entry_price for ts, entry_price in history if ts >= cutoff]
    if len(window_prices) < 2 or window_prices[0] == 0:
        return None

    # Compara o primeiro preço da janela com o preço atual para calcular a queda.
    # window_prices[0] é o mais antigo dentro da janela (deque preserva ordem de inserção).
    drop_pct = ((window_prices[0] - price) / window_prices[0]) * 100
    if drop_pct < pct:
        return None

    return {
        **base_alert(subscription, quote, price),
        "pattern": "pct_drop_window",
        "window_secs": window_secs,
        "drop_pct": round(drop_pct, 4),
        "threshold_pct": pct,
        "first_price": window_prices[0],
    }


def decode_message(message: pulsar.Message) -> dict[str, Any]:
    return json.loads(message.data().decode("utf-8"))


def consume_subscription_changes(
    client: pulsar.Client,
    state: CEPState,
    stop_event: threading.Event,
) -> None:
    # Thread dedicada a receber mudanças de configuração (subscribe/unsubscribe).
    # Roda separada da thread de cotações para não bloquear o processamento de preços.
    consumer = client.subscribe(
        CEP_SUBSCRIPTIONS_TOPIC,
        subscription_name=f"{GROUP_ID}-subscription-config",
    )
    try:
        while not stop_event.is_set():
            try:
                message = consumer.receive(timeout_millis=1000)
            except pulsar.Timeout:
                continue

            try:
                payload = decode_message(message)
                action = payload.get("action")
                if action == "subscribe":
                    state.upsert_subscription(payload)
                elif action == "unsubscribe":
                    state.remove_subscription(payload)
                else:
                    print(f"[CEP] Ignoring unknown action: {payload}", flush=True)
                consumer.acknowledge(message)
            except Exception as exc:
                print(f"[CEP] Failed to process config message: {exc}", flush=True)
                consumer.negative_acknowledge(message)
    finally:
        consumer.close()


def consume_quotes(
    client: pulsar.Client,
    state: CEPState,
    stop_event: threading.Event,
) -> None:
    consumer = client.subscribe(
        RAW_OPPORTUNITIES_TOPIC,
        subscription_name=f"{GROUP_ID}-raw-quotes",
    )
    producer = client.create_producer(CEP_ALERTS_TOPIC)
    try:
        while not stop_event.is_set():
            try:
                message = consumer.receive(timeout_millis=1000)
            except pulsar.Timeout:
                continue

            try:
                payload = decode_message(message)
                # process_quote retorna uma lista — pode haver múltiplos padrões
                # ativos para o mesmo ativo, cada um gerando seu próprio alerta.
                for alert in state.process_quote(payload):
                    producer.send(json.dumps(alert).encode("utf-8"))
                    print(f"[CEP] Alert emitted: {alert}", flush=True)
                consumer.acknowledge(message)
            except Exception as exc:
                print(f"[CEP] Failed to process raw quote: {exc}", flush=True)
                consumer.negative_acknowledge(message)
    finally:
        producer.close()
        consumer.close()


def main() -> None:
    print(f"[CEP] Starting worker | pulsar={PULSAR_URL}", flush=True)
    client = pulsar.Client(PULSAR_URL)
    state = CEPState()
    stop_event = threading.Event()

    def request_stop(*_: object) -> None:
        stop_event.set()

    # Graceful shutdown: SIGTERM (Docker stop) e SIGINT (Ctrl+C) sinalizam o
    # stop_event, que encerra os loops de consumo de forma limpa.
    signal.signal(signal.SIGTERM, request_stop)
    signal.signal(signal.SIGINT, request_stop)

    # Thread de configuração roda como daemon: se a thread principal morrer,
    # ela é encerrada automaticamente pelo runtime.
    config_thread = threading.Thread(
        target=consume_subscription_changes,
        args=(client, state, stop_event),
        daemon=True,
    )
    config_thread.start()

    try:
        consume_quotes(client, state, stop_event)
    finally:
        stop_event.set()
        config_thread.join(timeout=5)
        client.close()


if __name__ == "__main__":
    main()