import json
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

import httpx
import pulsar
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


PULSAR_URL = os.getenv("PULSAR_URL", "pulsar://localhost:6650")
PULSAR_ADMIN_URL = os.getenv("PULSAR_ADMIN_URL", "http://localhost:8080")

DESIRED_ASSETS_TOPIC = "persistent://public/default/desired-assets"
RAW_OPPORTUNITIES_TOPIC = "persistent://public/default/raw-opportunities"
CEP_SUBSCRIPTIONS_TOPIC = "persistent://public/default/cep-subscriptions"
CEP_ALERTS_TOPIC = "persistent://public/default/alerts-cep"
FUNCTION_LOG_TOPIC = "persistent://public/default/function-logs"
FUNCTION_TENANT = "public"
FUNCTION_NAMESPACE = "default"
FUNCTION_FILE = Path(
    os.getenv("FUNCTION_FILE", "/app/pulsar_function/alert_function.py")
)


class SubscriptionRequest(BaseModel):
    asset: str = Field(..., min_length=1)
    rule: Literal["gte", "lte"]
    value: float


class SubscriptionResponse(BaseModel):
    client_id: str
    subscription_id: str
    alert_topic: str


class SubscriptionState(BaseModel):
    client_id: str
    subscription_id: str
    asset: str
    rule: str
    value: float
    function_name: str
    alert_topic: str


class DebugRawQuoteRequest(BaseModel):
    asset: str = Field(..., min_length=1)
    price: float
    timestamp: str | None = None


class DebugRawQuoteResponse(BaseModel):
    raw_topic: str
    payload: dict


class CEPSubscriptionRequest(BaseModel):
    asset: str = Field(..., min_length=1)
    pattern: Literal["consecutive_drops", "consecutive_rises", "pct_drop_window"]
    count: int = Field(3, ge=2)
    pct: float = Field(2.0, gt=0)
    window_secs: int = Field(300, ge=1)


class CEPSubscriptionResponse(BaseModel):
    client_id: str
    subscription_id: str
    alert_topic: str


app = FastAPI(title="Pulsar Asset Alerts")
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv("CORS_ORIGINS", "*").split(","),
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)
subscriptions: dict[str, SubscriptionState] = {}
pulsar_client: pulsar.Client | None = None
desired_assets_producer: pulsar.Producer | None = None
raw_opportunities_producer: pulsar.Producer | None = None
cep_subscriptions_producer: pulsar.Producer | None = None


def function_endpoint(function_name: str) -> str:
    return (
        f"{PULSAR_ADMIN_URL}/admin/v3/functions/"
        f"{FUNCTION_TENANT}/{FUNCTION_NAMESPACE}/{function_name}"
    )


def get_producer() -> pulsar.Producer:
    if desired_assets_producer is None:
        raise RuntimeError("Pulsar producer is not initialized")
    return desired_assets_producer


def get_raw_producer() -> pulsar.Producer:
    if raw_opportunities_producer is None:
        raise RuntimeError("Pulsar raw quote producer is not initialized")
    return raw_opportunities_producer


def get_cep_producer() -> pulsar.Producer:
    if cep_subscriptions_producer is None:
        raise RuntimeError("Pulsar CEP producer is not initialized")
    return cep_subscriptions_producer


def publish_asset_demand(message: dict) -> None:
    get_producer().send(json.dumps(message).encode("utf-8"))


def publish_raw_quote(message: dict) -> None:
    get_raw_producer().send(json.dumps(message).encode("utf-8"))


def publish_cep_subscription(message: dict) -> None:
    get_cep_producer().send(json.dumps(message).encode("utf-8"))


async def create_alert_function(
    *,
    client_id: str,
    asset: str,
    rule: str,
    value: float,
    function_name: str,
    alert_topic: str,
) -> None:
    # The API deploys one Pulsar Function per client. That Function receives
    # every raw quote, but its userConfig tells it which asset/rule to evaluate.
    print(f"Validating Pulsar Function file: {FUNCTION_FILE}", flush=True)
    
    if not FUNCTION_FILE.exists():
        raise HTTPException(
            status_code=500,
            detail=f"Pulsar Function file not found: {FUNCTION_FILE}",
        )
    
    function_config = {
        "name": function_name,
        "inputs": [RAW_OPPORTUNITIES_TOPIC],
        "output": alert_topic,
        "logTopic": FUNCTION_LOG_TOPIC,
        "className": "alert_function.AlertFunction",
        "userConfig": {
            "asset": asset,
            "rule": rule,
            "value": value,
            "client_id": client_id,
        },
        "runtime": "PYTHON",
    }

    async with httpx.AsyncClient(timeout=60.0) as client:
        with FUNCTION_FILE.open("rb") as function_data:
            # Pulsar Admin expects both the config and the Python source as
            # multipart parts. This is the REST equivalent of deploying the
            # Function through the CLI.
            response = await client.post(
                function_endpoint(function_name),
                files={
                    "functionConfig": (
                        None,
                        json.dumps(function_config),
                        "application/json",
                    ),
                    "data": (
                        "alert_function.py",
                        function_data,
                        "application/octet-stream",
                    )
                },
            )

    if response.status_code not in (200, 204):
        raise HTTPException(
            status_code=502,
            detail={
                "message": "Failed to create Pulsar Function",
                "status_code": response.status_code,
                "response": response.text,
            },
        )


async def delete_alert_function(function_name: str) -> None:
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.delete(function_endpoint(function_name))

    if response.status_code not in (200, 204, 404):
        raise HTTPException(
            status_code=502,
            detail={
                "message": "Failed to delete Pulsar Function",
                "status_code": response.status_code,
                "response": response.text,
            },
        )


@app.on_event("startup")
def startup() -> None:
    global pulsar_client, desired_assets_producer, raw_opportunities_producer, cep_subscriptions_producer
    pulsar_client = pulsar.Client(PULSAR_URL)
    desired_assets_producer = pulsar_client.create_producer(DESIRED_ASSETS_TOPIC)
    raw_opportunities_producer = pulsar_client.create_producer(RAW_OPPORTUNITIES_TOPIC)
    cep_subscriptions_producer = pulsar_client.create_producer(CEP_SUBSCRIPTIONS_TOPIC)


@app.on_event("shutdown")
def shutdown() -> None:
    if desired_assets_producer is not None:
        desired_assets_producer.close()
    if raw_opportunities_producer is not None:
        raw_opportunities_producer.close()
    if cep_subscriptions_producer is not None:
        cep_subscriptions_producer.close()
    if pulsar_client is not None:
        pulsar_client.close()


@app.post("/subscriptions", response_model=SubscriptionResponse, status_code=201)
async def create_subscription(request: SubscriptionRequest) -> SubscriptionResponse:
    client_id = str(uuid.uuid4())
    subscription_id = str(uuid.uuid4())
    function_name = f"alert-{client_id}"
    alert_topic = f"persistent://public/default/alerts-{client_id}"

    await create_alert_function(
        client_id=client_id,
        asset=request.asset,
        rule=request.rule,
        value=request.value,
        function_name=function_name,
        alert_topic=alert_topic,
    )

    try:
        # This tells the ingestor which asset must be polled. Price filtering
        # stays inside the Pulsar Function, not here.
        publish_asset_demand(
            {
                "action": "subscribe",
                "asset": request.asset,
                "client_id": client_id,
            }
        )
    except Exception as exc:
        await delete_alert_function(function_name)
        raise HTTPException(
            status_code=502,
            detail=f"Failed to publish subscription demand: {exc}",
        ) from exc

    subscriptions[client_id] = SubscriptionState(
        client_id=client_id,
        subscription_id=subscription_id,
        asset=request.asset,
        rule=request.rule,
        value=request.value,
        function_name=function_name,
        alert_topic=alert_topic,
    )

    return SubscriptionResponse(
        client_id=client_id,
        subscription_id=subscription_id,
        alert_topic=alert_topic,
    )


@app.get("/subscriptions", response_model=list[SubscriptionState])
async def list_subscriptions() -> list[SubscriptionState]:
    return list(subscriptions.values())


@app.post("/cep-subscriptions", response_model=CEPSubscriptionResponse, status_code=201)
async def create_cep_subscription(
    request: CEPSubscriptionRequest,
) -> CEPSubscriptionResponse:
    client_id = str(uuid.uuid4())
    subscription_id = str(uuid.uuid4())
    asset = request.asset.upper()

    try:
        publish_cep_subscription(
            {
                "action": "subscribe",
                "client_id": client_id,
                "subscription_id": subscription_id,
                "asset": asset,
                "pattern": request.pattern,
                "count": request.count,
                "pct": request.pct,
                "window_secs": request.window_secs,
            }
        )
        publish_asset_demand(
            {
                "action": "subscribe",
                "asset": asset,
                "client_id": client_id,
            }
        )
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to publish CEP subscription: {exc}",
        ) from exc

    return CEPSubscriptionResponse(
        client_id=client_id,
        subscription_id=subscription_id,
        alert_topic=CEP_ALERTS_TOPIC,
    )


@app.delete("/subscriptions/{client_id}", status_code=204)
async def delete_subscription(client_id: str) -> None:
    state = subscriptions.get(client_id)
    if state is None:
        raise HTTPException(status_code=404, detail="Subscription not found")

    await delete_alert_function(state.function_name)

    try:
        # The ingestor removes this client from its in-memory subscriber set.
        publish_asset_demand(
            {
                "action": "unsubscribe",
                "asset": state.asset,
                "client_id": client_id,
            }
        )
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to publish unsubscribe demand: {exc}",
        ) from exc

    subscriptions.pop(client_id, None)


@app.delete("/cep-subscriptions/{client_id}", status_code=204)
async def delete_cep_subscription(
    client_id: str,
    subscription_id: str | None = None,
    asset: str | None = None,
) -> None:
    cep_subscription_id = subscription_id or client_id
    try:
        publish_cep_subscription(
            {
                "action": "unsubscribe",
                "client_id": client_id,
                "subscription_id": cep_subscription_id,
            }
        )
        if asset:
            publish_asset_demand(
                {
                    "action": "unsubscribe",
                    "asset": asset.upper(),
                    "client_id": client_id,
                }
            )
    except Exception as exc:
        raise HTTPException(
            status_code=502,
            detail=f"Failed to delete CEP subscription: {exc}",
        ) from exc


@app.get("/debug/functions/{client_id}")
async def get_function_debug(client_id: str) -> dict:
    function_name = f"alert-{client_id}"

    async with httpx.AsyncClient(timeout=30.0) as client:
        config_response = await client.get(function_endpoint(function_name))
        status_response = await client.get(f"{function_endpoint(function_name)}/status")

    return {
        "function_name": function_name,
        "config_status_code": config_response.status_code,
        "config": config_response.json()
        if config_response.headers.get("content-type", "").startswith("application/json")
        else config_response.text,
        "runtime_status_code": status_response.status_code,
        "runtime_status": status_response.json()
        if status_response.headers.get("content-type", "").startswith("application/json")
        else status_response.text,
        "expected_input_topic": RAW_OPPORTUNITIES_TOPIC,
        "expected_alert_log_topic": FUNCTION_LOG_TOPIC,
    }


@app.post("/debug/raw-quotes", response_model=DebugRawQuoteResponse)
async def publish_debug_raw_quote(
    request: DebugRawQuoteRequest,
) -> DebugRawQuoteResponse:
    # Debug-only Postman helper. It publishes the same raw message shape that
    # the ingestor normally publishes after reading yFinance.
    payload = {
        "asset": request.asset,
        "price": request.price,
        "timestamp": request.timestamp
        or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source": "debug-api",
    }
    publish_raw_quote(payload)
    return DebugRawQuoteResponse(raw_topic=RAW_OPPORTUNITIES_TOPIC, payload=payload)
