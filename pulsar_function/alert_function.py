import json

from pulsar import Function


def emit_debug(logger, stage, **fields):
    # Prefixing the log makes it easy to find these entries in the
    # function-logs topic from Postman.
    payload = {"stage": stage, **fields}
    message = f"ALERT_FUNCTION_DEBUG {json.dumps(payload, default=str)}"
    print(message, flush=True)
    logger.info(message)


def load_user_config(context):
    # Pulsar Python runtimes expose userConfig differently across versions.
    # This project runs on Pulsar 3.2.0, where get_user_config() may not exist.
    if hasattr(context, "get_user_config"):
        return context.get_user_config(), "get_user_config"

    if hasattr(context, "get_user_config_map"):
        return context.get_user_config_map(), "get_user_config_map"

    if hasattr(context, "get_user_config_value"):
        keys = ("asset", "rule", "value", "client_id")
        return (
            {key: context.get_user_config_value(key) for key in keys},
            "get_user_config_value",
        )

    return {}, "unavailable"


class AlertFunction(Function):
    def process(self, input, context):
        logger = context.get_logger()
        cfg, config_source = load_user_config(context)

        # Every message from raw-opportunities should reach this point. If this
        # debug entry does not appear, the Function is not consuming the input
        # topic or is not running.
        data = json.loads(input)
        emit_debug(
            logger,
            "received",
            input=data,
            config_source=config_source,
            full_config=cfg,
            configured_asset=cfg.get("asset"),
            configured_rule=cfg.get("rule"),
            configured_value=cfg.get("value"),
            client_id=cfg.get("client_id"),
        )

        incoming_asset = data["asset"]
        configured_asset = cfg["asset"]
        asset_matches = incoming_asset == configured_asset

        emit_debug(
            logger,
            "asset_filter",
            incoming_asset=incoming_asset,
            configured_asset=configured_asset,
            asset_matches=asset_matches,
        )

        if not asset_matches:
            emit_debug(logger, "ignored_asset", reason="asset_mismatch")
            return None

        # Convert both values to float before comparing. This avoids accidental
        # string comparison and makes the debug output show the exact numbers.
        price = float(data["price"])
        target = float(cfg["value"])
        rule = cfg["rule"]
        gte_result = price >= target
        lte_result = price <= target
        matched = (rule == "gte" and gte_result) or (rule == "lte" and lte_result)

        emit_debug(
            logger,
            "comparison",
            price=price,
            target=target,
            rule=rule,
            gte_result=gte_result,
            lte_result=lte_result,
            matched=matched,
        )

        if matched:
            alert = {
                "asset": data["asset"],
                "price": price,
                "rule": rule,
                "target_value": target,
                "client_id": cfg["client_id"],
                "triggered_at": data["timestamp"],
                "source": data.get("source", "unknown"),
            }
            emit_debug(logger, "alert_returned", alert=alert)
            return json.dumps(alert)

        emit_debug(logger, "no_alert_returned", reason="rule_not_matched")
        return None
