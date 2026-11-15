"""Turns an applied ledger command into a small event for RabbitMQ."""


def to_event(item: dict) -> dict | None:
    """item = {"log_index", "command", "result"} from the ledger's /events endpoint.
    Returns None for anything that changed nothing (rejected, duplicate, noop, test)."""
    command, result = item["command"], item["result"]
    if not result.get("ok") or result.get("duplicate"):
        return None
    kind = command.get("type")
    event = {"log_index": item["log_index"], "type": kind}
    if kind == "register_farm":
        event.update(farm_id=command["farm_id"])
    elif kind == "open_day":
        event.update(day=command["day"], limit_l=int(command["limit_l"]),
                     allocations={f: int(a) for f, a in command.get("allocations", {}).items()})
    elif kind == "record_consumption":
        if not result.get("amount_l"):
            return None                                # every event in the batch was already recorded
        event.update(day=command["day"], farm_id=command["farm_id"], amount_l=int(result["amount_l"]),
                     events=int(result["recorded"]))
    elif kind == "transfer":
        event.update(day=command["day"], sender=command["sender"], recipient=command["recipient"],
                     amount_l=int(command["amount_l"]), transfer_id=command["transfer_id"])
    else:
        return None
    return event
