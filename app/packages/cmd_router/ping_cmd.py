import logging as logger


def ping_implementation() -> bytes:
    _repsonse: bytes = "PONG".encode(encoding="utf-8")
    logger.debug(f"_repsonse = {_repsonse}")
    return _repsonse

