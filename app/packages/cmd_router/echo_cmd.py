import logging as logger


def echo_implementation(payload: bytes) -> bytes:
    logger.debug(f"payload = {payload}")
    return payload
