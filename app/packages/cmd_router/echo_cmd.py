import logging as logger

from app.packages.resp_handler import resp_converter as RESPENC


def echo_impl(payload: bytes) -> bytes:
    payload_splited: list[bytes] = payload.split(RESPENC.RESP_BASE_SEP)
    _response_payload: bytes = b""

    payload_splited = [item for item in payload_splited if item]
    logger.debug(f"payload_splited = {payload_splited}")

    for x in payload_splited:
        _response_payload += x

    logger.debug(f"_response_payload = {_response_payload}")
    return _response_payload
