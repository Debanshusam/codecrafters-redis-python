import logging as logger
from app.packages.resp_handler import resp_converter as RESPENC


# Store all the Mapping sent from client here in this dict
_set_data_map: dict[bytes, bytes] = {}

_KEY_NOT_FOUND: bytes = b"$-1"
_KEY_SAVED: bytes = b"OK"
_INVALID_INPUT: bytes = b"ERR"


def set_impl(payload: bytes) -> bytes:
    payload_splited: list[bytes] = payload.split(RESPENC.RESP_BASE_SEP)
    _response_payload: bytes = b""

    payload_splited = [item for item in payload_splited if item]

    if len(payload_splited) > 2:
        _response_payload = _INVALID_INPUT
        logger.error(f"payload_splited = {payload_splited}")
        logger.error(f"Invalid SET command: {payload}")
    else:
        logger.debug(f"SET command: {payload}")
        _set_data_map[payload_splited[0]] = payload_splited[1]
        _response_payload = _KEY_SAVED

    logger.debug(f"_response_payload = {_response_payload}")
    return _response_payload


def get_impl(payload: bytes) -> bytes:
    payload_splited: list[bytes] = payload.split(RESPENC.RESP_BASE_SEP)
    _response_payload: bytes = b""

    payload_splited = [item for item in payload_splited if item]

    if len(payload_splited) > 1:
        _response_payload = _INVALID_INPUT
        logger.error(f"Invalid SET command: {payload}")

    elif payload_splited[0] not in _set_data_map:
        logger.debug(f"Key not found: {payload_splited[0]}")
        _response_payload = _KEY_NOT_FOUND

    else:
        value = _set_data_map[payload_splited[0]]
        logger.debug(f"GET command: {payload} => {value}")
        _response_payload = _set_data_map[payload_splited[0]]

    logger.debug(f"_response_payload = {_response_payload}")
    return _response_payload
