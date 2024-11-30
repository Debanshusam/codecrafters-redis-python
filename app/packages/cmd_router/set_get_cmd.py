import logging as logger
from app.packages.resp_handler import resp_converter as RESPENC
import datetime
from typing import TypedDict

# Store all the Mapping sent from client here in this dict
# _set_data_map: dict[bytes, bytes] = {}


class ValueValidityDict(TypedDict):
    value: bytes
    valid_until: datetime.datetime | None


_set_data_map: dict[bytes, ValueValidityDict] = {}


_KEY_NOT_FOUND: bytes = b"$-1"
_KEY_EXPIRED: bytes = b"$-1"
_KEY_SAVED: bytes = b"OK"
_INVALID_INPUT: bytes = b"ERR"


def set_impl(payload: bytes) -> bytes:
    payload_splited: list[bytes] = payload.split(RESPENC.RESP_BASE_SEP)
    _response_payload: bytes = b""

    payload_splited = [item for item in payload_splited if item]
    logger.debug(f"payload_splited = {payload_splited}")

    if len(payload_splited) > 2 and payload_splited[2].lower() != b"px":
        _response_payload = _INVALID_INPUT
        logger.error(f"payload_splited = {payload_splited}")
        logger.error(f"Invalid SET command: {payload}")

    elif len(payload_splited) == 2:
        logger.debug(f"SET command: {payload}")

        _set_data_map[payload_splited[0]] = {
            "value": b"",
            "valid_until":  None
            }

        _set_data_map[payload_splited[0]]["value"] = payload_splited[1]

        logger.debug(
            f"key: {payload_splited[0]}"
            f"value: {_set_data_map[payload_splited[0]]['value']} "
            f"validity: {_set_data_map[payload_splited[0]]['valid_until']}"
        )

        _response_payload = _KEY_SAVED

    elif len(payload_splited) == 4 and payload_splited[2].lower() == b"px":

        # Adding support for "px" in SET command

        logger.debug(f"SET command: {payload}")
        _set_data_map[payload_splited[0]] = {
            "value": b"",
            "valid_until": None
            }

        _set_data_map[payload_splited[0]]["value"] = payload_splited[1]

        _VALIDITY_DELTA: int = int(payload_splited[3])
        _CURR_TIME: datetime.datetime = datetime.datetime.now(
            datetime.timezone.utc
            )
        _VALID_UNTIL: datetime.datetime = _CURR_TIME \
            + datetime.timedelta(milliseconds=_VALIDITY_DELTA)

        _set_data_map[payload_splited[0]]["valid_until"] = _VALID_UNTIL

        logger.debug(f"_CURR_TIME: {_CURR_TIME}")
        logger.debug(f"_VALID_UNTIL: {_VALID_UNTIL}")
        logger.debug(f"_VALIDITY_DELTA: {_VALIDITY_DELTA} ms")
        logger.debug(f"_VALID_UNTIL: {_VALID_UNTIL}")

        _response_payload = _KEY_SAVED

    logger.debug(
        f"key: {payload_splited[0]}"
        f"value: {_set_data_map[payload_splited[0]]['value']} "
        f"validity: {_set_data_map[payload_splited[0]]['valid_until']}"
    )
    logger.debug(f"_response_payload = {_response_payload}")
    return _response_payload


def get_impl(payload: bytes) -> bytes:

    # We have to capture the current time as soon as the request hits here
    # to maintain maximum accuracy
    _CURR_TIME: datetime.datetime = datetime.datetime.now(
        datetime.timezone.utc
        )

    payload_splited: list[bytes] = payload.split(RESPENC.RESP_BASE_SEP)
    _response_payload: bytes = b""

    payload_splited = [item for item in payload_splited if item]

    if len(payload_splited) > 1:
        _response_payload = _INVALID_INPUT
        logger.error(f"Invalid SET command: {payload}")

    elif payload_splited[0] not in _set_data_map:
        logger.debug(f"Key not found: {payload_splited[0]}")
        _response_payload = _KEY_NOT_FOUND

    elif _set_data_map[payload_splited[0]]["valid_until"] is None:
        # INFINITE VALIDITY KEY TYPES

        logger.debug(
            f"key: {payload_splited[0]}"
            f"value: {_set_data_map[payload_splited[0]]['value']} "
            f"validity ( INFINITE ): {_set_data_map[payload_splited[0]]['valid_until']}"  # noqa: E501
        )

        _response_payload = _set_data_map[payload_splited[0]]["value"]

    elif _set_data_map[payload_splited[0]]["valid_until"] >= _CURR_TIME:  # type: ignore # noqa: E501

        value = _set_data_map[payload_splited[0]]["value"]
        logger.debug(f"GET command: {payload} => {value}")
        logger.debug(
            msg=(
                f"key: {payload_splited[0]}"
                f"value: {_set_data_map[payload_splited[0]]['value']} "
                f"validity: {_set_data_map[payload_splited[0]]['valid_until']}"
            )
        )

        _response_payload = _set_data_map[payload_splited[0]]["value"]

    else:
        logger.debug(f"Key expired: {payload_splited[0]}")
        logger.debug(
            msg=(
                f"key: {payload_splited[0]}"
                f"value: {_set_data_map[payload_splited[0]]['value']} "
                f"validity ( EXPIRED ): {_set_data_map[payload_splited[0]]['valid_until']}"  # noqa: E501
            )
        )
        logger.debug(f"_CURR_TIME: {_CURR_TIME}")
        valid_until: datetime.datetime | None = _set_data_map[payload_splited[0]]['valid_until']  # noqa: E501
        if valid_until is not None:
            expired_since: datetime.timedelta = valid_until - _CURR_TIME
            logger.debug(f"Expired since: {expired_since.total_seconds() * 1000} ms")  # noqa: E501
        else:
            logger.error("Expired since: None")
        _response_payload = _KEY_EXPIRED

    logger.debug(f"_response_payload = {_response_payload}")
    return _response_payload
