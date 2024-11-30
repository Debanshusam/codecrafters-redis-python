import logging as logger

from app.packages.resp_handler import resp_converter as RESPENC


def redis_client_inp_parser(data: bytes) -> bytes:
    _response_to_client: bytes | list[bytes] = RESPENC.redis_resp_parser(
        resp_inp=data
        )

    if isinstance(_response_to_client, bytes):
        logger.debug(f"_response_to_client = {_response_to_client}")
        return RESPENC.encode_to_resp2(_response_to_client)

    elif isinstance(_response_to_client, list):
        _concat_response_to_client: bytes = b""
        for x in _response_to_client:
            _concat_response_to_client: bytes = x + RESPENC.RESP_BASE_SEP

        logger.debug(f"_concat_response_to_client = {_concat_response_to_client}")  # noqa: E501
        return RESPENC.encode_to_resp2(_concat_response_to_client)

    else:
        return RESPENC.encode_to_resp2(b'')
