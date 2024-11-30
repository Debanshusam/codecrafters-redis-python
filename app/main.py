import logging as logger
import asyncio
from . import _redis_cmd_parser as PARSER


async def _ready_client_response(
        writer: asyncio.StreamWriter,
        client_input_cmd: bytes
        ) -> None:

    response_data: bytes = b""
    response_data = PARSER.redis_client_inp_parser(
        client_input_cmd
        ) or b""
    # writer.write(RESP.encode_string_as_resp2(response_data))
    writer.write(data=response_data)
    await writer.drain()


async def _receive_client_input(
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter
        ) -> None:
    """Receives the client input."""

    while True:
        buffered_data: bytes = await reader.read(1024)
        logger.debug(f"buffered_data = {buffered_data}")

        buffered_data_str: str = buffered_data.decode()
        logger.debug(f"buffered_data_str = {buffered_data_str}")

        if not buffered_data_str:
            logger.warning("Empty data received from client")
            break

        # Readying the Client input
        await _ready_client_response(
            writer=writer,
            client_input_cmd=buffered_data
            )

    writer.close()
    await writer.wait_closed()


async def _init_socket_server(sock_add: tuple[str, int]) -> None:
    """Initializes the socket server.

    - uses asyncio.start_server() to create a server object

    Args:
        sock_add (tuple[str, int]): _description_
    """
    server: asyncio.Server = await asyncio.start_server(
        client_connected_cb=_receive_client_input,
        host=sock_add[0],
        port=sock_add[1]
        )
    async with server:
        await server.serve_forever()


if __name__ == "__main__":

    # Define a custom logging format
    LOG_FORMAT = (
        "%(asctime)s - %(levelname)s - "
        "%(filename)s - %(funcName)s():%(lineno)d - %(message)s"
    )

    # Configure the logging module
    logger.basicConfig(
        level=logger.DEBUG,
        format=LOG_FORMAT,
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    logger.basicConfig(level=logger.DEBUG)

    # Socket Address Definition
    sock_add = ("127.0.0.1", 6379)

    # Running the main function
    asyncio.run(main=_init_socket_server(sock_add=sock_add))
