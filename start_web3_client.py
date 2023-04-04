import time
import web3
from typing import Any, Callable, Dict, Iterator, Optional, Tuple, Union, cast

from flwr.proto.transport_pb2 import ClientMessage, ServerMessage
from flwr.common.telemetry import event, EventType
from flwr.client.message_handler.message_handler import handle
from flwr.common.logger import log, INFO

"""
Server Message
- ReconnectIns
- GetPropertiesIns
- GetPropertiesIns.ConfigEntry
- GetParametersIns
- GetParametersIns.ConfigEntry
- FitIns
- FitIns.ConfigEntry
- EvaluateIns
- EvaluateIns.ConfigEntry
"""

"""
Client Message
- DisconnectRes
- GetPropertiesRes
- GetPropertiesRes.PropertiesEntry
- GetParametersRes
- FitRes
- FitRes.MetricsEntry
- EvaluateRes
- EvaluateRes.MetricsEntry
"""

def web3_connection(contract_address
                    ) -> Iterator[Tuple[Callable[[], ServerMessage], Callable[[ClientMessage], None]]]:
    """
    Establish a connection to the Flower server using web3.py.

    Functionality
    -------------
    1. connect to the contract
    2. 주기적으로 contract에 접근해서 message를 받아옴
    3. update된 message가 있으면 받아온 message를 receive에 넣음
    4. send에 넣은 message를 contract에 넣음

    Returns
    -------
    receive, send : Callable, Callable
    """

    pass


def start_web3_client(client, contract_address):
    """Start a Flower client using web3.py.
    Everything is same with start_client except the connection, message part.
    grpc_connection -> web3_connection
    """
    event(EventType.START_CLIENT_ENTER)

    while True:
        sleep_duration: int = 0
        with web3_connection(
            contract_address,
        ) as conn:
            receive, send = conn

            while True:
                server_message = receive()
                client_message, sleep_duration, keep_going = handle(
                    client, server_message
                )
                send(client_message)
                if not keep_going:
                    break
        if sleep_duration == 0:
            log(INFO, "Disconnect and shut down")
            break
        # Sleep and reconnect afterwards
        log(
            INFO,
            "Disconnect, then re-establish connection after %s second(s)",
            sleep_duration,
        )
        time.sleep(sleep_duration)

    event(EventType.START_CLIENT_LEAVE)
    pass
