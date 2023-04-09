import time
import web3
import json
from typing import Any, Callable, Dict, Iterator, Optional, Tuple, Union, cast
import torch

from flwr.proto.transport_pb2 import ClientMessage, ServerMessage
from flwr.common.telemetry import event, EventType
from flwr.client.message_handler.message_handler import handle
from flwr.common.logger import log
from flwr.client.client import (
    Client,
    maybe_call_evaluate,
    maybe_call_fit,
    maybe_call_get_parameters,
    maybe_call_get_properties,
)
from logging import DEBUG, INFO
import boto3
import hashlib

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

def _fit(client: Client, fit_ins) -> ClientMessage:
    # Perform fit
    fit_res = maybe_call_fit(
        client=client,
        fit_ins=fit_ins,
    )

    return fit_res

def _evaluate(client: Client, evaluate_ins) -> ClientMessage:
    # Perform evaluation
    evaluate_res = maybe_call_evaluate(
        client=client,
        evaluate_ins=evaluate_ins,
    )

    return evaluate_res

def make_global_model(model_hashes):
    # read the files from the local storage
    models = []
    for model_hash in model_hashes:
        with open(f'./models/{model_hash}', 'rb') as f:
            models.append(f.read())
    
    # average the models
    global_model = sum(models) / len(models)

    return global_model


def handle_receive(client, msg, s3):
    # deserialize the message
    server_msg = ServerMessage()
    server_msg.ParseFromString(msg)

    field = server_msg.WhichOneof('msg')
    if field == "fit_ins":
        # read model hashes from the contract and download the model from S3
        model_hashes = server_msg.fit_ins.model_hashes
        for model_hash in model_hashes:
            # specify the bucket name and object key
            bucket_name = 'my-bucket'
            object_key = model_hash

            # specify the path to the file to download
            file_path = f'./models/{model_hash}'

            # check hash value


            # download the file from S3
            with open(file_path, 'wb') as f:
                s3.download_fileobj(bucket_name, object_key, f)

        # make the global model from aggregating the downloaded models
        global_model = make_global_model(model_hashes)

        # fit the model on client
        return _fit(client, server_msg.fit_ins)
    
    elif field == "evaluate_ins":
        # evaluate the model on client
        return _evaluate(client, server_msg.evaluate_ins)


def listen_for_event(contract, event_name):
    # create a filter to listen for the specified event
    event_filter = contract.events[event_name].createFilter(fromBlock='latest')

    while True:
        # check if any new events have been emitted
        for event in event_filter.get_new_entries():
            # if the specified event has been emitted, return its message
            if event.event == event_name:
                yield event.args
                return
        # wait for new events
        time.sleep(60)


def handle_send(w3, s3, contract, msg, sender_address, sender_private_key):
    
    if msg['type'] == 'FitRes':
        # hash the message params which is dictionary

        # Concatenate all tensors in the state_dict
        concatenated_tensor = torch.cat([param.view(-1) for param in model['params'].values()])

        # Convert the concatenated tensor to bytes and calculate the hash
        tensor_bytes = concatenated_tensor.numpy().tobytes()
        model_hash = hashlib.sha256(tensor_bytes).hexdigest()

        # save the model to the local storage
        with open(f'./models/{model_hash}.bin', 'wb') as f:
            f.write(msg['params'])

        # specify the bucket name and object key
        bucket_name = 'my-bucket'
        object_key = 'my-object'

        # upload the file to S3
        with open(f'./models/{model_hash}.bin', 'rb') as f:
            s3.upload_fileobj(f, bucket_name, object_key)

        args = [model_hash]

        # get the function object from the contract ABI
        function = getattr(contract.functions, 'FitRes')(*args)

    elif msg['type'] == 'EvaluateRes':
        args = [msg['params']]
        function = getattr(contract.functions, 'EvaluateRes')(*args)

    # build the transaction dictionary
    transaction = {
        'from': sender_address,
        'to': contract.address,
        'gas': w3.eth.estimateGas({'to': contract.address, 'data': function.encodeABI()}),
        'gasPrice': w3.eth.gasPrice,
        'nonce': w3.eth.getTransactionCount(sender_address),
        'data': function.encodeABI(),
    }

    # sign the transaction using the sender's private key
    signed_txn = w3.eth.account.signTransaction(
        transaction, sender_private_key)

    # send the signed transaction to the network
    txn_hash = w3.eth.sendRawTransaction(signed_txn.rawTransaction)

    # wait for the transaction to be mined and return the receipt
    txn_receipt = w3.eth.waitForTransactionReceipt(txn_hash)
    return txn_receipt


def web3_connection(contract_address, abi
                    ) -> Iterator[Tuple[Callable[[], ServerMessage], Callable[[ClientMessage], None]]]:
    """
    Establish a connection to the Flower server using web3.py.

    Functionality
    -------------
    1. connect to the contract
    2. 주기적으로 contract에 접근해서 message를 받아옴
    3. update된 message가 있으면 받아온 message를 receive에서 return
    4. send에 넣은 message를 contract에 send transaction

    Returns
    -------
    receive, send : Callable, Callable
    """

    # Connect to the contract
    w3 = web3.Web3(web3.HTTPProvider("http://localhost:8545"))

    contract = w3.eth.contract(address=contract_address, abi=abi)

    # Create a generator to iterate over the messages
    web3_message_iterator = listen_for_event(contract, "ServerMessage")

    # read account address and private key from json file
    with open('account.json') as f:
        account = json.load(f)

    account_address = account['address']
    account_private_key = account['privateKey']

    receive: Callable[[], ServerMessage] = lambda: next(web3_message_iterator)
    send: Callable[[ClientMessage], None] = lambda msg: handle_send(w3, contract, "ClientMessage",
                                                                    msg, account_address, account_private_key)

    try:
        yield (receive, send)
    finally:
        # Close the connection
        log(DEBUG, "web3 connection closed")


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
