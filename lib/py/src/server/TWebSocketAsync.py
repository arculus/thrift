# -*- coding: utf-8 -*-
import time
import asyncio
import logging
from thrift.Thrift import TMessageType

from thrift.wsasync import on_task_done


class WebSocketConnectionRunner:

    def __init__(self, ttrans, tproto, processor):
        self.ttransport = ttrans
        self.tprotocol = tproto
        self.processor = processor

        self.task = None
        
        # just a timestamp for when we recieved the last message
        self.last_message_recieved = 0

    async def run(self):

        while 1:
            try:
                iproto = await self.tprotocol.wait_for_message(TMessageType.CALL, TMessageType.ONEWAY)
                
                self.last_message_recieved = time.time()

                # Mittels await wird hier der Kontrollfluss gestoppt, bis die Message verarbeitet wurde.
                # Um Nachrichten paralleler empfangen zu können sollte man überlegen, hier das Bearbeiten mit
                # asyncio.ensure_future() abzusetzen.
                await self.processor.process(iproto, self.tprotocol)

            except asyncio.CancelledError:
                break
            except Exception as x:
                logging.exception(x)
                continue

        self.ttransport.close()

    def start_async_task(self):
        self.task = asyncio.ensure_future(self.run())
        self.task.add_done_callback(lambda ft: on_task_done("WebSocketConnectionRunner.run()", ft))


class ClientRegistry:
    """
    Auf Serverseite zum tracken der verbundenen Clients
    """

    def __init__(self, ClientStub):
        self._ClientStub = ClientStub
        self._clients = {}

    def new_connection(self, tprotocol, peer):
        client = self._ClientStub(tprotocol)
        client.peer = peer
        self._clients[peer] = client

    def drop_connection(self, peer):
        if peer in self._clients:
            del self._clients[peer]

    def __iter__(self):
        return iter(self._clients.values())

    def __getitem__(self, peer):
        return self._clients[peer]

    def __contains__(self, peer):
        return peer in self._clients

    @property
    def peer_ids(self):
        return list(self._clients.keys())

    def __str__(self):
        return str(self.peer_ids)
