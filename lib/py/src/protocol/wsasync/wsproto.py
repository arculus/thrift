# -*- coding: utf-8 -*-
import asyncio
import logging
from autobahn.asyncio.websocket import WebSocketClientProtocol, WebSocketServerProtocol

from thrift.transport.TWebSocketAsync import TBufferedWebSocketServerTransport
from thrift.protocol.wsasync.tproto import TBinaryMessageTypeDispatchingAcceleratedProtocol
from thrift.server.TWebSocketAsync import WebSocketConnectionRunner

log = logging.getLogger("thrift.wsasync.wsproto")
log.setLevel(logging.WARNING)

connlog = logging.getLogger("thrift.wsasync.conn")
connlog.setLevel(logging.INFO)


class WSProtocolBase:

    OPEN_TIMEOUT = 10

    def __init__(self):
        super().__init__()

        self._open_event = asyncio.Event()

        self._on_message_callback = None

    def set_on_message_callback(self, cb):
        self._on_message_callback = cb

    async def wait_for_opened(self):
        await asyncio.wait_for(self._open_event.wait(), self.OPEN_TIMEOUT)

    def onConnect(self, response):
        log.debug("(peer=%s) websocket connected" % response.peer)
        assert self.peer == response.peer

    def onOpen(self):
        log.debug("(peer=%s) websocket connection now open" % self.peer)
        connlog.info("websocket connection opened for peer=%s" % self.peer)
        self._open_event.set()

    def onClose(self, wasClean, code, reason):
        log.debug("(peer=%s) websocket connection closed by peer (clean=%s, code=%s, reason=%s)" % \
                  (self.peer, wasClean, code, reason))
        connlog.info("websocket connection CLOSED for peer=%s" % self.peer)
        self._open_event.clear()

    def onMessage(self, payload, isBinary):  # @UnusedVariable
        """
        ob `isBinary` True oder False ist, payload ist immer ein bytes-Objekt. Diese Information ist also irrelevant.
        """
        log.debug("(peer=%s) --> recieved %d bytes" % (self.peer, len(payload)))
        if self._on_message_callback:
            self._on_message_callback(payload)

    def sendMessage(self, payload, isBinary=False):
        log.debug("(peer=%s) <-- sending %d bytes" % (self.peer, len(payload)))
        return super().sendMessage(payload, isBinary=isBinary)


class WSClientProtocol(WSProtocolBase, WebSocketClientProtocol):
    pass


class WSServerProtocol(WSProtocolBase, WebSocketServerProtocol):

    def __init__(self):
        super().__init__()

        self.processor = None
        self.client_registry = None
        self._conn_runner = None

    def onOpen(self):
        assert self.processor
        assert self.client_registry

        # der Transport muss hier nicht explizit geöffnet werden, da die Verbindung ja vom Client kam, also auf
        # TCP-Ebene schon steht und auf WebSocket-Ebene wurde dieser Task aus dem onOpened-Handler gestartet, also
        # der WebSocket ist auch schon betriebsbereit.
        transport = TBufferedWebSocketServerTransport(self)
        protocol = TBinaryMessageTypeDispatchingAcceleratedProtocol(transport)

        self._conn_runner = WebSocketConnectionRunner(transport, protocol, self.processor)
        self._conn_runner.start_async_task()

        self.client_registry.new_connection(protocol, self)

        super().onOpen()

    def onClose(self, wasClean, code, reason):
        assert self.client_registry

        # onClose wird auch aufgerufen, wenn noch kein onOpen gelaufen war
        if self._conn_runner:
            self._conn_runner.task.cancel()
            self.client_registry.remove_connection(self.peer)

        super().onClose(wasClean, code, reason)
