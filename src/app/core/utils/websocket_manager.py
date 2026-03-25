from fastapi import WebSocket


class ConnectionManager:
    def __init__(self):
        # WebSocket 객체를 키로, target_ip를 값으로 가지는 딕셔너리로 변경
        self.active_connections: dict[WebSocket, str | None] = {}

    async def connect(self, websocket: WebSocket, target_ip: str | None = None):
        await websocket.accept()
        self.active_connections[websocket] = target_ip

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            del self.active_connections[websocket]

    async def broadcast(self, message: dict, target_ip: str | None = None):
        dead_connections = []

        for connection, conn_target_ip in self.active_connections.items():
            # target_ip가 지정되지 않았거나, 연결의 target_ip와 일치하는 경우 전송
            if target_ip is None or conn_target_ip == target_ip:
                try:
                    await connection.send_json(message)
                except Exception:
                    dead_connections.append(connection)

        for connection in dead_connections:
            self.disconnect(connection)


manager = ConnectionManager()
