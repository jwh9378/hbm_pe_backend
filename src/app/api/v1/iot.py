import asyncio
import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect
from sqlalchemy.ext.asyncio import AsyncSession

from ...core.config import settings
from ...core.db.database import async_get_db
from ...core.exceptions.http_exceptions import NotFoundException
from ...core.utils import queue
from ...core.utils.iot_client import check_hardware_status
from ...core.utils.websocket_manager import manager
from ...crud.crud_command import crud_command
from ...schemas.command import CommandCreate, CommandRead

logger = logging.getLogger(__name__)

router = APIRouter(tags=["iot"])


@router.post("/send_command", response_model=CommandRead)
async def send_command(data: CommandCreate, db: Annotated[AsyncSession, Depends(async_get_db)]):
    """사용자가 웹에서 기기로 명령을 보냅니다."""
    # 1. DB에 명령어 저장 내역 기록
    new_command = await crud_command.create(db, object=data)

    # 2. 라즈베리파이로 소켓 통신 전송 (ARQ Redis 큐로 위임)
    target_ip = data.target_device_ip
    command_text = data.command_text

    if queue.pool is None:
        raise HTTPException(status_code=503, detail="Queue is not available")

    # 네트워크 I/O 대기 없이 즉각 응답하기 위해 Redis 기반 ARQ 큐에 작업을 추가합니다.
    try:
        job = await queue.pool.enqueue_job(
            "send_socket_command", new_command.id, target_ip, settings.PI_PORT, command_text
        )
    except Exception as e:
        logger.exception("Failed to enqueue command task")
        raise HTTPException(status_code=500, detail="Failed to enqueue command task") from e

    if job is None:
        raise HTTPException(status_code=500, detail="Failed to enqueue command task")

    return new_command


@router.get("/command/{command_id}", response_model=CommandRead)
async def get_command_status(command_id: int, db: Annotated[AsyncSession, Depends(async_get_db)]):
    """명령어의 현재 상태를 조회합니다."""
    db_command = await crud_command.get(db=db, id=command_id, schema_to_select=CommandRead)

    if not db_command:
        raise NotFoundException("Command not found")

    return db_command


@router.websocket("/raspberry/status/{target_device_ip}")
async def websocket_status_endpoint(websocket: WebSocket, target_device_ip: str):
    """웹소켓을 통해 5초마다 라즈베리파이 상태를 클라이언트에게 전송합니다."""
    await manager.connect(websocket)
    logger.info(f"WebSocket client connected for {target_device_ip}")

    try:
        # 연결 직후 즉시 한 번 상태를 보내줌 (첫 렌더링용)
        initial_status = await check_hardware_status(target_device_ip, settings.PI_PORT)
        await websocket.send_json(initial_status)

        # 이후 5초마다 백그라운드에서 상태 체크 후 전송
        while True:
            await asyncio.sleep(5)
            status = await check_hardware_status(target_device_ip, settings.PI_PORT)
            await websocket.send_json(status)
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")
    except Exception as e:
        logger.error(f"WebSocket Error: {e}")
    finally:
        manager.disconnect(websocket)
