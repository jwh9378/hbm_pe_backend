import asyncio
import logging
from typing import Annotated, Any

from fastapi import APIRouter, BackgroundTasks, Depends, WebSocket, WebSocketDisconnect
from fastcrud import FastCRUD
from sqlalchemy.ext.asyncio import AsyncSession

from ...core.db.database import async_get_db, local_session
from ...core.exceptions.http_exceptions import NotFoundException
from ...core.utils.websocket_manager import manager
from ...models.command import Command
from ...schemas.command import CommandCreate, CommandRead, CommandUpdate

logger = logging.getLogger(__name__)

router = APIRouter(tags=["iot"])

# CRUD 인스턴스 생성
crud_command = FastCRUD(Command)

PI_PORT = 9999


async def send_socket_command(command_id: int, target_ip: str, port: int, command_text: str) -> None:
    """백그라운드에서 소켓 통신을 실행하는 헬퍼 함수입니다."""
    final_status = "SUCCESS"
    writer = None
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(target_ip, port), timeout=5.0)
        writer.write(command_text.encode("utf-8"))
        await writer.drain()

        # 명령 전송 후 장비의 응답(ACK)을 대기합니다.
        data = await asyncio.wait_for(reader.read(1024), timeout=5.0)
        response_text = data.decode("utf-8").strip()

        if "OK" not in response_text.upper():
            logger.error(f"Invalid response from {target_ip}:{port} - '{response_text}'")
            final_status = "FAILED"

    except TimeoutError:
        logger.error(f"Timeout while communicating with {target_ip}:{port}")
        final_status = "FAILED"
    except Exception as e:
        logger.error(f"Failed to send command to {target_ip}:{port} - {e}")
        final_status = "FAILED"
    finally:
        if writer:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception as close_error:
                logger.debug(f"Socket close failed for {target_ip}: {close_error}")
        # 백그라운드 작업이므로 요청 스코프와 분리된 새로운 DB 세션을 열어서 업데이트를 수행합니다.
        async with local_session() as db:
            await crud_command.update(db, object=CommandUpdate(status=final_status), id=command_id)


@router.post("/send_command", response_model=CommandRead)
async def send_command(
    data: CommandCreate, background_tasks: BackgroundTasks, db: Annotated[AsyncSession, Depends(async_get_db)]
):
    """사용자가 웹에서 기기로 명령을 보냅니다."""
    # 1. DB에 명령어 저장 내역 기록
    new_command = await crud_command.create(db, object=data)

    # 2. 라즈베리파이로 소켓 통신 전송 (백그라운드 처리로 위임)
    target_ip = data.target_device_ip
    command_text = data.command_text

    # 네트워크 I/O 대기 없이 즉각 응답하기 위해 백그라운드 태스크에 등록합니다.
    background_tasks.add_task(send_socket_command, new_command.id, target_ip, PI_PORT, command_text)

    return new_command


@router.get("/command/{command_id}", response_model=CommandRead)
async def get_command_status(command_id: int, db: Annotated[AsyncSession, Depends(async_get_db)]):
    """명령어의 현재 상태를 조회합니다."""
    db_command = await crud_command.get(db=db, id=command_id, schema_to_select=CommandRead)

    if not db_command:
        raise NotFoundException("Command not found")

    return db_command


async def check_hardware_status(target_device_ip: str) -> dict[str, Any]:
    pi_a_status = "not ready"
    pi_b_status = "not ready"
    last_result = "N/A"  # TODO (Notice : Do not modify it)
    last_updated = "N/A"  # TODO (Notice : Do not modify it)
    writer = None

    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(target_device_ip, PI_PORT), timeout=2.0)

        writer.write(b"STATUS")
        await writer.drain()

        data = await asyncio.wait_for(reader.read(1024), timeout=1.5)
        response_text = data.decode("utf-8").strip()

        pi_a_status = "ready"
        if "ready" in response_text.lower() and "not ready" not in response_text.lower():
            pi_b_status = "ready"

    except Exception as e:
        logger.debug(f"Status check failed for {target_device_ip}: {e}")
    finally:
        if writer:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception as close_error:
                logger.debug(f"Socket close failed for {target_device_ip}: {close_error}")

    return {
        "backendStatus": "ready",
        "piAStatus": pi_a_status,
        "piBStatus": pi_b_status,
        "lastResult": last_result,
        "lastUpdated": last_updated,
    }


@router.websocket("/raspberry/status/{target_device_ip}")
async def websocket_status_endpoint(websocket: WebSocket, target_device_ip: str):
    """웹소켓을 통해 5초마다 라즈베리파이 상태를 클라이언트에게 전송합니다."""
    await manager.connect(websocket)
    logger.info(f"WebSocket client connected for {target_device_ip}")

    try:
        # 연결 직후 즉시 한 번 상태를 보내줌 (첫 렌더링용)
        initial_status = await check_hardware_status(target_device_ip)
        await websocket.send_json(initial_status)

        # 이후 5초마다 백그라운드에서 상태 체크 후 전송
        while True:
            await asyncio.sleep(5)
            status = await check_hardware_status(target_device_ip)
            await websocket.send_json(status)
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")
    except Exception as e:
        logger.error(f"WebSocket Error: {e}")
    finally:
        manager.disconnect(websocket)
