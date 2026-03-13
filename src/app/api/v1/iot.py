import asyncio
import logging
from typing import Annotated

from fastapi import APIRouter, BackgroundTasks, Depends
from fastcrud import FastCRUD
from sqlalchemy.ext.asyncio import AsyncSession

from ...core.db.database import async_get_db
from ...core.exceptions.http_exceptions import NotFoundException
from ...models.command import Command
from ...schemas.command import CommandCreate, CommandRead, CommandUpdate

logger = logging.getLogger(__name__)

router = APIRouter(tags=["iot"])

# CRUD 인스턴스 생성
crud_command = FastCRUD(Command)


async def send_socket_command(command_id: int, target_ip: str, port: int, command_text: str) -> None:
    """백그라운드에서 소켓 통신을 실행하는 헬퍼 함수입니다."""
    final_status = "SUCCESS"
    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(target_ip, port), timeout=5.0)
        writer.write(command_text.encode("utf-8"))
        await writer.drain()
        writer.close()
        await writer.wait_closed()
    except Exception as e:
        logger.error(f"Failed to send command to {target_ip}:{port} - {e}")
        final_status = "FAILED"
    finally:
        # 백그라운드 작업이므로 요청 스코프와 분리된 새로운 DB 세션을 열어서 업데이트를 수행합니다.
        async for db in async_get_db():
            await crud_command.update(db, object=CommandUpdate(status=final_status), id=command_id)
            break


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
    port = 9999  # 라즈베리파이에서 수신 대기 중인 소켓 서버의 포트 (환경에 맞게 수정하세요)

    # 네트워크 I/O 대기 없이 즉각 응답하기 위해 백그라운드 태스크에 등록합니다.
    background_tasks.add_task(send_socket_command, new_command.id, target_ip, port, command_text)

    return new_command


@router.get("/command/{command_id}", response_model=CommandRead)
async def get_command_status(command_id: int, db: Annotated[AsyncSession, Depends(async_get_db)]):
    """명령어의 현재 상태를 조회합니다."""
    db_command = await crud_command.get(db=db, id=command_id, schema_to_select=CommandRead)

    if not db_command:
        raise NotFoundException("Command not found")

    return db_command
