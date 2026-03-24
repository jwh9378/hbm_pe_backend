import asyncio
import json
import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect
from redis.asyncio import Redis
from sqlalchemy import delete, func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from ...core.config import settings
from ...core.db.database import async_get_db
from ...core.exceptions.http_exceptions import NotFoundException
from ...core.utils import queue
from ...core.utils.cache import async_get_redis
from ...core.utils.iot_client import check_hardware_status
from ...core.utils.websocket_manager import manager
from ...crud.crud_command import crud_command
from ...crud.crud_queue import crud_pgm_queue
from ...models.pgm_queue import PGMQueue
from ...schemas.command import CommandCreate, CommandRead
from ...schemas.pgm_queue import (
    PGMQueueCreateInternal,
    PGMQueueListResponse,
    PGMQueuePayload,
    PGMQueueRead,
)

logger = logging.getLogger(__name__)

router = APIRouter(tags=["iot"])


GLOBAL_TARGET_DEVICE_IP: str | None = None


# --- Helper Functions ---
def _get_target_ip() -> str:
    """현재 연결된 라즈베리파이 IP를 가져오거나 예외를 발생시킵니다."""
    if not GLOBAL_TARGET_DEVICE_IP:
        raise HTTPException(
            status_code=400,
            detail="Target device IP is not set. WebSocket connection required first.",
        )
    return GLOBAL_TARGET_DEVICE_IP


def _ensure_queue() -> None:
    """ARQ 큐가 사용 가능한지 확인합니다."""
    if queue.pool is None:
        raise HTTPException(status_code=503, detail="Queue is not available")


async def _enqueue_job(job_name: str, *args) -> None:
    """에러 처리를 포함하여 백그라운드 큐에 작업을 추가합니다."""
    _ensure_queue()
    try:
        job = await queue.pool.enqueue_job(job_name, *args)
        if job is None:
            raise Exception("Job is None")
    except Exception as e:
        logger.exception(f"Failed to enqueue {job_name} task")
        raise HTTPException(status_code=500, detail=f"Failed to enqueue {job_name} task") from e


async def _publish_event(redis: Redis, target_ip: str, payload: dict) -> None:
    """Redis Pub/Sub을 통해 웹소켓 클라이언트로 이벤트를 발행합니다."""
    await redis.publish(f"test-status:{target_ip}", json.dumps(payload))


@router.post("/send_command", response_model=CommandRead)
async def send_command(data: CommandCreate, db: Annotated[AsyncSession, Depends(async_get_db)]):
    """사용자가 웹에서 기기로 명령을 보냅니다."""
    new_command = await crud_command.create(db, object=data)

    # 네트워크 I/O 대기 없이 즉각 응답하기 위해 Redis 기반 ARQ 큐에 작업을 추가합니다.
    await _enqueue_job(
        "send_socket_command",
        new_command.id,
        data.target_device_ip,
        settings.PI_PORT,
        data.command_text,
    )

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
    global GLOBAL_TARGET_DEVICE_IP
    GLOBAL_TARGET_DEVICE_IP = target_device_ip

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


@router.websocket("/raspberry/test-status")
async def websocket_test_status_endpoint(websocket: WebSocket, redis: Annotated[Redis, Depends(async_get_redis)]):
    """웹소켓을 통해 라즈베리파이의 큐(테스트) 상태 변화를 클라이언트에게 실시간으로 푸시합니다."""
    await manager.connect(websocket)
    logger.info(f"Test status WebSocket client connected for {GLOBAL_TARGET_DEVICE_IP}")

    pubsub = redis.pubsub()
    await pubsub.subscribe(f"test-status:{GLOBAL_TARGET_DEVICE_IP}")

    try:
        while True:
            message = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if message:
                data = message["data"]
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                await websocket.send_text(data)
            else:
                # 메시지가 없을 경우 다른 태스크가 실행될 수 있도록 짧게 대기합니다.
                await asyncio.sleep(0.1)
    except WebSocketDisconnect:
        logger.info("Test status WebSocket client disconnected")
    except Exception as e:
        logger.error(f"Test status WebSocket Error: {e}")
    finally:
        await pubsub.unsubscribe(f"test-status:{GLOBAL_TARGET_DEVICE_IP}")
        manager.disconnect(websocket)


@router.get("/raspberry/pgm_queue", response_model=PGMQueueListResponse)
async def fetch_raspberry_queue(db: Annotated[AsyncSession, Depends(async_get_db)]):
    """라즈베리파이의 대기열(Queue) 목록을 조회합니다."""
    # RUNNING 및 PENDING 상태인 항목의 전체 개수를 조회합니다.
    count_stmt = select(func.count()).select_from(PGMQueue).where(PGMQueue.status.in_(["RUNNING", "PENDING"]))
    total_count = (await db.execute(count_stmt)).scalar() or 0

    # DB에서 해당 IP 기기의 대기열 목록 중 RUNNING 및 PENDING 상태인 항목을 오래된 순으로 최대 20개 조회합니다.
    stmt = select(PGMQueue).where(PGMQueue.status.in_(["RUNNING", "PENDING"])).order_by(PGMQueue.id.asc()).limit(20)
    result = await db.execute(stmt)
    items = result.scalars().all()

    return {"total_count": total_count, "data": items}


@router.post("/raspberry/pgm_queue", response_model=PGMQueueRead)
async def add_raspberry_queue(payload: PGMQueuePayload, db: Annotated[AsyncSession, Depends(async_get_db)]):
    """라즈베리파이의 대기열(Queue)에 새 테스트를 추가합니다."""
    create_data = PGMQueueCreateInternal(**payload.model_dump())
    return await crud_pgm_queue.create(db=db, object=create_data)


@router.delete("/raspberry/pgm_queue/{item_id}")
async def remove_raspberry_queue(item_id: int, db: Annotated[AsyncSession, Depends(async_get_db)]):
    """라즈베리파이의 대기열(Queue)에서 특정 테스트를 삭제합니다."""
    stmt = delete(PGMQueue).where(PGMQueue.id == item_id, PGMQueue.status != "RUNNING").returning(PGMQueue.id)
    result = await db.execute(stmt)
    await db.commit()

    if not result.first():
        raise HTTPException(status_code=400, detail="Queue item not found or is currently running")

    return {"message": "Success", "id": item_id}


async def _handle_start_action(db: AsyncSession, redis: Redis, target_ip: str) -> dict:
    """Start 명령에 대한 구체적인 처리 로직을 담당합니다."""
    # 현재 실행중인 항목이 있는지 확인 (중복 실행 방지)
    running_stmt = select(PGMQueue.id).where(PGMQueue.status == "RUNNING").limit(1)
    if (await db.execute(running_stmt)).first():
        raise HTTPException(status_code=400, detail="A test is already running")

    # Atomic하게 PENDING -> RUNNING 업데이트 (동시성 문제 방지)
    subq = (
        select(PGMQueue.id)
        .where(PGMQueue.status == "PENDING")
        .order_by(PGMQueue.id.asc())
        .limit(1)
        .with_for_update()
        .scalar_subquery()
    )

    stmt = (
        update(PGMQueue)
        .where(
            PGMQueue.id == subq,
            ~select(PGMQueue.id).where(PGMQueue.status == "RUNNING").exists(),
        )
        .values(status="RUNNING")
        .returning(PGMQueue.id, PGMQueue.name)
    )

    result = await db.execute(stmt)
    await db.commit()
    started_item = result.first()

    if not started_item:
        # 동시성으로 인해 다른 요청이 먼저 작업을 시작했는지 재확인
        if (await db.execute(running_stmt)).first():
            raise HTTPException(status_code=400, detail="A test is already running")
        raise HTTPException(status_code=404, detail="No pending queue items found")

    item_id = started_item.id
    await _publish_event(
        redis,
        target_ip,
        {
            "type": "TEST_STATUS_CHANGED",
            "status": "RUNNING",
            "id": item_id,
            "action": "start",
        },
    )

    try:
        await _enqueue_job(
            "send_pgm_queue_to_pi",
            item_id,
            target_ip,
            settings.PI_PORT,
            started_item.name,
        )
    except Exception:
        # 작업 추가 실패 시 상태 롤백
        rollback_stmt = update(PGMQueue).where(PGMQueue.id == item_id).values(status="PENDING")
        await db.execute(rollback_stmt)
        await db.commit()
        raise

    return {"message": "Test started successfully", "id": item_id}


async def _handle_stop_action(db: AsyncSession, redis: Redis, target_ip: str, action: str) -> dict:
    """Stop 명령에 대한 구체적인 처리 로직을 담당합니다."""
    new_command = await crud_command.create(
        db, object=CommandCreate(target_device_ip=target_ip, command_text=action.upper())
    )
    await _enqueue_job(
        "send_socket_command",
        new_command.id,
        target_ip,
        settings.PI_PORT,
        action.upper(),
    )

    # RUNNING 중인 항목이 있다면 상태를 PENDING으로 원자적 롤백 (Worker 종료 시점과 Race Condition 방지)
    stmt = update(PGMQueue).where(PGMQueue.status == "RUNNING").values(status="PENDING").returning(PGMQueue.id)
    result = await db.execute(stmt)
    await db.commit()
    stopped_item = result.first()

    if stopped_item:
        item_id = stopped_item.id
        await _publish_event(
            redis,
            target_ip,
            {
                "type": "TEST_STATUS_CHANGED",
                "status": "PENDING",
                "id": item_id,
                "action": action,
            },
        )

    return {"message": f"Test {action} command sent"}


@router.post("/raspberry/test/{action}")
async def handle_test_command(
    action: str,
    db: Annotated[AsyncSession, Depends(async_get_db)],
    redis: Annotated[Redis, Depends(async_get_redis)],
):
    """
    라즈베리파이 테스트 프로그램 명령(start, stop)을 처리합니다.
    - start: PENDING 상태 중 가장 오래된 항목을 RUNNING으로 변경하고 라즈베리파이로 전송
    - stop: 현재 실행중인 테스트를 중지 상태로 변경 후 기기에 명령 전송
    """
    if action not in ["start", "stop"]:
        raise HTTPException(status_code=400, detail="Invalid action")

    target_ip = _get_target_ip()

    if action == "start":
        return await _handle_start_action(db, redis, target_ip)
    return await _handle_stop_action(db, redis, target_ip, action)
