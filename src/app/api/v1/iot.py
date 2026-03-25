import asyncio
import json
import logging
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, WebSocket, WebSocketDisconnect
from redis.asyncio import Redis
from sqlalchemy import delete, func, select, update
from sqlalchemy.exc import IntegrityError
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


# --- Helper Functions ---
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
    """웹소켓을 통해 라즈베리파이의 연결 상태가 변경될 때마다 클라이언트에게 상태를 전송합니다."""
    await manager.connect(websocket, target_ip=target_device_ip)
    logger.info(f"WebSocket client connected for {target_device_ip}")

    try:
        # 연결 직후 즉시 한 번 상태를 보내줌 (첫 렌더링용)
        last_status = await check_hardware_status(target_device_ip, settings.PI_PORT)
        await websocket.send_json(last_status)

        # 이후 5초마다 백그라운드에서 상태 체크 후, 변경되었을 때만 전송
        while True:
            await asyncio.sleep(5)
            current_status = await check_hardware_status(target_device_ip, settings.PI_PORT)

            if (
                last_status["piAStatus"] != current_status["piAStatus"]
                or last_status["piBStatus"] != current_status["piBStatus"]
            ):
                await websocket.send_json(current_status)
                last_status = current_status
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")
    except Exception as e:
        logger.error(f"WebSocket Error: {e}")
    finally:
        manager.disconnect(websocket)


@router.websocket("/raspberry/test-status/{target_device_ip}")
async def websocket_test_status_endpoint(
    websocket: WebSocket, target_device_ip: str, redis: Annotated[Redis, Depends(async_get_redis)]
):
    """웹소켓을 통해 라즈베리파이의 큐(테스트) 상태 변화를 클라이언트에게 실시간으로 푸시합니다."""
    await manager.connect(websocket, target_ip=target_device_ip)
    logger.info(f"Test status WebSocket client connected for {target_device_ip}")

    pubsub = redis.pubsub()
    await pubsub.subscribe(f"test-status:{target_device_ip}")

    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                data = message["data"]
                if isinstance(data, bytes):
                    data = data.decode("utf-8")
                await websocket.send_text(data)
    except WebSocketDisconnect:
        logger.info("Test status WebSocket client disconnected")
    except Exception as e:
        logger.error(f"Test status WebSocket Error: {e}")
    finally:
        await pubsub.unsubscribe(f"test-status:{target_device_ip}")
        manager.disconnect(websocket)


@router.get("/raspberry/pgm_queue", response_model=PGMQueueListResponse)
async def fetch_raspberry_queue(target_device_ip: str, db: Annotated[AsyncSession, Depends(async_get_db)]):
    """라즈베리파이의 대기열(Queue) 목록을 조회합니다."""
    # RUNNING 및 PENDING 상태인 항목의 전체 개수를 조회합니다.
    count_stmt = (
        select(func.count())
        .select_from(PGMQueue)
        .where(PGMQueue.status.in_(["RUNNING", "PENDING"]), PGMQueue.target_device_ip == target_device_ip)
    )
    total_count = (await db.execute(count_stmt)).scalar() or 0

    # DB에서 해당 IP 기기의 대기열 목록 중 RUNNING 및 PENDING 상태인 항목을 오래된 순으로 최대 20개 조회합니다.
    stmt = (
        select(PGMQueue)
        .where(PGMQueue.status.in_(["RUNNING", "PENDING"]), PGMQueue.target_device_ip == target_device_ip)
        .order_by(PGMQueue.id.asc())
        .limit(20)
    )
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

    if not result.first():
        await db.rollback()
        raise HTTPException(status_code=400, detail="Queue item not found or is currently running")

    await db.commit()
    return {"message": "Success", "id": item_id}


async def _handle_start_action(db: AsyncSession, redis: Redis, target_ip: str) -> dict:
    """Start 명령에 대한 구체적인 처리 로직을 담당합니다."""
    # 현재 실행중인 항목이 있는지 확인 (중복 실행 방지)
    running_stmt = (
        select(PGMQueue.id).where(PGMQueue.status == "RUNNING", PGMQueue.target_device_ip == target_ip).limit(1)
    )
    if (await db.execute(running_stmt)).first():
        raise HTTPException(status_code=400, detail="A test is already running")

    # Atomic하게 PENDING -> RUNNING 업데이트 (동시성 문제 방지)
    subq = (
        select(PGMQueue.id)
        .where(PGMQueue.status == "PENDING", PGMQueue.target_device_ip == target_ip)
        .order_by(PGMQueue.id.asc())
        .limit(1)
        .with_for_update()
        .scalar_subquery()
    )

    stmt = (
        update(PGMQueue)
        .where(PGMQueue.id == subq)
        .values(status="RUNNING", started_at=func.now())
        .returning(PGMQueue.id, PGMQueue.name)
    )

    try:
        result = await db.execute(stmt)
        await db.commit()
        started_item = result.first()
    except IntegrityError:
        await db.rollback()
        raise HTTPException(status_code=400, detail="A test is already running")

    if not started_item:
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
        rollback_stmt = update(PGMQueue).where(PGMQueue.id == item_id).values(status="PENDING", started_at=None)
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

    # RUNNING 중인 항목이 있다면 상태를 ABORTED으로 원자적 변경 (Worker 종료 시점과 Race Condition 방지)
    stmt = (
        update(PGMQueue)
        .where(PGMQueue.status == "RUNNING", PGMQueue.target_device_ip == target_ip)
        .values(status="ABORTED")
        .returning(PGMQueue.id)
    )
    result = await db.execute(stmt)
    await db.commit()
    stopped_item = result.first()

    if stopped_item:
        item_id = stopped_item.id
        await _publish_event(
            redis,
            target_ip,
            {
                "type": "QUEUE_PAUSED",
                "status": "ABORTED",
                "id": item_id,
                "action": action,
            },
        )

    return {"message": f"Test {action} command sent"}


@router.post("/raspberry/test/{action}")
async def handle_test_command(
    action: str,
    target_device_ip: str,
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

    if action == "start":
        return await _handle_start_action(db, redis, target_device_ip)
    return await _handle_stop_action(db, redis, target_device_ip, action)
