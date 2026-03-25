import asyncio
import json
import logging
import time

import uvloop
from arq.worker import Worker
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from ...crud.crud_command import crud_command
from ...models.pgm_queue import PGMQueue
from ...schemas.command import CommandUpdate
from ..db.database import local_session

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


# --- Helper Functions ---
async def _publish_event(redis, target_ip: str, payload: dict) -> None:
    """Redis Pub/Sub을 통해 웹소켓 클라이언트로 이벤트를 발행합니다."""
    if redis:
        await redis.publish(f"test-status:{target_ip}", json.dumps(payload))


async def _send_payload(target_ip: str, port: int, payload_str: str, context_info: str = "") -> tuple[str, int | None]:
    """기기로 페이로드를 전송하고 응답을 확인한 뒤 상태와 소요 시간을 반환합니다."""
    start_time = time.time()

    for attempt in range(3):
        writer = None
        try:
            reader, writer = await asyncio.wait_for(asyncio.open_connection(target_ip, port), timeout=5.0)
            writer.write(payload_str.encode("utf-8"))
            await writer.drain()

            data = await asyncio.wait_for(reader.read(1024), timeout=5.0)
            response_text = data.decode("utf-8").strip()

            try:
                response_json = json.loads(response_text)
                if response_json.get("status") != "OK":
                    logging.error(f"Invalid status from {target_ip}:{port} {context_info} - '{response_text}'")
                    return "FAILED", None
            except json.JSONDecodeError:
                # JSON 파싱 실패(부분 수신 등) 시 예외를 발생시켜 재시도 로직을 타게 합니다.
                raise ValueError(f"Invalid non-JSON response: '{response_text}'")

            return "SUCCESS", int(time.time() - start_time)

        except TimeoutError:
            if attempt == 2:
                logging.error(f"Timeout while communicating with {target_ip}:{port} {context_info} after 3 attempts")
                return "FAILED", None
            logging.warning(
                f"Timeout communicating with {target_ip}:{port} {context_info}. Retrying {attempt + 1}/3..."
            )
            await asyncio.sleep(1)
        except Exception as e:
            if attempt == 2:
                logging.error(f"Failed to send payload to {target_ip}:{port} {context_info} after 3 attempts - {e}")
                return "FAILED", None
            logging.warning(
                f"Error communicating with {target_ip}:{port} {context_info} - {e}. Retrying {attempt + 1}/3..."
            )
            await asyncio.sleep(1)
        finally:
            if writer:
                try:
                    writer.close()
                    await writer.wait_closed()
                except Exception as close_error:
                    logging.debug(f"Socket close failed for {target_ip}: {close_error}")

    return "FAILED", None


async def _chain_next_queue_item(db: AsyncSession, redis, target_ip: str, port: int) -> None:
    """다음 PENDING 상태 큐를 찾아 RUNNING으로 원자적 변경 후 작업을 이어서 등록(체이닝)합니다."""
    subq = (
        select(PGMQueue.id)
        .where(PGMQueue.status == "PENDING")
        .order_by(PGMQueue.id.asc())
        .limit(1)
        .with_for_update(skip_locked=True)
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
        next_item = result.first()
    except IntegrityError:
        await db.rollback()
        next_item = None

    if next_item:
        await _publish_event(
            redis,
            target_ip,
            {
                "type": "TEST_STATUS_CHANGED",
                "status": "RUNNING",
                "id": next_item.id,
                "action": "start",
            },
        )
        if redis:
            await redis.enqueue_job(
                "send_pgm_queue_to_pi",
                next_item.id,
                target_ip,
                port,
                next_item.name,
            )


# -------- background tasks --------
async def sample_background_task(ctx: Worker, name: str) -> str:
    await asyncio.sleep(5)
    return f"Task {name} is complete!"


async def send_socket_command(ctx: Worker, command_id: int, target_ip: str, port: int, command_text: str) -> None:
    """백그라운드에서 소켓 통신을 실행하는 ARQ 워커 함수입니다."""
    final_status, _ = await _send_payload(target_ip, port, command_text)

    async with local_session() as db:
        await crud_command.update(db, object=CommandUpdate(status=final_status), id=command_id)


async def send_pgm_queue_to_pi(ctx: Worker, pgm_queue_id: int, target_ip: str, port: int, name: str) -> None:
    """백그라운드에서 큐 데이터를 라즈베리파이로 전송하는 ARQ 워커 함수입니다."""
    payload_str = json.dumps({"action": "PGM_QUEUE", "id": pgm_queue_id, "name": name})
    final_status, duration = await _send_payload(
        target_ip,
        port,
        payload_str,
        context_info=f"for PGM queue {pgm_queue_id}",
    )

    redis = ctx.get("redis")

    async with local_session() as db:
        # 현재 상태가 여전히 RUNNING일 때만 성공/실패 상태로 업데이트 (API의 stop으로 인한 PENDING 롤백과 충돌 방지)
        stmt = (
            update(PGMQueue)
            .where(PGMQueue.id == pgm_queue_id, PGMQueue.status == "RUNNING")
            .values(status=final_status, duration=duration)
            .returning(PGMQueue.id)
        )
        result = await db.execute(stmt)
        await db.commit()
        updated_item = result.first()

        if not updated_item:
            # 이미 PENDING 등으로 롤백되었다면 이벤트를 발행하거나 이어서 실행하지 않고 종료
            return

        status_msg = "COMPLETED" if final_status == "SUCCESS" else "FAILED"
        await _publish_event(
            redis,
            target_ip,
            {
                "type": "TEST_COMPLETED",
                "status": status_msg,
                "id": pgm_queue_id,
            },
        )

        if final_status == "SUCCESS":
            await _chain_next_queue_item(db, redis, target_ip, port)
        else:
            await _publish_event(
                redis,
                target_ip,
                {
                    "type": "QUEUE_PAUSED",
                    "message": "오류로 인해 큐가 일시 정지되었습니다.",
                    "id": pgm_queue_id,
                },
            )


# -------- base functions --------
async def startup(ctx: Worker) -> None:
    logging.info("Worker Started")


async def shutdown(ctx: Worker) -> None:
    logging.info("Worker end")
