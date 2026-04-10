import asyncio
import logging
from typing import Any
from zoneinfo import ZoneInfo

from sqlalchemy import select

from ...models.pgm_queue import PGMQueue
from ..config import settings
from ..db.database import local_session
from .protocol import cmd_health_check, decode_json_line, encode_json_line, health_check_response

logger = logging.getLogger(__name__)


async def check_hardware_status(target_device_ip: str, port: int = settings.PI_PORT) -> dict[str, Any]:
    """지정된 IP와 포트로 소켓 연결을 통해 하드웨어의 상태를 확인합니다."""
    pi_a_status = "not ready"
    pi_b_status = "not ready"
    last_result = "N/A"
    last_updated = "N/A"
    writer = None

    # 가장 최근에 실행 종료된(COMPLETED / ABORTED / ERROR) 테스트 결과 조회
    try:
        async with local_session() as db:
            stmt = (
                select(PGMQueue)
                .where(
                    PGMQueue.status.in_(["COMPLETED", "ABORTED", "ERROR"]),
                    PGMQueue.target_device_ip == target_device_ip,
                )
                .order_by(PGMQueue.id.desc())
                .limit(1)
            )
            result = await db.execute(stmt)
            last_test = result.scalars().first()

            if last_test:
                last_result = last_test.status
                last_updated = (
                    last_test.started_at.astimezone(ZoneInfo("Asia/Seoul")).strftime("%Y-%m-%d %H:%M:%S")
                    if last_test.started_at
                    else "N/A"
                )
    except Exception as e:
        logger.debug(f"Failed to fetch last test result from DB: {e}")

    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(target_device_ip, port), timeout=2.0)

        writer.write(encode_json_line(cmd_health_check()))
        await writer.drain()

        data = await asyncio.wait_for(reader.read(1024), timeout=1.5)
        response = decode_json_line(data)

        pi_a_status = "ready"
        pi_b_status = health_check_response(response)

    except Exception as e:
        logger.debug(f"Health check failed for {target_device_ip}: {e}")
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
