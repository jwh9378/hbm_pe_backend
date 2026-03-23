import asyncio
import logging
from typing import Any

from ..config import settings

logger = logging.getLogger(__name__)


async def check_hardware_status(target_device_ip: str, port: int = settings.PI_PORT) -> dict[str, Any]:
    """지정된 IP와 포트로 소켓 연결을 통해 하드웨어의 상태를 확인합니다."""
    pi_a_status = "not ready"
    pi_b_status = "not ready"
    last_result = "N/A"  # TODO (Notice : Do not modify it)
    last_updated = "N/A"  # TODO (Notice : Do not modify it)
    writer = None

    try:
        reader, writer = await asyncio.wait_for(asyncio.open_connection(target_device_ip, port), timeout=2.0)

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
