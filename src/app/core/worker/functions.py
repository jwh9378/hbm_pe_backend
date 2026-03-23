import asyncio
import logging

import uvloop
from arq.worker import Worker

from ...crud.crud_command import crud_command
from ...schemas.command import CommandUpdate
from ..db.database import local_session

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


# -------- background tasks --------
async def sample_background_task(ctx: Worker, name: str) -> str:
    await asyncio.sleep(5)
    return f"Task {name} is complete!"


async def send_socket_command(ctx: Worker, command_id: int, target_ip: str, port: int, command_text: str) -> None:
    """백그라운드에서 소켓 통신을 실행하는 ARQ 워커 함수입니다."""
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
            logging.error(f"Invalid response from {target_ip}:{port} - '{response_text}'")
            final_status = "FAILED"

    except TimeoutError:
        logging.error(f"Timeout while communicating with {target_ip}:{port}")
        final_status = "FAILED"
    except Exception as e:
        logging.error(f"Failed to send command to {target_ip}:{port} - {e}")
        final_status = "FAILED"
    finally:
        if writer:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception as close_error:
                logging.debug(f"Socket close failed for {target_ip}: {close_error}")

        # 백그라운드 작업이므로 요청 스코프와 분리된 새로운 DB 세션을 열어서 업데이트를 수행합니다.
        async with local_session() as db:
            await crud_command.update(db, object=CommandUpdate(status=final_status), id=command_id)


# -------- base functions --------
async def startup(ctx: Worker) -> None:
    logging.info("Worker Started")


async def shutdown(ctx: Worker) -> None:
    logging.info("Worker end")
