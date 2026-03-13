from datetime import datetime

from sqlalchemy import DateTime, String
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func, text

from ..core.db.database import Base


class Command(Base):
    __tablename__ = "command"

    # 기본키 (자동 생성)
    id: Mapped[int] = mapped_column(primary_key=True, index=True, init=False)

    # 라즈베리파이 ip address
    target_device_ip: Mapped[str] = mapped_column(String, server_default=text("'None'"), default="None")

    # 전달할 명령어 (예: "LED on", "LED off")
    command_text: Mapped[str] = mapped_column(String, server_default=text("'None'"), default="None")

    # 명령어 처리 상태
    status: Mapped[str] = mapped_column(String, server_default=text("'PENDING'"), default="PENDING")

    # 생성 시간 기록
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), init=False)
