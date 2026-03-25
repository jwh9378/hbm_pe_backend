from datetime import datetime

from sqlalchemy import DateTime, Index, Integer, String, func, text
from sqlalchemy.orm import Mapped, mapped_column

from ..core.db.database import Base


class PGMQueue(Base):
    __tablename__ = "pgm_queues"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, init=False)
    target_device_ip: Mapped[str] = mapped_column(String(50))
    name: Mapped[str] = mapped_column(String(100))
    duration: Mapped[int | None] = mapped_column(Integer, nullable=True, default=None)
    status: Mapped[str] = mapped_column(String(50), server_default=text("'PENDING'"), default="PENDING")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), init=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True, default=None)

    __table_args__ = (
        Index(
            "ix_pgm_queues_unique_running",
            "target_device_ip",
            "status",
            unique=True,
            sqlite_where=text("status = 'RUNNING'"),
            postgresql_where=text("status = 'RUNNING'"),
        ),
    )
