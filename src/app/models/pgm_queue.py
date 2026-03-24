from datetime import datetime

from sqlalchemy import DateTime, Integer, String, func, text
from sqlalchemy.orm import Mapped, mapped_column

from ..core.db.database import Base


class PGMQueue(Base):
    __tablename__ = "pgm_queues"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True, init=False)
    name: Mapped[str] = mapped_column(String(100))
    duration: Mapped[int | None] = mapped_column(Integer, nullable=True, default=None)
    status: Mapped[str] = mapped_column(String(50), server_default=text("'PENDING'"), default="PENDING")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), init=False)
