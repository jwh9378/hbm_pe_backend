from datetime import datetime
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field


class PGMQueuePayload(BaseModel):
    target_device_ip: Annotated[str, Field(examples=["192.168.0.10"])]
    name: Annotated[str, Field(examples=["Test 1"])]


class PGMQueueCreateInternal(PGMQueuePayload):
    pass


class PGMQueueUpdate(BaseModel):
    name: str | None = None
    duration: int | None = None
    status: str | None = None


class PGMQueueUpdateInternal(PGMQueueUpdate):
    pass


class PGMQueueDelete(BaseModel):
    pass


class PGMQueueRead(PGMQueueCreateInternal):
    model_config = ConfigDict(from_attributes=True)

    id: int
    created_at: datetime
    started_at: datetime | None = None
    status: str
    duration: int | None = None


class PGMQueueListResponse(BaseModel):
    total_count: int
    data: list[PGMQueueRead]
