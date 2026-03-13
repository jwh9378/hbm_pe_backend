from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class CommandBase(BaseModel):
    target_device_ip: str
    command_text: str


class CommandCreate(CommandBase):
    pass


class CommandUpdate(BaseModel):
    command_text: Optional[str] = None
    status: Optional[str] = None


class CommandRead(CommandBase):
    id: int
    target_device_ip: str
    status: str
    created_at: datetime

    class Config:
        from_attributes = True
