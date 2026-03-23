from fastcrud import FastCRUD

from ..models.command import Command
from ..schemas.command import CommandCreate, CommandRead, CommandUpdate

CRUDCommand = FastCRUD[
    Command,
    CommandCreate,
    CommandUpdate,
    CommandUpdate,
    CommandUpdate,
    CommandRead,
]
crud_command = CRUDCommand(Command)
