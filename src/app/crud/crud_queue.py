from fastcrud import FastCRUD

from ..models.pgm_queue import PGMQueue
from ..schemas.pgm_queue import (
    PGMQueueCreateInternal,
    PGMQueueDelete,
    PGMQueueRead,
    PGMQueueUpdate,
    PGMQueueUpdateInternal,
)

CRUDPGMQueue = FastCRUD[
    PGMQueue, PGMQueueCreateInternal, PGMQueueUpdate, PGMQueueUpdateInternal, PGMQueueDelete, PGMQueueRead
]

crud_pgm_queue = CRUDPGMQueue(PGMQueue)
