# src/lakehouse_engine/ingestion/models.py
from typing import Annotated, Final

from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    JsonValue,
    StrictInt,
    StrictStr,
    TypeAdapter,
)

INT64_MAX: Final[int] = 2**63 - 1


class Event(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    event_id: Annotated[StrictInt, Field(gt=0, le=INT64_MAX)]
    user_id: Annotated[StrictInt, Field(ge=0, le=INT64_MAX)]
    event_name: Annotated[StrictStr, Field(min_length=1, max_length=256)]
    event_ts: AwareDatetime
    payload: dict[str, JsonValue] | None = None


# Instantiated ONCE at import time. Never construct a TypeAdapter inside a call path.
EVENT_ADAPTER: Final[TypeAdapter[Event]] = TypeAdapter(Event)
EVENT_LIST_ADAPTER: Final[TypeAdapter[list[Event]]] = TypeAdapter(list[Event])
