from typing import List

from pydantic import BaseModel

from simcc.schemas.researcher import Researcher


class MariaResponse(BaseModel):
    query: str
    researchers: List[Researcher]
