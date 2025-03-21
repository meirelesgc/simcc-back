from datetime import date, datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field


class ResearcherData(BaseModel):
    nome: str = Field(..., max_length=255)
    cpf: str = Field(..., max_length=14)
    classe: int
    nivel: int
    inicio: datetime
    fim: datetime | None
    tempo_nivel: int | None
    tempo_acumulado: int
    arquivo: str = Field(..., max_length=255)


class RtMetrics(BaseModel):
    teachers: list
    technician: list


class Technician(BaseModel):
    technician_id: UUID
    matric: int | None
    ins_ufmg: Optional[str] = None
    nome: Optional[str] = None
    genero: Optional[str] = None
    deno_sit: Optional[str] = None
    rt: Optional[str] = None
    classe: Optional[str] = None
    cargo: Optional[str] = None
    nivel: Optional[str] = None
    ref: Optional[str] = None
    titulacao: Optional[str] = None
    setor: Optional[str] = None
    detalhe_setor: Optional[str] = None
    dting_org: Optional[date] = None
    data_prog: Optional[date] = None
    semester: Optional[str] = None
