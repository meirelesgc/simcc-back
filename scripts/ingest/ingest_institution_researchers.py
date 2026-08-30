import argparse
import asyncio
import csv
import json
import sys
from pathlib import Path
from typing import Any, Optional
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

# Adiciona raiz do projeto ao path
project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root / 'src'))

from simcc.core.settings import Settings  # noqa: E402


def _read_csv_rows(csv_path: Path) -> list[dict[str, Any]]:
    """Lê todas as linhas do CSV testando múltiplos encodings."""
    if not csv_path.exists():
        raise FileNotFoundError(f'Arquivo não encontrado: {csv_path}')

    encodings = ['utf-8', 'utf-8-sig', 'latin-1', 'iso-8859-1']
    for enc in encodings:
        try:
            with open(csv_path, mode='r', encoding=enc) as f:
                return list(csv.DictReader(f))
        except UnicodeDecodeError:
            continue
    return []


async def _lookup_lattes_mapping(
    session: AsyncSession, rows: list[dict[str, Any]]
) -> dict[str, UUID]:
    """Retorna dicionário mapeando lattes_id -> researcher_id."""
    lattes_list = [
        r['lattes_id'].strip()
        for r in rows
        if r.get('lattes_id') and r['lattes_id'].strip()
    ]
    if not lattes_list:
        return {}

    sql_lookup = (
        'SELECT lattes_id, id FROM researcher '
        'WHERE lattes_id = ANY(:lattes_list);'
    )
    result = await session.execute(
        text(sql_lookup), {'lattes_list': lattes_list}
    )
    return {str(row[0]): row[1] for row in result}


def _extract_researcher_id(
    row: dict[str, Any],
    has_researcher_id: bool,
    has_lattes: bool,
    lattes_to_id: dict[str, UUID],
) -> Optional[UUID]:
    """Resolve o researcher_id a partir da linha do CSV."""
    if has_researcher_id and row.get('researcher_id'):
        try:
            return UUID(str(row['researcher_id']).strip())
        except ValueError:
            pass

    if has_lattes and row.get('lattes_id'):
        lid = str(row['lattes_id']).strip()
        return lattes_to_id.get(lid)

    return None


def _build_record(
    row: dict[str, Any], researcher_id: UUID
) -> dict[str, Any]:
    """Monta o payload de dados para inserção na tabela."""
    zip_code = (
        str(row['zip_code']).strip()
        if row.get('zip_code') and str(row['zip_code']).strip()
        else None
    )
    work_regime = (
        str(row['work_regime']).strip()
        if row.get('work_regime') and str(row['work_regime']).strip()
        else None
    )

    reserved_keys = {
        'name',
        'lattes_id',
        'researcher_id',
        'zip_code',
        'work_regime',
    }
    custom_attrs: dict[str, Any] = {}
    for k, v in row.items():
        if k not in reserved_keys and v is not None:
            val_str = str(v).strip()
            if val_str:
                custom_attrs[k] = val_str

    return {
        'researcher_id': str(researcher_id),
        'zip_code': zip_code,
        'work_regime': work_regime,
        'custom_attributes': json.dumps(custom_attrs),
    }


async def ingest_csv(csv_path: Path, session: AsyncSession) -> dict[str, int]:
    rows = _read_csv_rows(csv_path)
    if not rows:
        print(f'Nenhum registro encontrado no CSV: {csv_path}')
        return {'total': 0, 'upserted': 0, 'ignored': 0}

    fieldnames = list(rows[0].keys())
    has_lattes = 'lattes_id' in fieldnames
    has_researcher_id = 'researcher_id' in fieldnames

    if not has_lattes and not has_researcher_id:
        msg = (
            'O CSV deve conter ao menos a coluna "lattes_id" '
            f'ou "researcher_id". Colunas encontradas: {fieldnames}'
        )
        raise ValueError(msg)

    lattes_to_id = (
        await _lookup_lattes_mapping(session, rows) if has_lattes else {}
    )

    records: list[dict[str, Any]] = []
    ignored = 0

    for row in rows:
        rid = _extract_researcher_id(
            row, has_researcher_id, has_lattes, lattes_to_id
        )
        if not rid:
            ignored += 1
            continue
        records.append(_build_record(row, rid))

    if records:
        upsert_sql = """
            INSERT INTO researcher_institution_data (
                researcher_id, zip_code, work_regime, custom_attributes
            )
            VALUES (
                :researcher_id, :zip_code, :work_regime,
                CAST(:custom_attributes AS jsonb)
            )
            ON CONFLICT (researcher_id) DO UPDATE SET
                zip_code = EXCLUDED.zip_code,
                work_regime = EXCLUDED.work_regime,
                custom_attributes = EXCLUDED.custom_attributes;
        """
        await session.execute(text(upsert_sql), records)
        await session.commit()

    return {'total': len(rows), 'upserted': len(records), 'ignored': ignored}


async def main():
    parser = argparse.ArgumentParser(
        description='Ingestão de dados institucionais de pesquisadores'
    )
    parser.add_argument(
        '--file',
        '-f',
        required=True,
        type=Path,
        help='Caminho do arquivo CSV (ex: storage/researchers/ufrb.csv)',
    )
    args = parser.parse_args()

    settings = Settings()
    engine = create_async_engine(settings.DATABASE_URL, echo=False)
    async_session = sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )

    print(f'==> Iniciando ingestão do arquivo: {args.file}')
    async with async_session() as session:
        metrics = await ingest_csv(args.file, session)

    print('=' * 50)
    print('Relatório de Ingestão:')
    print(f'  - Total de linhas no CSV:     {metrics["total"]}')
    print(f'  - Registros inseridos/upsert: {metrics["upserted"]}')
    print(f'  - Linhas ignoradas sem match: {metrics["ignored"]}')
    print('=' * 50)
    print('==> Ingestão concluída com sucesso!')


if __name__ == '__main__':
    asyncio.run(main())
