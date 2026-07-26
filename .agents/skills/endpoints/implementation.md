# Guia de Implementação de Endpoints

Esta seção contém um modelo passo a passo simplificado para implementar um endpoint abstraído em todas as quatro camadas do SimCC.

---

## 1. Passo a Passo de Codificação

Para criar um novo endpoint analítico (ex: buscar entidades filtradas), siga a ordem de implementação abaixo:

### Passo 1: Definir o Esquema Pydantic (Schema)
Crie os modelos de dados para validação de entrada/saída em `src/simcc/schemas/`.

```python
# src/simcc/schemas/my_entity.py
from pydantic import BaseModel
from uuid import UUID
from typing import Optional

class MyEntityResponse(BaseModel):
    id: UUID
    name: str
    description: Optional[str] = None
    metric: int = 0
```

### Passo 2: Criar a Query SQL Dinâmica (Query Builder)
Crie o construtor de query em `src/simcc/queries/my_entity_query.py` herdando de `BaseQuery`.

```python
# src/simcc/queries/my_entity_query.py
from simcc.queries.base import BaseQuery

class MyEntitySearchQuery(BaseQuery):
    SUPPORTED_FILTERS = {'institution_id', 'term'}

    def __init__(self, session, term: str = None):
        super().__init__(session)
        self.term = term

    def _apply_institution_id_filter(self, value):
        self.params['institution_id'] = value
        self.filters_sql.append(' AND e.institution_id = :institution_id ')

    def build_sql(self) -> str:
        filters_sql = ' '.join(self.filters_sql)
        
        # Filtro textual dinâmico opcional
        if self.term:
            self.params['term_filter'] = f"%{self.term}%"
            filters_sql += ' AND e.name ILIKE :term_filter '

        return f"""
            SELECT e.id, e.name, e.description, COALESCE(e.metric, 0) AS metric
            FROM my_entity e
            WHERE e.status = True
            {filters_sql}
            {self.pagination_sql};
        """
```

### Passo 3: Implementar o Repositório (Repository)
Adicione o método de acesso a dados em `src/simcc/repositories/my_entity_repo.py`.

```python
# src/simcc/repositories/my_entity_repo.py
from simcc.queries.my_entity_query import MyEntitySearchQuery

async def search_entities(session, filters, term: str = None, pagination = None):
    query = MyEntitySearchQuery(session, term=term)
    query.apply_filters(filters)
    query.apply_pagination(pagination)
    return await query.execute()
```

### Passo 4: Implementar o Serviço (Service)
Adicione a lógica de negócio ou enriquecimento em `src/simcc/services/my_entity_service.py`.

```python
# src/simcc/services/my_entity_service.py
from simcc.repositories import my_entity_repo

async def get_my_entities(session, filters, term: str = None, pagination = None):
    # Executa a busca base no repositório
    entities = await my_entity_repo.search_entities(
        session, filters, term=term, pagination=pagination
    )
    
    # Adicione processamento ou enriquecimento extra aqui caso necessário
    
    return entities
```

### Passo 5: Expor a Rota no FastAPI (Router)
Registre o endpoint HTTP em `src/simcc/routers/my_entity.py` e inclua o router no arquivo principal da aplicação `src/simcc/__init__.py`.

```python
# src/simcc/routers/my_entity.py
from fastapi import APIRouter, Query
from simcc.core.dependencies import AsyncSession, Filters
from simcc.schemas.my_entity import MyEntityResponse
from simcc.services import my_entity_service

router = APIRouter(tags=['MyEntity'])

@router.get('/my-entities', response_model=list[MyEntityResponse])
async def list_my_entities(
    session: AsyncSession,
    filters: Filters,
    term: str | None = Query(None)
):
    return await my_entity_service.get_my_entities(
        session, filters, term=term, pagination=filters
    )
```
