# Guia de Validação e Testes de Endpoints

Esta seção estabelece o padrão de validação e escrita de testes para endpoints no backend do SimCC.

---

## 1. Princípios de Testabilidade

*   **Banco Limpo por Teste**: O banco de dados de testes roda em um container Docker efêmero (`PostgresContainer`). A transação é iniciada e revertida (`rollback`) automaticamente ao final de cada teste para garantir isolamento absoluto.
*   **Dados Não Duplicados**: Usamos **factory_boy** com sequências numéricas e geradores alfa-numéricos estruturados para que cadastros em lote nos testes nunca violem chaves exclusivas do banco.
*   **Fixtures Dinâmicas**: Nossas fixtures são funções criadoras assíncronas que permitem a inserção flexível e parametrizável de dados sob demanda dentro de cada teste.

---

## 2. Padrão de Escrita de Factories (`tests/factories.py`)

Defina classes herdando de `factory.Factory` associadas aos modelos do banco. Utilize `factory.Sequence` em campos únicos e `factory.Faker` para dados fictícios genéricos:

```python
# tests/factories.py
import factory
from simcc.core.db.model import Researcher

class ResearcherFactory(factory.Factory):
    class Meta:
        model = Researcher

    name = factory.Sequence(lambda n: f"Researcher_{n}")
    lattes_id = factory.Sequence(lambda n: f"{n:016d}")
    lattes_10_id = factory.Sequence(lambda n: f"L{n:010d}")
    orcid = factory.Sequence(lambda n: f"0000-0002-1825-{n:04d}")
    abstract = factory.Faker('paragraph')
```

---

## 3. Padrão de Fixtures Criadoras Assíncronas (`tests/fixtures.py`)

As fixtures devem ser decoradas com `@pytest_asyncio.fixture` e retornar uma função assíncrona interna (ex: `_create_researcher`) que gera a entidade via factory, insere no banco (`session.add`), sincroniza os IDs gerados pelo banco (`await session.flush()`) e resolve dependências ausentes automaticamente:

```python
# tests/fixtures.py
import pytest_asyncio
from tests.factories import ResearcherFactory, InstitutionFactory

@pytest_asyncio.fixture
def create_institution(session):
    async def _create_institution(**kwargs):
        institution = InstitutionFactory.build(**kwargs)
        session.add(institution)
        await session.flush()
        return institution
    return _create_institution

@pytest_asyncio.fixture
def create_researcher(session, create_institution):
    async def _create_researcher(**kwargs):
        # Resolve dependência automaticamente caso não informada
        if 'institution_id' not in kwargs:
            inst = await create_institution()
            kwargs['institution_id'] = inst.id
            
        researcher = ResearcherFactory.build(**kwargs)
        session.add(researcher)
        await session.flush()
        return researcher
    return _create_researcher
```

---

## 4. Estrutura Padrão dos Arquivos de Teste (`tests/routers/`)

*   **Localização**: Crie os testes dentro de subdiretórios apropriados em `tests/routers/` (ex: `tests/routers/test_researcher.py`), de forma a espelhar a estrutura do código de produção.
*   **Padrão de escrita**: Sempre marque testes de integração assíncronos com `@pytest.mark.asyncio`.

```python
# tests/routers/test_researcher.py
from http import HTTPStatus
import pytest

@pytest.mark.asyncio
async def test_get_researchers_empty(client):
    """Garante retorno vazio (HTTP 200 OK) quando não há dados no banco."""
    response = client.get('/researcherName')
    assert response.status_code == HTTPStatus.OK
    assert response.json() == []

@pytest.mark.asyncio
async def test_get_researchers_with_data(client, create_researcher):
    """Valida o mapeamento e exibição de dados ao inserir registros com fixtures."""
    # 1. Cria entidade dinamicamente usando parâmetros sob medida
    res_1 = await create_researcher(name="João Silva")
    res_2 = await create_researcher(name="Maria Souza") # lattes_id gerado unicamente via factory sequence
    
    # 2. Faz requisição HTTP de teste
    response = client.get('/researcherName')
    assert response.status_code == HTTPStatus.OK
    
    # 3. Asserções
    data = response.json()
    assert len(data) == 2
    assert data[0]['name'] == "João Silva"
    assert data[1]['name'] == "Maria Souza"
```
