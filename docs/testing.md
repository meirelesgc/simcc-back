# Testes e Boas Práticas

Nossa estratégia de testes é baseada em **semântica** e **confiabilidade de dados**. Como o projeto lida com grandes volumes de dados e buscas complexas, dividimos os testes para garantir performance e correção.

## Estratégia de Duas Camadas

### Tier A: Unidade de Lógica SQL
Localizados em `tests/queries/`, validam se o `Query Object` gera os snippets de SQL corretos e popula os parâmetros adequadamente. 

- **Por que?** Evita o overhead de subir um banco de dados para testar concatenação de strings e lógica de filtros.
- **Como funciona?** Instanciamos a classe de query com `session=None`, chamamos `apply_filters()` e inspecionamos o resultado de `build_sql()`.

### Tier B: Integração Semântica
Localizados em `tests/routers/`, validam o comportamento real da API.
- **Busca Textual**: Garantir que o Postgres TSVECTOR ignore acentos e trate stemming corretamente.
- **Integridade Relacional**: Validar se os filtros de JOIN (cidade, instituição) funcionam no DB real.
- **Setup**: Utiliza `testcontainers` para subir um Postgres real, garantindo que as extensões (`unaccent`) e o schema estejam idênticos ao de produção.

---

## Mecânica de Queries

O sistema de busca utiliza o padrão **Query Object**. Todas as buscas complexas herdam de `BaseQuery`.

### Estrutura de uma Query
1.  **`SUPPORTED_FILTERS`**: Conjunto de strings que define quais filtros a query aceita.
2.  **`_apply_<nome>_filter`**: Métodos privados chamados automaticamente por `apply_filters`. É aqui que a lógica de JOIN e WHERE reside.
3.  **`self.params`**: Dicionário de parâmetros nomeados para evitar SQL Injection.
4.  **`self.joins`**: Dicionário de joins. Usar um dicionário permite que diferentes filtros ativem o mesmo JOIN sem duplicá-lo no SQL final.

### Exemplo de Implementação de Filtro
```python
def _apply_institution_filter(self, value):
    # Força o JOIN a ser INNER em vez de LEFT se o filtro estiver presente
    self.joins['institution'] = 'INNER JOIN institution i ON i.id = r.institution_id'
    self.params['institution'] = value.split(';')
    self.filters_sql.append(' AND i.name = ANY(:institution)')
```

---

## Como Adicionar um Novo Teste

### 1. Testando Lógica de Query (Tier A)
Ao adicionar um novo filtro ou mudar a lógica de uma busca:
1. Crie ou edite o arquivo em `tests/queries/test_<sua_query>_query.py`.
2. Teste se o SQL gerado contém as cláusulas esperadas.
3. Teste se os `self.params` estão sendo populados corretamente.

```python
def test_my_new_filter():
    query = MyQuery(session=None)
    query.apply_filters(DefaultFilters(my_field="valor"))
    sql = query.build_sql()
    assert "AND table.col = :my_field" in sql
    assert query.params["my_field"] == "valor"
```

### 2. Testando Rotas e Filtros (Tier B)
Para garantir que a integração com o banco está correta (especialmente para filtros complexos como `ANY`, `&&` ou busca textual):
1. Use o fixture `client` (FastAPI TestClient) e `session` (AsyncSession).
2. Utilize as **Factories** para popular o banco de teste.
3. Faça a chamada via `client.get`.

```python
@pytest.mark.asyncio
async def test_route_with_complex_filter(client, session):
    # Cria os dados necessários
    researcher = await create_researcher_with_full_graph(session)
    
    # Executa a requisição
    response = client.get("/researchers", params={"city": "Belo Horizonte"})
    
    assert response.status_code == 200
    assert len(response.json()) > 0
```

---

## Complexidade de Filtros Padrão

Alguns filtros possuem comportamentos específicos que devem ser observados:

- **Filtros de Lista (Arrays)**: Filtros como `institution`, `city` e `graduation` aceitam múltiplos valores separados por `;`. No SQL, isso é convertido para `ANY(:param)`.
  - *Exemplo*: `city=Belo Horizonte;Curitiba` filtra por ambas as cidades.
- **Busca Semântica (Websearch)**: Filtros de termo (`term`) utilizam `tools.websearch_filter`, que suporta operadores lógicos especiais:
  - `;` representa `AND`.
  - `|` representa `OR`.
  - `.` representa `NOT`.
  - *Exemplo*: `machine|learning` busca por um ou outro.
- **Joins Dinâmicos**: Para manter a performance, as queries começam com `LEFT JOIN`. Se um filtro de uma tabela relacionada for aplicado, a query automaticamente promove o join para `INNER JOIN`. Isso deve ser validado no Tier A.
- **Busca Textual e Normalização**: Utilizamos `unaccent` e `translate` para normalizar strings no Postgres. Sempre teste isso no Tier B, inserindo dados com acentos e buscando por termos sem acentos.

## Boas Práticas com Factories

### Composite Factories
Utilize o helper `create_researcher_with_full_graph(session)` em `tests/factories.py` para criar cenários complexos de teste com uma única chamada. Ele cria automaticamente Pesquisador, Instituição, Cidade, Publicações e Revistas vinculadas.

### Integridade de Dados Relacionais
Para campos com restrição `UNIQUE` (como nomes de países, códigos ou Lattes ID), utilize sempre `factory.Sequence` em vez de Faker aleatório para evitar erros de `UniqueViolation` durante a execução paralela de testes.

```python
class CountryFactory(factory.Factory):
    name = factory.Sequence(lambda n: f'Country {n}')
```

## Configuração do Banco de Testes
O setup automático (`tests/conftest.py`) cria o banco utilizando Containers. É **obrigatório** garantir que a extensão `unaccent` esteja habilitada, pois ela é fundamental para a normalização de buscas textuais.
