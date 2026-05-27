# Como Adicionar Filtros

A adição de novos filtros segue um fluxo declarativo para manter a consistência entre a API e a query SQL.

## Passo a Passo

### 1. Atualizar o Schema
Adicione o novo campo no arquivo `src/simcc/schemas/__init__.py` na classe `DefaultFilters` (ou no schema de filtros específico da rota).

```python
class DefaultFilters(PaginationParams, BaseModel):
    # ...
    novo_filtro: Optional[str] = None
```

### 2. Registrar no Query Object
No arquivo da query desejada (ex: `src/simcc/queries/researcher_query.py`), adicione o nome do campo ao conjunto `SUPPORTED_FILTERS`.

```python
SUPPORTED_FILTERS = {
    # ...
    "novo_filtro"
}
```

### 3. Implementar a Lógica de Aplicação
Crie um método privado na classe seguindo o padrão `_apply_<nome_do_filtro>_filter(self, value)`.

```python
def _apply_novo_filtro_filter(self, value):
    # Registrar parâmetro
    self.params['novo_filtro'] = value
    # Adicionar snippet SQL
    self.filters_sql.append(" AND r.coluna = :novo_filtro")
```

### 4. Gerenciar Joins (Se necessário)
Se o filtro depender de uma tabela que não está no `FROM` base, gerencie o join dinamicamente no dicionário `self.joins`.

```python
def _apply_novo_filtro_filter(self, value):
    self.joins['tabela_extra'] = "INNER JOIN tabela_extra te ON te.researcher_id = r.id"
    self.params['novo_filtro'] = value
    self.filters_sql.append(" AND te.coluna = :novo_filtro")
```
