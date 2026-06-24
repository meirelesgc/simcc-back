---
name: powerbi-migration
description: Metodologia e aprendizados para migração e refatoração de endpoints de dados do PowerBI no ecossistema Simcc utilizando Polars e camadas assíncronas.
---

# Metodologia e Aprendizados de Migração de Endpoints do PowerBI

Esta skill condensa a metodologia de migração de endpoints legados do PowerBI (antigos scripts baseados em Pandas e conexões síncronas) para a nova arquitetura assíncrona baseada em Polars, além de documentar aprendizados importantes obtidos durante o processo.

---

## 1. Metodologia de Camadas

A arquitetura para novos endpoints segue o padrão de quatro camadas assíncronas:

### 1.1 Camada de Query (`simcc/queries/powerBi_query.py`)
- **Herança:** Toda query deve herdar de `BaseQuery` e sobrescrever o método `build_sql(self) -> str` com o decorador `@override`.
- **Conversão de Tipos (Cast):** Deve-se realizar conversão explícita de tipos no SQL (`::TEXT`, `::INT`, etc.) para garantir consistência e evitar problemas de tipos nulos com o Polars.
- **Exemplo:**
  ```python
  class FatAreaSpecialtyQuery(BaseQuery):
      @override
      def build_sql(self) -> str:
          return """
          SELECT DISTINCT asp.id::TEXT AS area_specialty_id, researcher_id::TEXT,
              asp.name::TEXT AS area_specialty
          FROM researcher_area_expertise r
          INNER JOIN area_specialty asp ON asp.id = r.area_specialty_id;
          """
  ```

### 1.2 Camada de Repositório (`simcc/repositories/powerBi_repo.py`)
- **Assinatura:** Funções assíncronas (`async def`) que aceitam `session` ou `admin_session`.
- **Execução:** Instancia o objeto de query passando a sessão ativa e executa `await query.execute()`.
- **Exemplo:**
  ```python
  async def get_fat_area_specialty(session):
      query = powerBi_query.FatAreaSpecialtyQuery(session)
      return await query.execute()
  ```

### 1.3 Camada de Serviço (`simcc/services/powerBi_service.py`)
- **Assinatura:** Funções assíncronas (`async def`) que recebem as sessões necessárias.
- **Conversão Polars:** Mapeia os dados obtidos usando um esquema Polars rígido (`pl.Utf8`, `pl.Int64`, etc.).
- **Escrita de Arquivos:** Utiliza `write_csv` (ou outros métodos nativos do Polars) gravando no caminho físico definido por `PATH`.
- **Exemplo:**
  ```python
  async def fat_area_specialty(session):
      data = await powerBi_repo.get_fat_area_specialty(session)
      df_schema = {
          'area_specialty_id': pl.Utf8,
          'researcher_id': pl.Utf8,
          'area_specialty': pl.Utf8,
      }
      df = pl.DataFrame(data, schema=df_schema)
      csv_path = os.path.join(PATH, 'fat_area_specialty.csv')
      df.write_csv(csv_path)
  ```

### 1.4 Camada de Roteamento (`APIRouter`)
- **Tag e Schema:** Configurar `APIRouter` com `tags=['Power BI']` e ocultar da documentação pública através de `include_in_schema=False`.
- **Dependência:** Injetar `session: AsyncSession` e/ou `admin_session: AdminAsyncSession`.
- **Retorno:** Retorna o caminho do arquivo físico usando `FileResponse`.
- **Exemplo:**
  ```python
  @router.get('/fat_area_specialty.csv')
  async def fat_area_specialty_csv(session: AsyncSession):
      await powerBi_service.fat_area_specialty(session)
      file_path = os.path.join(STORAGE_PATH, 'fat_area_specialty.csv')
      return FileResponse(file_path, filename='fat_area_specialty.csv')
  ```

---

## 2. Aprendizados Práticos e Cuidados Especiais

### 2.1 Separação de Schemas (Banco de Dados Admin vs. Público)
- O ecossistema possui bases/schemas distintos (`public` e `admin`).
- Queries executadas na `admin_session` devem qualificar **explicitamente** as tabelas com o schema correspondente (ex: `admin.researcher_area`, `admin.areas`, `admin.researcher`).
- Queries na sessão normal (`session`) buscam tabelas públicas (ou o schema público). Tabelas como `guidance_tracking` devem ser qualificadas como `public.guidance_tracking` para evitar ambiguidades de contexto de busca.

### 2.2 SELECT DISTINCT e ORDER BY no PostgreSQL
- Ao usar `SELECT DISTINCT` no PostgreSQL com conversão de tipos (ex: `title::TEXT`), qualquer coluna referenciada na cláusula `ORDER BY` **deve constar exatamente** na lista de seleção.
- Prefira dar um alias explícito para a expressão (ex: `bar.qualis::TEXT AS qualis`) e usar esse alias no `ORDER BY` (ex: `ORDER BY qualis DESC`). Do contrário, ocorrerá o erro: `psycopg.errors.InvalidColumnReference: for SELECT DISTINCT, ORDER BY expressions must appear in select list`.

### 2.3 Utilização de Dollar Quoting (`$$`) para Strings no SQL
- Evite escapar aspas simples e caracteres especiais (como `\`, `'`, `"`) dentro de strings SQL no Python.
- Em vez de `'-\\\".:,;\\''`, utilize a sintaxe de dollar quoting do PostgreSQL: `$$-\".:,;'$$`. Isso torna a query legível e previne erros de parser de string do psycopg.

### 2.4 Remoção da Dependência do Pandas
- O Pandas não está disponível ou não deve ser usado no ambiente web por limitações de memória e performance.
- Substitua lógica de Joins, Merge, Groupby ou Pivot do Pandas utilizando Polars ou dicionários/estruturas nativas do Python antes de construir o `pl.DataFrame`.

### 2.5 Configuração de Testes e Tipos Customizados
- Se a query referenciar tipos customizados (como `routine_type` enum) ou tabelas específicas no banco de dados, certifique-se de adicioná-los no setup de banco de dados nos testes (`tests/conftest.py`).
- Exemplo de criação segura de Enums nos testes:
  ```sql
  DO $$
  BEGIN
      IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'routine_type') THEN
          CREATE TYPE routine_type AS ENUM ('dim_titulacao', 'fat_area_specialty');
      END IF;
  END$$;
  ```
