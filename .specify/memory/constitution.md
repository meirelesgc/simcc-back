<!--
SYNC IMPACT REPORT
==================
Version change: 1.0.0 → 1.1.0 (MINOR: adição de dois novos princípios)
Added sections:
  - Princípio VIII: Git Worktrees para Specs Grandes e Trabalho Multi-Agente
  - Princípio IX: Commits Atômicos em Português e Fluxo de Integração via develop
  - Seção "Fluxo de Especificações" expandida com critérios de classificação de spec
Modified sections:
  - "Fluxo de Desenvolvimento" expandido com regras de classificação de spec e worktree
Follow-up TODOs:
  - RATIFICATION_DATE: marcado como data de criação (2026-08-29); data real de adoção
    do projeto deve ser verificada e atualizada.
-->

# SIMCC Constitution

## Core Principles

### I. Arquitetura em Camadas (NON-NEGOTIABLE)

O projeto MUST adotar estritamente a separação em quatro camadas:
**Routers → Services → Repositories → Query Objects**.

- **Routers** (`src/simcc/routers/`): Responsáveis exclusivamente por gerenciar rotas
  HTTP, validar entrada via Pydantic e injetar dependências via FastAPI DI.
  Nenhuma lógica de negócio ou acesso a dados é permitida aqui.
- **Services** (`src/simcc/services/`): Orquestram a lógica de negócio e coordenam
  chamadas aos repositories. MUST ser a única camada a chamar IA/LLM.
- **Repositories** (`src/simcc/repositories/`): Camada fina de acesso a dados;
  delegam queries complexas e dinâmicas para os Query Objects. Não devem conter
  SQL inline complexo; use `text()` apenas para queries simples e pontuais.
- **Query Objects** (`src/simcc/queries/`): Classes especializadas em construir queries
  SQL dinâmicas, herdando de `BaseQuery`. MUST implementar `build_sql()`, usar
  `self.joins` (dict) para evitar duplicação de JOINs e `self.params` para prevenir
  SQL Injection.

**Rationale**: Esta arquitetura garante testabilidade isolada (Tier A), rastreabilidade
de bugs e escalabilidade sem "classes deus".

---

### II. Padrão Query Object com SQL Seguro

Toda query dinâmica complexa MUST seguir o padrão `BaseQuery`:

- Filtros MUST ser implementados como métodos `_apply_<nome>_filter(value)`.
- JOINs MUST ser armazenados em `self.joins` (dict com chave única) para evitar
  duplicações quando múltiplos filtros ativam o mesmo JOIN.
- Parâmetros MUST ser nomeados em `self.params`; interpolação direta de strings
  é PROIBIDA para evitar SQL Injection.
- JOINs iniciam como `LEFT JOIN` e são promovidos para `INNER JOIN` apenas quando
  o filtro da tabela associada está ativo.
- O limite de segurança de paginação é de **500 itens por página** (`PaginationParams.lenght`,
  validado via Pydantic com `le=500`).

**Rationale**: SQL dinâmico sem parametrização é uma vulnerabilidade crítica.
O padrão centraliza a lógica de filtros e facilita testes unitários sem banco (Tier A).

---

### III. Banco de Dados: Schema Versionado e Async

- O banco de dados MUST ser gerenciado exclusivamente via **Alembic** com migrações versionadas.
- Toda alteração de schema (adição/remoção de coluna, tabela ou índice) MUST gerar
  uma migration com `alembic revision --autogenerate -m "<descrição>"`.
- O script gerado MUST ser revisado manualmente antes de aplicar, pois `autogenerate`
  não detecta triggers, extensões (`unaccent`, `vector`), tipos ENUM ou índices parciais.
- Toda conexão com o banco MUST usar drivers assíncronos (`asyncpg` para SQLAlchemy,
  `psycopg3` para Alembic).
- O banco MUST ter as extensões `unaccent` e `vector` (pgvector) ativas em todos
  os ambientes, incluindo contêineres de teste.
- As migrations MUST ser automaticamente aplicadas via `alembic upgrade head` no
  `entrypoint.sh` antes de iniciar a aplicação.

**Rationale**: O uso de drivers síncronos em rotas async causaria deadlocks. Migrações
não versionadas impossibilitam rollbacks e rastreabilidade do schema em produção.

---

### IV. Camada de IA Desacoplada por Interface

- Toda funcionalidade de IA/LLM MUST utilizar as interfaces abstratas `LLMProvider`
  ou `EmbeddingsProvider` definidas em `src/simcc/ai/providers/base.py`.
- Novos modelos ou provedores (ex.: Anthropic, Google Gemini) MUST herdar dessas
  interfaces; chamadas diretas a SDKs externos fora da camada `providers/` são PROIBIDAS.
- Prompts MUST residir em `src/simcc/ai/prompts/`; prompts inline em services ou
  routers são PROIBIDOS.
- Injeção de IA em services MUST ser feita via FastAPI Dependency Injection, configurada
  em `src/simcc/ai/dependencies.py`.
- Em testes automatizados, chamadas reais a APIs de IA são PROIBIDAS; MUST usar
  `MagicMock(spec=LLMProvider)` e `AsyncMock`. Testes que consomem tokens reais
  MUST ser marcados com `@pytest.mark.ai_live` e excluídos da execução padrão.

**Rationale**: O desacoplamento via interface permite substituição de provedor sem
alterar lógica de negócio e garante custo zero em testes automatizados.

---

### V. Observabilidade Estruturada com structlog

- Todos os eventos de negócio, HTTP, rotinas e banco de dados MUST ser registrados
  via funções semânticas do módulo `src/simcc/core/logging/events.py`
  (ex.: `request_received`, `routine_started`, `query_error`).
- Logs MUST ser emitidos via `structlog` com campos de contexto padronizados:
  `request_id`, `route`, `method`, `duration`, `category`, `user_id`.
- O `request_id` MUST ser propagado via header `X-Request-ID` em todas as requisições
  (gerado pelo `LoggingMiddleware` se ausente).
- SQL MUST ser logado apenas no nível `DEBUG` e NUNCA em produção.
- Logs são retidos por **7 dias** por padrão (`LOG_RETENTION_DAYS`), com limpeza
  automática executada no startup da aplicação.
- `print()` é PROIBIDO para observabilidade; use sempre `structlog.get_logger()`.

**Rationale**: Logs não estruturados impossibilitam correlação de requisições em
produção e diagnóstico de incidentes em pipelines distribuídos.

---

### VI. Qualidade de Código Obrigatória com Ruff

- Todo código MUST passar nas verificações do **Ruff** antes de ser commitado.
- Configuração canônica em `pyproject.toml`: `line-length = 79`, aspas simples
  (`quote-style = 'single'`), regras ativas: `I` (isort), `F` (Pyflakes), `E`/`W`
  (pycodestyle), `PL` (Pylint), `PT` (pytest-style).
- O diretório `migrations/` está excluído das regras de lint para preservar templates
  gerados pelo Alembic; esta exclusão MUST ser mantida.
- Commits com falha de lint são BLOQUEADOS; o pipeline de CI MUST executar
  `ruff check .` e `ruff format --check .` como etapa obrigatória.

**Rationale**: Estilo inconsistente aumenta o custo cognitivo de code review.
A configuração com aspas simples e 79 chars reflete a convenção estabelecida da equipe.

---

### VII. Estratégia de Testes em Dois Tiers

- **Tier A — Unidade de SQL (sem banco)**: Testes em `tests/queries/` validam a
  geração de SQL e a população de `self.params` instanciando Query Objects com
  `session=None`. MUST ser rápidos, determinísticos e não requerem infraestrutura.
  Marcados com `@pytest.mark.unit`.
- **Tier B — Integração Semântica (com banco real)**: Testes em `tests/routers/`
  utilizam `testcontainers` para subir PostgreSQL real com as extensões `unaccent`
  e `vector` ativas, garantindo identidade com o ambiente de produção.
  Marcados com `@pytest.mark.integration`.
- Factories MUST usar `factory.Sequence` para campos com restrição `UNIQUE`
  (ex.: `lattes_id`, nomes de países) para evitar `UniqueViolation` em execuções
  paralelas.
- `pytest -m 'not ai_live'` é o modo padrão de execução (configurado em `pyproject.toml`).
- Testes MUST ser executados antes de qualquer commit; a pipeline CI MUST falhar
  se a cobertura regredir em módulos críticos (`routers`, `services`, `queries`).

**Rationale**: Testes de integração puros são lentos e acoplados; Tier A isola a
lógica de construção de SQL sem overhead de contêiner. Tier B valida o comportamento
real incluindo normalização textual (`unaccent`) que não pode ser mockada.

---

### VIII. Git Worktrees para Specs Grandes e Trabalho Multi-Agente

Uma spec é classificada como **grande** quando satisfaz ao menos um dos critérios abaixo:
- Envolve **3 ou mais camadas** da arquitetura simultaneamente
  (ex.: novo modelo + queries + service + router + testes).
- Estimativa de **mais de 10 tarefas** no `tasks.md`.
- Envolve **múltiplos agentes** trabalhando em paralelo.
- Impacta **2 ou mais domínios** distintos do projeto (ex.: `researcher` + `graduate_program`).

Para specs grandes, o fluxo MUST ser:

1. Criar um **git worktree** dedicado a partir da branch `develop`:
   ```bash
   git worktree add ../simcc-back-<nome-da-spec> -b feat/<nome-da-spec> develop
   ```
2. Todo o trabalho da spec MUST ocorrer dentro do worktree isolado; a branch `develop`
   principal MUST permanecer estável durante o desenvolvimento.
3. Agentes paralelos MUST operar em worktrees distintos; NUNCA compartilhar o mesmo
   worktree entre agentes concorrentes.
4. O worktree MUST ser removido após o merge:
   ```bash
   git worktree remove ../simcc-back-<nome-da-spec>
   ```
5. Specs pequenas (abaixo dos critérios acima) PODEM ser desenvolvidas diretamente
   na branch `feat/<nome>` sem worktree.

**Rationale**: Worktrees permitem que múltiplos agentes ou desenvolvedores trabalhem
em paralelo em specs independentes sem poluir o working directory principal. Eliminam
conflitos de contexto e preservam a estabilidade de `develop` durante implementações longas.

---

### IX. Commits Atômicos em Português e Fluxo via develop

- Todo commit MUST ser **atômico**: uma unidade lógica coesa e independentemente
  reversível. Commits que misturam refatorações, features e correções em um único
  `git commit` são PROIBIDOS.
- Mensagens de commit MUST ser escritas em **português**, seguindo o formato
  convencional:
  ```
  <tipo>: <descrição imperativa e objetiva no presente>

  [corpo opcional: contexto, motivação, impacto]
  [rodapé opcional: refs, breaking changes]
  ```
- **Tipos permitidos**:
  | Tipo | Uso |
  |---|---|
  | `feat` | Nova funcionalidade ou endpoint |
  | `fix` | Correção de bug |
  | `refactor` | Refatoração sem mudança de comportamento |
  | `test` | Adição ou correção de testes |
  | `docs` | Documentação, constitution, specs |
  | `chore` | Infraestrutura, dependências, configuração |
  | `migration` | Migrações de banco de dados |
  | `style` | Formatação, lint (sem mudança lógica) |

- **Exemplos válidos**:
  ```
  feat: adiciona endpoint de busca de pesquisadores por área de especialidade
  fix: corrige paginação duplicada no ResearcherSearchQuery
  migration: adiciona coluna extra_field na tabela researcher
  test: adiciona Tier B para filtro de cidade com múltiplos valores
  ```
- O destino de merge de toda feature MUST ser a branch **`develop`**;
  merges diretos em `main` ou `master` são PROIBIDOS sem passar por `develop`.
- Pull Requests MUST referenciar a spec ou issue correspondente na descrição.
- `git push --force` em branches compartilhadas (`develop`, `main`) é PROIBIDO.
  Em branches pessoais de feature, use `--force-with-lease` se necessário.

**Rationale**: Commits atômicos em português reduzem o custo de leitura do histórico
para toda a equipe e facilitam bisect, rollback e geração de changelogs automáticos.
O fluxo via `develop` garante um ponto de integração estável antes da promoção à produção.

---

## Padrões de Qualidade e Estilo

### Contratos de API

- Toda rota MUST declarar `response_model` com um schema Pydantic explícito.
- Rotas legadas MUST ser mantidas com `include_in_schema=False` para compatibilidade
  retroativa com clientes existentes.
- Paginação MUST utilizar `PaginationParams` (`page`, `lenght`, máximo de 500 itens).
- Ordenação MUST utilizar `SortParams` (`sort_by`, `sort_order: asc|desc`).

### Modelos de Banco de Dados

- Todos os modelos SQLAlchemy MUST usar o decorator `@table_registry.mapped_as_dataclass`
  e residir em `src/simcc/core/db/models/`.
- PKs MUST usar `UUID` gerado via `gen_random_uuid()` como `server_default`.
- Chaves estrangeiras com deleção em cascata MUST declarar `ondelete='CASCADE'`.
- Novos modelos MUST ser exportados via `src/simcc/core/db/models/__init__.py`
  para que o `table_registry` os registre e o Alembic os detecte.

### Configuração e Segredos

- Toda configuração MUST ser definida em `src/simcc/core/settings.py` via
  `pydantic-settings`; variáveis de ambiente não tipadas ou acessadas via
  `os.environ` diretamente são PROIBIDAS.
- Segredos (`OPENAI_API_KEY`, `INTERNAL_API_KEY`, `LOG_STREAM_TOKEN`) MUST ser
  `Optional[str]` com valor padrão `None`; a aplicação MUST iniciar sem eles
  (funcionalidades dependentes SHOULD ser desabilitadas graciosamente).
- O arquivo `.env` MUST ser incluído no `.gitignore` e nunca commitado.

---

## Fluxo de Desenvolvimento

### Adicionando uma Nova Feature

**0. Classificar a spec** (ver Princípio VIII):
   - Se a spec for **grande** (≥ 3 camadas, ≥ 10 tarefas, multi-agente ou multi-domínio):
     ```bash
     git worktree add ../simcc-back-<nome-da-spec> -b feat/<nome-da-spec> develop
     ```
   - Se a spec for **pequena**: criar branch `feat/<nome>` normalmente a partir de `develop`.

1. Criar ou alterar o modelo SQLAlchemy em `src/simcc/core/db/models/`.
2. Gerar e revisar a migration com `alembic revision --autogenerate`.
3. Criar o schema Pydantic de entrada/saída em `src/simcc/schemas/`.
4. Implementar o Query Object em `src/simcc/queries/` (se houver SQL dinâmico).
5. Implementar o repository em `src/simcc/repositories/`.
6. Implementar a lógica de negócio no service em `src/simcc/services/`.
7. Expor o endpoint no router em `src/simcc/routers/` com `response_model` declarado.
8. Escrever testes Tier A (queries) e/ou Tier B (routers) conforme aplicável.
9. Executar `ruff check . --fix && ruff format .` antes de commitar.
10. Fazer commits atômicos em português seguindo o Princípio IX:
    ```bash
    git commit -m "feat: adiciona <descrição objetiva>"
    ```
11. Fazer merge para `develop`; remover o worktree se foi criado um.

### Deploy

- O deploy é realizado via Docker Compose com `compose.yaml`.
- O `entrypoint.sh` executa `alembic upgrade head` automaticamente antes de
  iniciar a API com `fastapi run --workers 4`.
- Deploy em produção é acionado via `poetry run task deploy` (SSH + pull + rebuild).

---

## Governance

Esta constituição representa o acordo técnico da equipe do SIMCC e DEVE ser
consultada em toda decisão arquitetural relevante.

**Processo de Emenda**:
- Qualquer princípio pode ser emendado por decisão da equipe técnica, documentada
  neste arquivo com incremento de versão semântica.
- Adição de novo princípio ou seção: incremento MINOR (ex.: 1.0.0 → 1.1.0).
- Remoção ou redefinição de princípio existente: incremento MAJOR (ex.: 1.0.0 → 2.0.0).
- Clarificações e ajustes de redação: incremento PATCH (ex.: 1.0.0 → 1.0.1).
- Toda emenda MUST incluir atualização de `LAST_AMENDED_DATE`.

**Conformidade**:
- PRs e code reviews MUST verificar conformidade com esta constituição.
- Violações de princípios NON-NEGOTIABLE (I, II, IV) MUST bloquear merge.
- Violações dos demais princípios SHOULD ser sinalizadas e tratadas antes do merge.
- Commits fora do formato definido no Princípio IX MUST ser corrigidos antes do merge.
- Esta constituição DEVE ser lida por todo novo membro da equipe antes do primeiro PR.

**Version**: 1.1.0 | **Ratified**: 2026-08-29 | **Last Amended**: 2026-08-29
