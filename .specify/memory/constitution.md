<!--
Sync Impact Report:
- Version Change: 0.0.0 (Unratified Template) → 1.0.0 (Ratified)
- Modified Principles:
  - PRINCIPLE_1: Preservação de Especificações Legadas e Evolução Criteriosa
  - PRINCIPLE_2: Isolamento Obrigatório em Git Worktrees e Branch Develop
  - PRINCIPLE_3: Commits Granulares, Descritivos e em Português
  - PRINCIPLE_4: Metodologia de Testes Orientada a Risco e Camadas
  - PRINCIPLE_5: Observabilidade e Logging Estruturado Rigoroso (JSONL)
  - PRINCIPLE_6: Documentação Humanizada e Viva com MkDocs
- Added Sections:
  - Padrões Arquiteturais, Stack e Estilo de Código
  - Quality Gates e Definition of Done
- Removed Sections: N/A (Substituição de placeholders por regras concretas)
- Deferred TODOs: None
-->

# SIMCC Backend Constitution

## Core Principles

### I. Preservação de Especificações Legadas e Evolução Criteriosa
O SIMCC é um ecossistema em refatoração contínua de uma base legada em produção ativa.
- As especificações e comportamentos do sistema legado **têm precedência** sobre novas suposições arquiteturais;
- Mudanças que alterem contratos de rotas existentes, formatos de dados ou integrações de banco **DEVEM** ser tratadas com cautela máxima e compatibilidade retroativa;
- Melhorias e refatorações são encorajadas, mas grandes alterações estruturais **DEVEM** ser implementadas de forma incremental, segura e reversível.

### II. Isolamento Obrigatório em Git Worktrees e Branch Develop
Todo fluxo de desenvolvimento de novas features e refatorações deve manter o ambiente principal limpo e isolado:
- Novas implementações **DEVEM** ser desenvolvidas exclusivamente em uma `git worktree` dedicada e temporária;
- O merge para a branch `develop` só **DEVE** ocorrer após a conclusão total de todos os itens do checklist da especificação e validação de todos os testes;
- É proibido realizar merges diretos ou push direto na branch `main` ou `develop` com tarefas incompletas.

### III. Commits Granulares, Descritivos e em Português
A rastreabilidade e histórico do repositório devem ser impecáveis:
- Implementações longas **DEVEM** ser divididas em commits atômicos e granulares (um propósito lógico por commit);
- As mensagens de commit **DEVEM** ser escritas em português, no modo imperativo, claras, descritivas e curtas (ex: `feat(api): adiciona rota de busca por pesquisador`, `fix(db): corrige rollback em nested transactions`);
- Commits monolíticos com mensagens genéricas (ex: "wip", "ajustes gerais", "fim da task") são expressamente proibidos.

### IV. Metodologia de Testes Orientada a Risco e Camadas
A suíte de testes segue a arquitetura estabelecida em `.agents/skills/fastapi-testing-methodology`:
- **Camadas bem definidas**: Testes unitários para regras de negócio isoladas com mocks (`tests/unit/services/`), testes de integração com banco real via `testcontainers` para queries e repositórios (`tests/integration/repositories/`), e testes de ponta a ponta para routers (`tests/api/routers/`);
- **Performance e Isolamento de DB**: É proibido executar `drop_all`/`create_all` a cada teste. O setup cria as tabelas uma vez na sessão e utiliza *Nested Transactions (Savepoints)* com rollback para reset instantâneo de estado;
- **Execução Concorrente**: Execuções paralelas via `pytest-xdist` **DEVEM** utilizar a flag `--dist loadscope`;
- **Regressão**: Qualquer correção de bug **DEVE** incluir um teste de regressão demonstrando a resolução;
- **Testes de IA**: Testes marcados com `@pytest.mark.ai_live` consomem tokens externos e não devem ser executados na suíte padrão automatizada.

### V. Observabilidade e Logging Estruturado Rigoroso (JSONL)
A observabilidade do sistema segue estritamente as convenções de `.agents/skills/logging`:
- Todos os registros de log **DEVEM** ser emitidos no formato JSONL estruturado em conformidade com o schema global (`timestamp`, `level`, `application`, `environment`, `hostname`, `category`, `event`, `message`, `request_id`, `duration`, `data`);
- O objeto `data` **DEVE** aderir ao esquema fixo por categoria (`http`, `database`, `routine`, `system`);
- É estritamente proibido o uso de `print()`, loggers genéricos sem formatação JSON ou omissão de metadados de contexto.

### VI. Documentação Humanizada e Viva com MkDocs
O conhecimento do projeto deve ser preservado de forma contínua e acessível:
- Toda nova funcionalidade, alteração arquitetural ou decisão técnica **DEVE** ter seus conceitos incorporados à documentação no diretório `docs/` e indexados no `mkdocs.yml`;
- O tom da documentação **DEVE** ser humanizado, técnico-didático, contextualizado e focado em guias claros, diagramas e explicação do "porquê";
- Listas brutas de tarefas operacionais (como arquivos `tasks.md` ou backlogs efêmeros) **NÃO DEVEM** ser incluídas na documentação final do MkDocs.

## Padrões Arquiteturais, Stack e Estilo de Código

1. **Stack Tecnológica**:
   - Python 3.13+ gerenciado via Poetry e Taskipy;
   - FastAPI com tipagem estrita via Pydantic v2 e Pydantic Settings;
   - PostgreSQL 17 com extensões `unaccent` e `pgvector`, orquestrado via SQLAlchemy 2.0 (assíncrono com `asyncpg` e `psycopg3`);
   - Alembic para migrações controladas do schema relacional.

2. **Arquitetura em Camadas (`src/simcc/`)**:
   - `routers/`: Controladores de entrada HTTP, injeção de dependências e serialização de DTOs.
   - `services/`: Regras de negócio puras, orquestração e fluxos de domínio.
   - `repositories/`: Acesso a dados, persistência e execução de consultas.
   - `queries/`: *Query Objects* dedicados para montagem de SQL dinâmico e filtros complexos.
   - `schemas/`: Modelos Pydantic de entrada e resposta da API.
   - `core/`: Configurações de ambiente, infraestrutura de banco e observabilidade/logging.
   - `ai/`: Integração com provedores LLM, query planner e prompts.

3. **Padrão de Código e Linting (Ruff)**:
   - Limite de comprimento de linha: **79 caracteres** (`line-length = 79`);
   - Formatação de aspas: **Aspas simples** (`quote-style = 'single'`);
   - Regras ativas de lint: `I` (isort), `F` (Pyflakes), `E`/`W` (pycodestyle), `PL` (Pylint) e `PT` (pytest-style);
   - Todo código adicionado deve passar limpo em `poetry run ruff check .` e `poetry run ruff format .`.

4. **Migrações de Banco de Dados**:
   - Toda alteração em modelos SQLAlchemy **DEVE** gerar migração via Alembic;
   - O arquivo gerado deve ser revisado manualmente antes da aplicação, garantindo integridade de tipos específicos, extensões, índices e reversibilidade funcional em `downgrade()`.

## Quality Gates e Definition of Done

Nenhuma entrega é considerada pronta para integração em `develop` sem satisfazer todos os critérios abaixo:

1. **Cobertura Funcional e de Risco**:
   - Caminho crítico (Happy Path) coberto de ponta a ponta via teste de API (`TestClient`);
   - Regras de negócio e ramificações críticas validadas com testes unitários rápidos e isolados;
   - Consultas com JOINs ou filtros complexos cobertas em testes de repositório com `testcontainers`.

2. **Integridade de Observabilidade**:
   - Rotas, rotinas ou operações de persistência registram eventos de log com schema válido e categorias corretas (`http`, `database`, `routine`).

3. **Qualidade de Código e Testes**:
   - Execução completa da suíte de testes passando sem falhas (`poetry run task test`);
   - Código 100% formatado e validado pelo Ruff sem violações (`poetry run ruff check .`).

4. **Documentação e Versionamento**:
   - Documentação humanizada atualizada no `docs/` e refletida no `mkdocs.yml`;
   - Commits granulares, explicativos e escritos em português;
   - Validação da funcionalidade finalizada antes do merge para a branch `develop`.

## Governance

- **Supremacia da Constituição**: Este documento define as leis fundamentais de desenvolvimento e engenharia do SIMCC. Todo agente autônomo, ferramenta de IA e desenvolvedor humano deve cumprir estritamente estes princípios;
- **Procedimento de Emenda**: Qualquer alteração ou inclusão de novos princípios exige proposta documentada, justificativa técnica de impacto e atualização da versão semântica da constituição acompanhada do *Sync Impact Report*;
- **Política de Versionamento**:
  - **MAJOR (X.0.0)**: Alterações que quebrem contratos existentes, remoção ou redefinição de princípios fundamentais;
  - **MINOR (1.X.0)**: Adição de novas diretrizes, padrões arquiteturais ou expansão de critérios de aceite;
  - **PATCH (1.0.X)**: Correções de redação, ajustes ortográficos ou refinamentos que não alterem a semântica das regras;
- **Revisão Contínua**: O alinhamento com a constituição deve ser verificado em todos os planos gerados pelo Spec-Kit (`spec.md`, `plan.md`, `tasks.md`).

**Version**: 1.0.0 | **Ratified**: 2026-09-01 | **Last Amended**: 2026-09-01
