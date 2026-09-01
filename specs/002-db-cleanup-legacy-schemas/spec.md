# Feature Specification: Limpeza do Banco e Dados Flexíveis de Pesquisadores

**Feature Branch**: `002-db-cleanup-legacy-schemas`

**Created**: 2026-08-29  
**Last Updated**: 2026-08-31  

**Status**: Ready for Implementation

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Remover Schemas Legados sem Quebrar o Frontend (Priority: P1)

O sistema deve remover completamente as dependências diretas em tempo de execução dos schemas externos `admin`,
`admin_ufmg`, `admin_simcc`, `logs` e `ufmg`, que pertencem a bancos de dados ou schemas legados administrados
por terceiros. Todas as consultas SQL que continham referências hardcoded a esses schemas (em filtros,
joins, agregações de métricas, contagens institucionais, rotas externas, filtros de pesquisadores e exportações PowerBI)
devem ser refatoradas ou protegidas com fallbacks resilientes. Quando esses schemas ou tabelas não estiverem
disponíveis no banco de dados, o sistema deve responder com dados válidos e compatíveis com a tipagem esperada
pelo frontend (listas vazias `[]`, contagens zeradas `0`, valores nulos `null`), sem lançar exceções de banco
(como `UndefinedTableError`, `ProgrammingError` ou `OperationalError`).

**Why this priority**: É o risco mais crítico — qualquer falha em consultas com schemas inexistentes impacta
o frontend diretamente, quebrando telas de busca, listagem, filtros de busca avançada, métricas institucionais,
produções e relatórios.

**Independent Test**: Pode ser testado subindo a API contra um banco de dados PostgreSQL contendo apenas o
schema `public` (sem os schemas `ufmg`, `admin`, etc.) e verificando que todos os endpoints de `/researchers`,
`/researchers/{id}`, `/researcher/filter`, `/institution`, `/metrics`, `/production`, `/external` e `/powerBi`
respondem com `200 OK` e dados compatíveis.

**Acceptance Scenarios**:

1. **Given** o banco não possui o schema `ufmg`, **When** o endpoint `/researchers` é chamado,
   **Then** a resposta retorna `200 OK` com a lista de pesquisadores onde o campo `ufmg`
   é `null` e `departments` é `[]`.

2. **Given** o banco não possui o schema `admin`, **When** o endpoint `/researchers` é chamado
   com `lattes_id` preenchido, **Then** a resposta retorna `200 OK` e o campo `user`
   no enrichment é `null` sem gerar erro.

3. **Given** a rota de opções de filtros de pesquisadores (`/researcher/filter`) é chamada sem o schema `ufmg`,
   **When** a consulta `ResearcherFilterQuery` é executada, **Then** o campo `departament` retorna
   uma lista vazia `[]` com status `200 OK`.

4. **Given** a listagem de instituições (`/institution`) é chamada sem o schema `ufmg`,
   **When** a consulta `InstitutionSearchQuery` é executada, **Then** os campos `count_d` e `count_t`
   retornam `0` com status `200 OK` sem falhar por tabela inexistente.

5. **Given** qualquer endpoint de produção (`/production/*`), métricas (`/metrics/*`), programas
   de pós-graduação (`/graduate-program/*`) ou filtros comuns é chamado com parâmetros de departamento
   (`dep_id` ou `departament`), **When** o schema `ufmg` não existe, **Then** a consulta é executada
   com segurança sem falhar por erro de SQL, retornando os resultados compatíveis ou lista vazia.

6. **Given** endpoints de métricas de RT (`/researcher/departament-rt` ou `RtMetricsQuery`) são chamados sem o schema `ufmg`,
   **When** a query falha ou é executada, **Then** a resposta retorna contagens zeradas (`{"teachers": 0, "technicians": 0}` ou `[]`) com `200 OK`.

7. **Given** endpoints externos (`/external/researchers`, `/external/technicians`, `/external/departments`, etc.)
   são chamados sem o schema `ufmg`, **When** as consultas são executadas, **Then** retornam `200 OK`
   com listas vazias `[]` ou objetos nulos sem relançar erro 500.

8. **Given** endpoints de exportação de dados para PowerBI (`/powerBi/*`) são chamados sem os schemas `ufmg` e `admin`,
   **When** as consultas aos dados legados são executadas, **Then** o sistema gera arquivos/respostas
   com estruturas vazias válidas (headers presentes e 0 linhas) com `200 OK`.

---

### User Story 2 - Dados Proprietários e Flexíveis por Instituição (Priority: P2)

Cada instituição poderá armazenar dados próprios vinculados a pesquisadores: campos
padronizados como `zip_code` e `work_regime`, além de atributos arbitrários em formato
livre (par chave-valor). Esses dados são injetados no resultado de `enrich_researchers`
sob a chave `custom_attributes`.

**Why this priority**: Resolve uma necessidade real de personalização sem exigir migrações
de schema para cada nova instituição; desbloqueia casos de uso que antes dependiam do schema
legado `ufmg` (dados institucionais específicos).

**Independent Test**: Pode ser testado criando uma entrada na nova tabela para um
pesquisador e verificando que o campo `custom_attributes` aparece populado na resposta
de `/researchers`.

**Acceptance Scenarios**:

1. **Given** uma entrada de dados proprietários existe para um pesquisador,
   **When** o endpoint `/researchers` retorna esse pesquisador,
   **Then** o campo `custom_attributes` contém os dados com `zip_code`, `work_regime`
   e os atributos livres do JSONB.

2. **Given** nenhuma entrada de dados proprietários existe para um pesquisador,
   **When** o endpoint retorna esse pesquisador,
   **Then** o campo `custom_attributes` é `null`.

3. **Given** uma entrada de dados proprietários existe com apenas atributos livres no JSONB,
   **When** o endpoint retorna o pesquisador,
   **Then** os campos `zip_code` e `work_regime` aparecem como `null` e os atributos
   livres são retornados corretamente.

---

### User Story 3 - Integridade Referencial: `lattes_id` Obrigatório (Priority: P3)

O campo `lattes_id` na tabela `researcher` passa a ser `NOT NULL`. A migração que aplica
essa restrição deve ser segura: primeiro removendo pesquisadores sem `lattes_id` (junto
com toda a sua produção via CASCADE), depois aplicando a constraint.

**Why this priority**: Garante integridade de dados a longo prazo; `lattes_id` é o
identificador canônico de pesquisadores no ecossistema CNPq e qualquer registro sem ele
é considerado inválido ou órfão.

**Independent Test**: Pode ser testado verificando que a tentativa de inserir um
`researcher` sem `lattes_id` resulta em erro de constraint, e que a migração rodou
sem erros em um banco de staging com dados reais.

**Acceptance Scenarios**:

1. **Given** existem pesquisadores com `lattes_id = NULL` antes da migração,
   **When** a migração é executada, **Then** esses registros são removidos (junto com
   todos os dados relacionados via CASCADE) antes da constraint ser aplicada.

2. **Given** a migração foi aplicada com sucesso, **When** uma inserção de pesquisador
   sem `lattes_id` é tentada, **Then** o banco rejeita com erro de violação de constraint.

3. **Given** a migração foi aplicada com sucesso, **When** todos os pesquisadores restantes
   são consultados, **Then** nenhum possui `lattes_id NULL`.

---

### User Story 4 - Script de Ingestão de Dados Proprietários via CSV (Priority: P2)

Existirá um script reutilizável em `scripts/ingest/` capaz de ler um arquivo CSV de
pesquisadores de uma instituição (ex.: `storage/researchers/ufrb.csv`) e popular todos
os dados na tabela `researcher_institution_data`.

A chave padrão de correspondência e integridade do projeto é **`researcher_id`**. O script
localiza o `researcher_id` correspondente (usando `lattes_id` do CSV para consultar `researcher.id`
ou `researcher_id` se fornecido). Todos os dados do CSV são centralizados na mesma tabela
`researcher_institution_data`:
- Colunas padronizadas (`zip_code`, `work_regime`) vão para suas respectivas colunas dedicadas.
- O campo `name` é considerado redundante e é completamente ignorado/descartado.
- Todas as demais colunas institucionais (ex.: `siape`, `department`, `city`) são salvas
  como atributos no campo JSONB `custom_attributes`.

O script é reutilizável para CSVs de outras universidades (ex.: UFBA, UESC, etc.) que
seguirão o mesmo fluxo de unificação na tabela `researcher_institution_data`.

**Why this priority**: É o mecanismo de validação e carga inicial dos `custom_attributes`;
sem ele não é possível verificar o funcionamento da tabela e do enrichment em dados reais.

**Independent Test**: Pode ser testado executando o script com o arquivo
`storage/researchers/ufrb.csv` e verificando que os pesquisadores vinculados pelo
`researcher_id` passam a ter todos os seus dados institucionais na tabela
`researcher_institution_data` e refletidos em `custom_attributes` no endpoint `/researchers`.

**Acceptance Scenarios**:

1. **Given** o arquivo `storage/researchers/ufrb.csv` contém dados de pesquisadores,
   **When** o script é executado, **Then** todos os dados são inseridos/atualizados na
   tabela `researcher_institution_data` vinculados pelo `researcher_id`, com `zip_code`
   e `work_regime` nas colunas dedicadas e as demais colunas (`siape`, `department`, `city`)
   no JSONB `custom_attributes`. O campo `name` não é armazenado.

2. **Given** o CSV contém uma linha cujo pesquisador não existe no banco,
   **When** o script processa essa linha, **Then** ela é ignorada e contabilizada
   no relatório final de execução.

3. **Given** o CSV contém uma linha com `zip_code` vazio (ex.: UFRB/CCS),
   **When** o script processa essa linha, **Then** `zip_code` é salvo como `null` sem erro.

4. **Given** CSVs de diferentes universidades são processados sucessivamente,
   **When** o script é executado para cada um, **Then** todos os dados são gravados na
   mesma tabela `researcher_institution_data` sem conflitos entre instituições.

5. **Given** o script é reexecutado com o mesmo CSV,
   **When** ocorre a execução, **Then** os registros existentes são atualizados (upsert
   via `researcher_id`) sem duplicação.

---

### Edge Cases

- **Ausência total dos schemas no banco**: Quando o PostgreSQL não tiver os schemas `ufmg`, `admin`, `admin_ufmg`, `logs`, `admin_simcc`, nenhuma rota deve retornar erro 500.
- **Filtros por departamento (`dep_id`, `departament`) sem tabelas de departamento**: Quando o usuário ou frontend enviar filtros de departamento mas o schema `ufmg` não existir, o sistema deve ignorar o join quebrado ou retornar conjunto vazio seguro sem gerar exceção de SQL.
- **Consulta de filtros (`ResearcherFilterQuery`) sem `ufmg.departament`**: Deve retornar lista vazia `[]` para o campo `departament`.
- **Contagens em `InstitutionSearchQuery`**: `count_d` (pesquisadores vinculados à instituição legado) e `count_t` (técnicos) devem retornar `0` de forma segura.
- **Consultas PowerBI e External**: Devem retornar estruturas neutras / DataFrames vazios quando os schemas não existirem.
- **Conflito de chaves em `custom_attributes`**: Se o JSONB contiver chaves como `zip_code` ou `work_regime`, os campos dedicados da tabela têm precedência.
- **Registros com `lattes_id = NULL`**: Serão removidos na migração com log de contagem prévia.

---

## Requirements *(mandatory)*

### Functional Requirements

**Remoção de Schemas Legados e Resiliência em Todas as Consultas:**

- **FR-001**: O sistema MUST substituir ou proteger todas as chamadas SQL e queries que referenciem
  tabelas dos schemas legados (`ufmg.*`, `admin.*`, `admin_ufmg.*`, `logs.*`, `admin_simcc.*`)
  por consultas desacopladas ou stubs com fallback gracioso (`[]`, `null`, `0`).
- **FR-002**: O sistema MUST capturar erros de tabela/schema inexistente (`ProgrammingError`,
  `UndefinedTableError`, `OperationalError`) em todas as camadas de repositório e serviços que
  possam interagir com dados legados, retornando o valor padrão correspondente sem relançar exceção.
- **FR-003**: O campo `departments` em `enrich_researchers` MUST retornar `[]` quando o schema
  `ufmg` estiver ausente.
- **FR-004**: O campo `ufmg` em `enrich_researchers` MUST retornar `null` quando o schema
  `ufmg` estiver ausente.
- **FR-005**: O campo `user` em `enrich_researchers` MUST retornar `null` quando o schema
  `admin` estiver ausente.
- **FR-006**: Métricas e contagens que dependiam do schema `ufmg` (RT de professores e técnicos,
  `count_d` e `count_t` em instituições) MUST retornar `0` quando o schema estiver ausente.
- **FR-007**: A consulta de filtros de pesquisadores (`ResearcherFilterQuery`) MUST retornar `[]`
  para `departament` sem falhar quando a tabela `ufmg.departament` não existir.
- **FR-008**: Os filtros de departamento (`dep_id`, `departament`) em consultas de produção,
  métricas, pesquisadores, programas de pós-graduação e consultas comuns MUST ser desacoplados
  do schema `ufmg` ou manipulados de forma que ausência de tabelas legadas não cause falha na consulta principal.
- **FR-009**: As consultas do módulo `external` (`ExternalResearcherSearchQuery`, `DepartmentSearchQuery`,
  `ResearcherDataQuery`, `TechnicianQuery`) e `powerBi` (`DimResearcherQuery`, `DimDepartamentQuery`,
  `DimDepartamentTechnicianQuery`, `DimDepartamentResearcherQuery`, `DimResearcherAreaQuery`, etc.)
  MUST tratar a ausência de schemas legados retornando listas/conjuntos de dados vazios sem quebrar os endpoints.
- **FR-010**: A rotina `scripts/routines/abstract_ai.py` MUST ser atualizada para não depender
  diretamente de consultas SQL duras ao schema `ufmg`.
- **FR-011**: Os modelos SQLAlchemy dos schemas legados (`admin.py`, `ufmg.py`) MUST ser removidos
  do `table_registry` para que o Alembic gerencie unicamente o schema `public`.

**Dados Proprietários de Instituição (`researcher_institution_data`):**

- **FR-012**: O sistema MUST criar uma nova tabela `researcher_institution_data` com
  as colunas: `id` (UUID PK), `researcher_id` (UUID FK → `researcher.id` ON DELETE CASCADE, UNIQUE),
  `zip_code` (VARCHAR, nullable), `work_regime` (VARCHAR, nullable),
  `custom_attributes` (JSONB, nullable).
- **FR-013**: O sistema MUST incluir `custom_attributes` como campo no resultado de
  `enrich_researchers`, mapeando o conteúdo consolidado da linha da nova tabela.
- **FR-014**: Quando não houver linha em `researcher_institution_data` para o pesquisador,
  o campo `custom_attributes` em `enrich_researchers` MUST retornar `null`.
- **FR-015**: A nova tabela MUST ter índice único em `researcher_id` para garantir integridade
  e alta performance nas buscas em lote do `enrich_researchers`.

**Integridade: `lattes_id NOT NULL`:**

- **FR-016**: A migração Alembic MUST primeiro executar um `DELETE FROM researcher WHERE lattes_id IS NULL`,
  logando a quantidade de registros removidos antes de executar o delete.
- **FR-017**: Após a limpeza, a migração MUST adicionar a constraint `NOT NULL` no
  campo `lattes_id` da tabela `researcher`.
- **FR-018**: O modelo SQLAlchemy de `Researcher` MUST atualizar `lattes_id` de
  `Optional[str]` para `str` (não-nullable).
- **FR-019**: A migração MUST ser reversível: o `downgrade()` MUST remover a constraint
  `NOT NULL` (voltar para nullable), sem tentar recriar os registros deletados.

**Script de Ingestão de Dados Proprietários via CSV:**

- **FR-020**: O sistema MUST fornecer um script em `scripts/ingest/ingest_institution_researchers.py`
  que receba como argumento o caminho de um arquivo CSV de instituição (ex.: `storage/researchers/ufrb.csv`).
- **FR-021**: O script MUST usar `researcher_id` como chave canônica de match no banco
  (resolvendo via `lattes_id` do CSV para `researcher.id`).
- **FR-022**: Todos os dados do CSV MUST ser gravados na tabela `researcher_institution_data`:
  `zip_code` e `work_regime` nas colunas dedicadas; atributos adicionais (`siape`, `department`, `city`, etc.)
  no JSONB `custom_attributes`; o campo `name` MUST ser descartado por redundância.
- **FR-023**: O script MUST realizar upsert (inserir se novo, atualizar se existente)
  na tabela `researcher_institution_data` utilizando `researcher_id` como chave de conflito.
- **FR-024**: O script MUST ignorar linhas sem correspondência no banco e exibir relatório
  com total de registros processados, inseridos/atualizados e ignorados.

---

### Key Entities

- **Researcher**: Entidade central; `lattes_id` passa a ser campo obrigatório não-nulo.
- **ResearcherInstitutionData**: Tabela única que unifica os dados institucionais e
  proprietários vinculados ao pesquisador via `researcher_id` (1 para 1).
  Atributos: `researcher_id`, `zip_code`, `work_regime`, `custom_attributes` (JSONB livre
  contendo os demais dados da instituição).
- **Schemas Legados** (`admin`, `admin_ufmg`, `ufmg`, `logs`, `admin_simcc`): Desacoplados do runtime e do Alembic.

---

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Todos os endpoints de pesquisadores, instituições, filtros, métricas, produções,
  external e powerBi retornam `200 OK` em ambiente de banco de dados sem os schemas legados
  `admin`, `admin_ufmg`, `ufmg`, `admin_simcc` e `logs`, retornando fallbacks válidos (`[]`, `null`, `0`).
- **SC-002**: Zero ocorrências de `UndefinedTableError` ou falhas de SQL decorrentes de schemas legados em runtime.
- **SC-003**: Nenhum pesquisador com `lattes_id = NULL` existe no banco após a aplicação da migração.
- **SC-004**: A inserção de um pesquisador sem `lattes_id` é rejeitada pelo banco de dados com erro de constraint após a migração.
- **SC-005**: O campo `custom_attributes` aparece populado na resposta do endpoint de pesquisadores para todos os pesquisadores com entrada em `researcher_institution_data`.
- **SC-006**: A suíte completa de testes passa integralmente após as mudanças, sem regressões nos contratos de API do frontend.
- **SC-007**: O Alembic não gera novas migrações relacionadas aos schemas legados após sua remoção do `table_registry`.
- **SC-008**: O script de ingestão processa com sucesso `storage/researchers/ufrb.csv`, inserindo todos os dados na tabela `researcher_institution_data` vinculados por `researcher_id`, descartando `name` e gravando os atributos extras no JSONB `custom_attributes`.

---

## Assumptions

- Todos os pesquisadores sem `lattes_id` são registros inválidos e podem ser deletados sem aprovação individual, pois `lattes_id` é o identificador canônico do sistema.
- A tabela `researcher_institution_data` unifica todos os dados proprietários e flexíveis de todas as instituições em uma única estrutura, com relação 1-para-1 por `researcher_id`.
- O identificador canônico do projeto é `researcher_id` (`researcher.id`), sendo utilizado como chave primária/estrangeira de relacionamento.
- A coluna `name` nos CSVs é redundante em relação a `researcher.name` e não é persistida.
- Os schemas `admin_simcc` e `logs` não são referenciados diretamente em consultas de aplicação; sua remoção do `table_registry` é suficiente para desacoplamento.
- O campo `work_regime` é uma string livre sem enum fixo, permitindo variações entre instituições.
- A remoção dos modelos do `table_registry` não exige migration de DROP físico imediato das tabelas legadas, mas a aplicação deve operar de forma 100% autônoma mesmo se as tabelas forem fisicamente excluídas.
- O frontend não depende de dados reais do schema `ufmg` para funcionar, apenas precisa receber a estrutura com valores válidos de fallback (`[]`, `null`, `0`).
- Arquivos CSV de outras universidades seguirão a mesma estrutura e serão processados pelo mesmo script de ingestão para a mesma tabela.
