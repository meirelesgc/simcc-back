# Feature Specification: Limpeza do Banco e Dados Flexíveis de Pesquisadores

**Feature Branch**: `002-db-cleanup-legacy-schemas`

**Created**: 2026-08-29

**Status**: Draft

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Remover Schemas Legados sem Quebrar o Frontend (Priority: P1)

O sistema deve remover as dependências em tempo de execução dos schemas externos `admin`,
`admin_simcc`, `logs` e `ufmg`, que pertencem a bancos de dados ou schemas administrados
por terceiros. Quando esses schemas não estiverem disponíveis, o sistema continua respondendo
com dados compatíveis (listas vazias, contagens zeradas, `null`) sem lançar exceções.

**Why this priority**: É o risco mais crítico — qualquer falha aqui impacta o frontend
diretamente, quebrando as telas de detalhe de pesquisadores e métricas institucionais.

**Independent Test**: Pode ser testado subindo a API sem os schemas legados no banco e
verificando que os endpoints `/researchers`, `/researchers/{id}` respondem com `200 OK`
e dados parciais válidos (campos herdados do legado como `departments`, `ufmg`, `user`
aparecem como `null` ou `[]`).

**Acceptance Scenarios**:

1. **Given** o banco não possui o schema `ufmg`, **When** o endpoint `/researchers` é chamado,
   **Then** a resposta retorna `200 OK` com a lista de pesquisadores onde o campo `ufmg`
   é `null` e `departments` é `[]`.

2. **Given** o banco não possui o schema `admin`, **When** o endpoint `/researchers` é chamado
   com `lattes_id` preenchido, **Then** a resposta retorna `200 OK` e o campo `user`
   no enrichment é `null` sem gerar erro.

3. **Given** a função `enrich_researchers` é chamada com schemas legados ausentes,
   **When** as queries falham por ausência de tabela, **Then** o sistema captura a
   exceção graciosamente e retorna o dado padrão correspondente ao tipo (lista vazia
   ou `null`), sem interromper o enriquecimento dos outros campos.

4. **Given** o endpoint de contagem de professores/técnicos (RT) é chamado sem o schema `ufmg`,
   **When** a query falha, **Then** a resposta retorna contagens zeradas com `200 OK`.

---

### User Story 2 - Dados Proprietários e Flexíveis por Instituição (Priority: P2)

Cada instituição poderá armazenar dados próprios vinculados a pesquisadores: campos
padronizados como `zip_code` e `work_regime`, além de atributos arbitrários em formato
livre (par chave-valor). Esses dados são injetados no resultado de `enrich_researchers`
sob a chave `custom_attributes`.

**Why this priority**: Resolve uma necessidade real de personalização sem exigir migrações
de schema para cada nova instituição; desbloqueia casos de uso que hoje dependem do schema
`ufmg` (dados institucionais específicos).

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

- O que acontece se o schema legado existir mas a tabela específica não existir?
  O sistema deve tratar `UndefinedTable` da mesma forma que schema ausente.
- Como o sistema se comporta quando o banco de dados legado está temporariamente
  indisponível por timeout? O fallback deve retornar o dado padrão, não propagar o timeout.
- O que acontece se o JSONB de `custom_attributes` contém chaves que colidem com campos
  padronizados (`zip_code`, `work_regime`)? Os campos padronizados têm precedência;
  o JSONB armazena apenas os atributos flexíveis/extras.
- Quantos pesquisadores serão deletados ao aplicar `lattes_id NOT NULL`?
  A migração deve logar a contagem antes de deletar para fins de auditoria.
- O que acontece se o CSV tiver encoding diferente de UTF-8?
  O script deve tentar detectar o encoding automaticamente ou aceitar um parâmetro
  de configuração de encoding (padrão UTF-8).
- O que acontece se o CSV não contiver a coluna de identificação (`lattes_id` ou `researcher_id`)?
  O script deve falhar explicitamente com uma mensagem clara indicando a coluna ausente.
- O que acontece com linhas do CSV onde o identificador está vazio (`""`)?
  Essas linhas devem ser ignoradas e contadas como ignoradas no resumo.

## Requirements *(mandatory)*

### Functional Requirements

**Remoção de Schemas Legados com Compatibilidade:**

- **FR-001**: O sistema MUST substituir todas as chamadas às tabelas dos schemas legados
  (`ufmg.*`, `admin.users`) por stubs que retornam dados compatíveis com a tipagem
  esperada (lista vazia, `null`, zero) quando o schema/tabela não estiver disponível.
- **FR-002**: O sistema MUST capturar erros de tabela/schema inexistente e retornar
  o valor padrão correspondente, sem relançar a exceção para a camada de router.
- **FR-003**: O campo `departments` em `enrich_researchers` MUST retornar `[]` quando
  o schema `ufmg` estiver ausente.
- **FR-004**: O campo `ufmg` em `enrich_researchers` MUST retornar `null` quando o
  schema `ufmg` estiver ausente.
- **FR-005**: O campo `user` em `enrich_researchers` MUST retornar `null` quando o
  schema `admin` estiver ausente.
- **FR-006**: Métricas que dependem de contagens do schema `ufmg` (RT de professores
  e técnicos) MUST retornar zero quando o schema estiver ausente.
- **FR-007**: Os modelos SQLAlchemy dos schemas legados MUST ser removidos do
  `table_registry` para que o Alembic deixe de tentar gerenciar suas migrações.

**Dados Proprietários de Instituição (`researcher_institution_data`):**

- **FR-008**: O sistema MUST criar uma nova tabela `researcher_institution_data` com
  as colunas: `id` (UUID PK), `researcher_id` (UUID FK → `researcher.id` ON DELETE CASCADE, UNIQUE),
  `zip_code` (VARCHAR, nullable), `work_regime` (VARCHAR, nullable),
  `custom_attributes` (JSONB, nullable).
- **FR-009**: O sistema MUST incluir `custom_attributes` como campo no resultado de
  `enrich_researchers`, mapeando o conteúdo completo da linha da nova tabela.
- **FR-010**: Quando não houver linha em `researcher_institution_data` para o pesquisador,
  o campo `custom_attributes` em `enrich_researchers` MUST retornar `null`.
- **FR-011**: A nova tabela MUST ter índice único em `researcher_id` para garantir integridade
  e alta performance nas buscas em lote do `enrich_researchers`.

**Integridade: `lattes_id NOT NULL`:**

- **FR-012**: A migração Alembic MUST primeiro executar um `DELETE FROM researcher WHERE lattes_id IS NULL`,
  logando a quantidade de registros removidos antes de executar o delete.
- **FR-013**: Após a limpeza, a migração MUST adicionar a constraint `NOT NULL` no
  campo `lattes_id` da tabela `researcher`.
- **FR-014**: O modelo SQLAlchemy de `Researcher` MUST atualizar `lattes_id` de
  `Optional[str]` para `str` (não-nullable).
- **FR-015**: A migração MUST ser reversível: o `downgrade()` MUST remover a constraint
  `NOT NULL` (voltar para nullable), sem tentar recriar os registros deletados.

**Script de Ingestão de Dados Proprietários via CSV:**

- **FR-016**: O sistema MUST fornecer um script em `scripts/ingest/ingest_institution_researchers.py`
  que receba como argumento o caminho de um arquivo CSV de instituição (ex.: `storage/researchers/ufrb.csv`).
- **FR-017**: O script MUST usar `researcher_id` como chave canônica de match no banco
  (resolvendo via `lattes_id` do CSV para `researcher.id`).
- **FR-018**: Todos os dados do CSV MUST ser gravados na tabela `researcher_institution_data`:
  `zip_code` e `work_regime` nas colunas dedicadas; atributos adicionais (`siape`, `department`, `city`, etc.)
  no JSONB `custom_attributes`; o campo `name` MUST ser descartado por redundância.
- **FR-019**: O script MUST realizar upsert (inserir se novo, atualizar se existente)
  na tabela `researcher_institution_data` utilizando `researcher_id` como chave de conflito.
- **FR-020**: O script MUST ignorar linhas sem correspondência no banco e exibir relatório
  com total de registros processados, inseridos/atualizados e ignorados.

### Key Entities

- **Researcher**: Entidade central; `lattes_id` passa a ser campo obrigatório não-nulo.
- **ResearcherInstitutionData**: Tabela única que unifica os dados institucionais e
  proprietários vinculados ao pesquisador via `researcher_id` (1 para 1).
  Atributos: `researcher_id`, `zip_code`, `work_regime`, `custom_attributes` (JSONB livre
  contendo os demais dados da instituição).
- **Schemas Legados** (`admin`, `ufmg`): Desacoplados do runtime e do Alembic.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Os endpoints `/researchers` e demais rotas de pesquisadores retornam
  `200 OK` em ambiente sem os schemas `admin`, `ufmg`, `admin_simcc` e `logs` presentes,
  com campos de fallback devidamente populados (`[]`, `null`, `0`).
- **SC-002**: Nenhum pesquisador com `lattes_id = NULL` existe no banco após a aplicação
  da migração.
- **SC-003**: A inserção de um pesquisador sem `lattes_id` é rejeitada pelo banco de
  dados com erro de constraint após a migração.
- **SC-004**: O campo `custom_attributes` aparece populado na resposta do endpoint de
  pesquisadores para todos os pesquisadores com entrada em `researcher_institution_data`.
- **SC-005**: A suíte de testes passa integralmente após as mudanças, sem regressões.
- **SC-006**: O Alembic não gera novas migrações relacionadas aos schemas legados após
  sua remoção do `table_registry`.
- **SC-007**: O script de ingestão processa com sucesso `storage/researchers/ufrb.csv`,
  inserindo todos os dados na tabela `researcher_institution_data` vinculados por
  `researcher_id`, descartando `name` e gravando os atributos extras no JSONB `custom_attributes`.

## Assumptions

- Todos os pesquisadores sem `lattes_id` são registros inválidos e podem ser deletados
  sem aprovação individual, pois `lattes_id` é o identificador canônico do sistema.
- A tabela `researcher_institution_data` unifica todos os dados proprietários e flexíveis
  de todas as instituições em uma única estrutura, com relação 1-para-1 por `researcher_id`.
- O identificador canônico do projeto é `researcher_id` (`researcher.id`), sendo utilizado
  como chave primária/estrangeira de relacionamento.
- A coluna `name` nos CSVs é redundante em relação a `researcher.name` e não é persistida.
- Os schemas `admin_simcc` e `logs` não são referenciados diretamente em código Python;
  sua remoção do `table_registry` é suficiente para desacoplamento.
- O campo `work_regime` é uma string livre sem enum fixo, permitindo variações entre instituições.
- A remoção dos modelos do `table_registry` não exige migration de DROP físico das tabelas legadas.
- O frontend não depende de dados reais do schema `ufmg` para funcionar, apenas precisa
  receber a estrutura com valores válidos de fallback (`[]` ou `null`).
- Arquivos CSV de outras universidades seguirão a mesma estrutura e serão processados pelo
  mesmo script de ingestão para a mesma tabela.
