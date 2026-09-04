# Feature Specification: Implementação de Rastreabilidade Distribuída e Telemetria com OpenTelemetry

**Feature Branch**: `002-opentelemetry-tracing`

**Created**: 2026-09-03

**Status**: Ready for Review

**Input**: User description: "Projeto de engenharia incremental para fazer o OpenTelemetry funcionar corretamente na API do SIMCC, gerando traces úteis da API FastAPI e dos estágios da pipeline de IA (MarIA: planner, redis.get, pgvector.search, cutoff, synthesis), instrumentando banco e cache de forma segura sem vazar dados sensíveis, criando métricas operacionais com baixa cardinalidade, mantendo os logs JSONL existentes e estruturando um módulo dedicado em simcc/core/telemetry/ com suporte a OTLP/Collector."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Rastreamento Distribuído da API e Ciclo de Vida HTTP (Priority: P1)

Como engenheiro de software, operador de infraestrutura ou mantenedor do SIMCC, quero que cada requisição HTTP recebida pela API FastAPI gere automaticamente um trace distribuído estruturado, para que eu possa diagnosticar rapidamente a latência de ponta a ponta, identificar rotas com problemas e visualizar o status de cada chamada.

**Why this priority**: Estabelece o alicerce fundamental de observabilidade. Sem o rastreamento do ciclo de vida HTTP da API, não é possível correlacionar as etapas internas subsequentes (banco de dados, cache e IA) a uma requisição específica.

**Independent Test**: Pode ser testado de forma isolada enviando requisições para rotas simples (ex: `GET /health`) e rotas analíticas (ex: `POST /ai/chat/ask`), verificando no exportador de telemetria se o trace raiz é emitido com `service.name`, método HTTP, rota semântica, código de status e duração em milissegundos.

**Acceptance Scenarios**:

1. **Given** uma requisição HTTP enviada a qualquer endpoint da API, **When** a resposta é retornada com sucesso (código 2xx/3xx), **Then** um span raiz de servidor é criado com a rota normalizada, método HTTP, status code e tempo exato de resposta.
2. **Given** uma requisição que resulta em erro interno (HTTP 5xx) ou timeout, **When** a falha é capturada, **Then** o span raiz é marcado com status de erro e os metadados da exceção são vinculados ao trace sem interromper a resposta graciosa ao cliente.
3. **Given** o processamento de uma requisição com identificador de correlação (`x-request-id`), **When** o trace é gerado, **Then** o `request_id` e o `trace_id` mantêm correlação bidirecional entre os logs estruturados e os spans de telemetria.

---

### User Story 2 - Decomposição Semântica da Pipeline da MarIA em Spans (Priority: P1)

Como desenvolvedor ou mantenedor da pipeline conversacional da MarIA, quero visualizar a execução de uma consulta de chat como uma árvore hierárquica de spans filhos (`planner`, `retrieval`, `cutoff` e `synthesis`), para que eu saiba exatamente quanto tempo cada etapa consumiu e qual componente foi responsável por lentidões ou falhas.

**Why this priority**: A pipeline de IA é o núcleo de maior valor e maior variabilidade de tempo do SIMCC. Decompor a execução em spans correlacionados viabiliza a otimização contínua dos prompts, dos modelos e das buscas vetoriais.

**Independent Test**: Submeter consultas de chat nos modos lote (`POST /ai/chat/ask`) e streaming (`POST /ai/chat/ask/stream`) e comprovar que a ferramenta de rastreamento exibe a árvore de spans filhos organizada hierarquicamente abaixo do span da requisição HTTP.

**Acceptance Scenarios**:

1. **Given** uma requisição de chat submetida à MarIA, **When** a pipeline é processada, **Then** spans filhos individuais são registrados com tempos precisos para os estágios `ai.planner`, `ai.retrieval`, `ai.cutoff` e `ai.synthesis`.
2. **Given** uma consulta idêntica subsequente que resulta em acerto de cache (`cache_hit = true`), **When** a resposta é servida a partir do Redis, **Then** o trace registra a recuperação rápida e evidencia a ausência da invocação do span de síntese ao modelo externo.
3. **Given** uma consulta cujos documentos recuperados ficam abaixo da linha de corte de similaridade, **When** a triagem semântica descarta os ruídos, **Then** o span de corte registra o descarte e o span de síntese documenta a ativação da variação empática de base em indexação.
4. **Given** uma falha na chamada à API do modelo de linguagem (ex: timeout ou credencial ausente), **When** a exceção é interceptada, **Then** apenas o span do estágio afetado e o trace raiz são marcados com erro, registrando o tipo de falha com clareza diagnóstica.

---

### User Story 3 - Visibilidade Segura de Operações de Infraestrutura (Banco e Cache) (Priority: P2)

Como mantenedor do sistema e responsável por segurança e governança, quero monitorar a latência das consultas ao PostgreSQL (incluindo buscas no `pgvector`) e operações no Redis, garantindo que nenhum comando SQL literal com parâmetros, chaves completas ou dados pessoais sensíveis seja exposto nos traces.

**Why this priority**: Permite encontrar queries lentas e contenções de cache sem transformar a telemetria em uma cópia desprotegida do banco de dados ou vazar dados confidenciais de pesquisadores.

**Independent Test**: Executar requisições que acionem consultas relacionais complexas e operações de cache, validando que os spans de banco e Redis são gerados com nomes lógicos de operações e durações, mas sem texto integral de SQL parametrizado ou conteúdos sigilosos.

**Acceptance Scenarios**:

1. **Given** a execução de uma consulta relacional ou vetorial no PostgreSQL, **When** a operação é executada, **Then** um span de banco é registrado com o nome da operação/tabela, status e tempo de execução, sem conter SQL com parâmetros de usuários em ambientes de produção.
2. **Given** uma leitura ou escrita na camada de cache Redis, **When** o comando é despachado, **Then** o span de infraestrutura registra o comando (`GET`, `SET`) e a latência, sem transformar chaves completas com valores dinâmicos em dimensões de alta cardinalidade.
3. **Given** chamadas HTTP externas a APIs de terceiros (provedores de LLM ou fontes acadêmicas), **When** a requisição de saída é enviada, **Then** o span de cliente registra o domínio de destino, o método e o tempo de rede consumido.

---

### User Story 4 - Métricas Operacionais com Baixa Cardinalidade (Priority: P2)

Como engenheiro de confiabilidade e operações, quero dispor de métricas agregadas fundamentais da API e da inteligência artificial (histogramas de latência, taxa de erros, proporção de acertos de cache e consumo agregado de tokens), com dimensões restritas para possibilitar retenção histórica prolongada sem alto custo de armazenamento.

**Why this priority**: Traces fornecem diagnósticos detalhados para períodos recentes (7 a 14 dias), enquanto métricas agregadas de baixa cardinalidade permitem acompanhar tendências históricas de performance e custos por meses.

**Independent Test**: Executar uma bateria de requisições concorrentes e inspecionar a geração de métricas numéricas agregadas (P50, P95, P99 e contadores), verificando que nenhuma dimensão de alta cardinalidade (como `user_id`, `request_id` ou texto de pergunta) foi associada aos medidores.

**Acceptance Scenarios**:

1. **Given** o tráfego regular de requisições na API, **When** as chamadas são atendidas, **Then** a duração é agregada em histogramas de distribuição de latência com dimensões limitadas (método, rota parametrizada, código de status e ambiente).
2. **Given** a execução contínua de interações com a MarIA, **When** os atendimentos são finalizados, **Then** contadores agregados registram o total de perguntas, taxa de acerto de cache e estimativas de tokens consumidos, segmentados estritamente por modelo e resultado.
3. **Given** uma análise de tendência histórica, **When** os dados agregados são consultados, **Then** percentis de latência (P50, P95, P99) podem ser calculados a partir dos histogramas sem degradação do sistema de telemetria.

---

### User Story 5 - Governança, Coexistência com JSONL e Preparação para o Collector (Priority: P3)

Como arquiteto de software da plataforma, quero que o OpenTelemetry seja estruturado em um módulo desacoplado (`simcc/core/telemetry/`) com suporte inicial a console/OTLP e sem interferir nos logs JSONL já existentes, para que a observabilidade possa evoluir para um OpenTelemetry Collector sem reescrita na aplicação.

**Why this priority**: Evita acoplamento prematuro a plataformas visuais específicas (Grafana, Jaeger, Datadog), preserva os investimentos já realizados no sistema de logs da aplicação e estabelece padrões claros para novos desenvolvedores.

**Independent Test**: Inicializar a aplicação e comprovar que os arquivos diários de log JSONL continuam sendo gravados normalmente, enquanto a telemetria do OpenTelemetry é gerada em paralelo e direcionada ao destino configurado via variáveis de ambiente.

**Acceptance Scenarios**:

1. **Given** a aplicação em execução com OpenTelemetry habilitado, **When** qualquer evento ou erro ocorre, **Then** os arquivos de log JSONL continuam sendo emitidos em conformidade com o schema vigente da Constituição do SIMCC.
2. **Given** a necessidade de alternar o exportador de telemetria (ex: saída em console para desenvolvimento local versus OTLP gRPC/HTTP para integração com OpenTelemetry Collector), **When** a variável de configuração é alterada, **Then** a aplicação ajusta o transporte sem modificações no código de domínio.
3. **Given** o desenvolvimento de novas rotas ou pipelines no futuro, **When** novos desenvolvedores consultam o guia de observabilidade, **Then** encontram regras claras proibindo o registro de dados sensíveis e o uso de dimensões de alta cardinalidade em métricas.

---

### Edge Cases

- **Queda ou latência extrema no OpenTelemetry Collector**: Caso o endpoint OTLP esteja fora do ar ou apresente timeout, a biblioteca de telemetria deve operar com processamento assíncrono em lote (*batch span processor*) com descarte silencioso de buffers cheios, assegurando que o tempo de resposta da API para o usuário final nunca seja degradado.
- **Cancelamento antecipado de conexões pelo usuário durante streaming SSE**: Quando o cliente fecha a aba ou interrompe o stream no meio da resposta, o span raiz e os spans dos estágios da IA devem ser fechados ordenadamente, registrando o cancelamento sem deixar spans órfãos ou conexões presas.
- **Consultas sem documentos válidos ou saudações simples**: Consultas como "Olá" ou termos desconhecidos devem gerar árvores de traces completas, onde o estágio de busca registra zero documentos e o estágio de síntese documenta a resposta empática padrão sem erros artificiais.
- **Indisponibilidade do provedor de IA ou do Redis**: Falhas de infraestrutura externa devem ser capturadas nos respectivos spans filhos com o tipo de exceção, enriquecendo o diagnóstico sem mascarar a resposta de erro amigável ao usuário.

---

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: O sistema DEVE disponibilizar um módulo dedicado e isolado para configuração da telemetria em `src/simcc/core/telemetry/`, encapsulando a inicialização de TracerProvider, MeterProvider, Recursos e Exportadores.
- **FR-002**: O sistema DEVE configurar os atributos de identidade do serviço aderindo estritamente às Semantic Conventions do OpenTelemetry: `service.name = simcc-back`, `service.namespace = simcc`, `service.version` e `deployment.environment.name`.
- **FR-003**: O sistema DEVE instrumentar automaticamente a aplicação FastAPI para geração de spans raiz em requisições HTTP, capturando método, rota normalizada, código de status e duração.
- **FR-004**: O sistema DEVE enriquecer a pipeline conversacional da MarIA evoluindo o `AITracer` para emitir spans semânticos filhos cobrindo os estágios:
  - `ai.planner`: Planejamento e extração semântica de intenções e filtros;
  - `ai.retrieval`: Busca vetorial e híbrida de documentos científicos;
  - `ai.cutoff`: Avaliação e filtragem por limiar de distância cosseno;
  - `ai.synthesis`: Geração da síntese conversacional adaptativa.
- **FR-005**: O sistema NÃO DEVE registrar nos spans de telemetria o conteúdo integral de prompts, respostas brutas, documentos científicos completos, comandos SQL com valores parametrizados ou dados pessoais sensíveis de pesquisadores e usuários.
- **FR-006**: O sistema DEVE instrumentar operações com o banco de dados PostgreSQL e com o cache Redis, registrando comandos lógicos e tempos de execução sem gerar cardinalidade excessiva de dimensões.
- **FR-007**: O sistema DEVE instrumentar chamadas HTTP externas a provedores de LLM e serviços externos, capturando a latência de trânsito e identificação do domínio de destino.
- **FR-008**: O sistema DEVE implementar coleta de métricas operacionais com histogramas de latência de requisições HTTP e da IA, contadores de volume de requisições, taxas de acerto/falha de cache e contagem estimada de tokens consumidos.
- **FR-009**: As métricas operacionais NÃO DEVEM utilizar identificadores únicos (`user_id`, `request_id`, `trace_id`) ou textos livres de perguntas como dimensões de rótulo, garantindo estrita baixa cardinalidade.
- **FR-010**: O sistema DEVE preservar integralmente o funcionamento, o formato e a retenção do sistema existente de logs estruturados em JSONL, permitindo que logs e traces coexistam de forma complementar.
- **FR-011**: O sistema DEVE suportar múltiplos exportadores de telemetria configuráveis via variáveis de ambiente, incluindo exportação para console em ambiente de desenvolvimento e exportação OTLP (gRPC/HTTP) para integração com o OpenTelemetry Collector.
- **FR-012**: O sistema DEVE disponibilizar documentação de governança de observabilidade no diretório `docs/` e refletida no `mkdocs.yml`, estabelecendo diretrizes claras sobre segurança, cardinalidade e spans obrigatórios para a equipe.

---

### Key Entities *(include if feature involves data)*

- **TraceContext**: Contexto distribuído contendo `trace_id` e `span_id`, correlacionando todas as operações originadas a partir de uma mesma requisição HTTP ou tarefa assíncrona.
- **SpanDefinition**: Estrutura temporal que delimita o início, fim, status (`UNSET`, `OK`, `ERROR`), atributos semânticos e eventos associados a uma unidade lógica de trabalho (ex: `ai.planner`, `pgvector.search`).
- **OperationalMetric**: Métrica quantitativa agregada (contador ou histograma) configurada com dimensões de baixa cardinalidade para monitoramento contínuo de latência, vazão e recursos.
- **TelemetryResource**: Conjunto padronizado de atributos globais que identificam a instância do serviço emissor perante a malha de observabilidade.

---

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: 100% das requisições HTTP atendidas e 100% das etapas da pipeline da MarIA geram spans correlacionados sob uma mesma árvore hierárquica de trace.
- **SC-002**: 0% de ocorrência de credenciais, dados pessoais ou comandos SQL com valores textuais de usuários em atributos de traces ou métricas.
- **SC-003**: O overhead computacional introduzido pela instrumentação de telemetria não deve exceder 2% da latência média de resposta da aplicação.
- **SC-004**: 100% dos logs estruturados em JSONL continuam sendo emitidos e mantidos sem nenhuma interrupção, quebra de contrato ou alteração de formato.
- **SC-005**: Em caso de indisponibilidade total ou timeout do OpenTelemetry Collector, 100% das requisições da API continuam sendo respondidas normalmente sem erros perceptíveis pelo usuário final.
- **SC-006**: Todas as métricas expostas pelo sistema possuem cardinalidade limitada a conjuntos pré-definidos de categorias e status, com zero dimensões dinâmicas de IDs ou textos livres.

---

## Assumptions

- O projeto utiliza Python 3.13+ gerenciado via Poetry, e as bibliotecas oficiais do OpenTelemetry para Python serão incluídas com versões controladas no `pyproject.toml`.
- O ambiente de desenvolvimento inicial utilizará exportação para console ou endpoint OTLP local para validação dos traces antes do provisionamento de um OpenTelemetry Collector dedicado.
- A retenção temporal de traces e métricas será gerenciada pela infraestrutura do backend de observabilidade externo (ex: Tempo, Prometheus), sem onerar o armazenamento em disco da aplicação local.
- O sistema existente de logs JSONL permanecerá ativo como mecanismo primário de auditoria local com retenção configurada para 14 dias.
