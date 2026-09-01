# Feature Specification: Melhorias na Interação com IA (MarIA) e Cache Distribuído

**Feature Branch**: `001-ai-chat-redis-cache`

**Created**: 2026-09-01

**Status**: Ready for Review

**Input**: User description: "Pacote de melhorias para a interação com IA: levantamento e documentação da pipeline atual da MarIA, tracing contínuo da pipeline, prompt mais amigável com 3 a 5 variações de resposta conforme o volume e natureza dos dados retornados, linha de corte mínima de qualidade/relevância com aviso empático sobre processamento da base, cache escalável com Redis para múltiplos workers e preservação rigorosa da compatibilidade com o frontend."

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Respostas Humanizadas e Adaptativas da MarIA (Priority: P1)

Como usuário pesquisador, gestor ou cidadão que interage com o chat da MarIA, quero receber respostas empáticas, claras e contextualizadas de acordo com o volume e variedade dos resultados científicos encontrados, para que eu não receba listagens mecânicas ou repetitivas e compreenda rapidamente os achados.

**Why this priority**: A experiência conversacional atual da MarIA é fria e limitada a listagens brutas de registros. Adaptar o estilo da resposta com variações dinâmicas agrega valor imediato à percepção do usuário.

**Independent Test**: Pode ser testado enviando consultas de diferentes naturezas (ampla, específica, comparativa e sem resultados) e verificando se a MarIA adota dinamicamente a estrutura de síntese adequada sem perder a precisão dos fatos.

**Acceptance Scenarios**:

1. **Given** uma consulta com volume elevado de dados encontrados (> 5 registros), **When** a MarIA gera a resposta, **Then** ela deve apresentar uma síntese executiva panorâmica, destacando tendências e principais líderes/instituições, sem listar exaustivamente todos os itens de forma crua.
2. **Given** uma consulta com poucos dados encontrados (1 a 4 registros), **When** a MarIA gera a resposta, **Then** ela deve oferecer uma análise detalhada e rica de cada registro encontrado, contextualizando a importância e produção individual.
3. **Given** uma consulta com resultados heterogêneos/variados (diferentes tipos de produção e instituições), **When** a MarIA gera a resposta, **Then** ela deve categorizar a resposta por eixos temáticos ou por instituição, facilitando a comparação.
4. **Given** uma consulta que não obteve registros na base ou ficou abaixo da linha de corte, **When** a MarIA responde, **Then** ela deve informar de maneira transparente e amigável que a base de dados estadual está em contínuo processamento/indexação, sugerindo termos alternativos ou retorno posterior.

---

### User Story 2 - Linha de Corte de Relevância e Tratamento de Gaps de Qualidade (Priority: P1)

Como usuário da plataforma, quero que a MarIA não apresente resultados forçados, desconexos ou com baixa aderência semântica à minha pergunta, para que eu tenha confiança de que as informações exibidas são verdadeiramente representativas.

**Why this priority**: Evita alucinações e associações espúrias decorrentes de aproximações vetoriais forçadas quando o banco não possui dados sobre o tema pesquisado.

**Independent Test**: Submeter consultas com termos inexistentes ou fora do domínio científico baiano e verificar se o sistema aplica a linha de corte mínima e aciona a mensagem empática de ausência de dados, em vez de listar registros irrelevantes.

**Acceptance Scenarios**:

1. **Given** uma busca cujos documentos recuperados possuem pontuação de similaridade semântica abaixo do limiar de qualidade estabelecido, **When** os dados são avaliados para síntese, **Then** esses registros são descartados como ruído.
2. **Given** que nenhum registro atingiu a linha de corte mínima de relevância, **When** a resposta é elaborada, **Then** o sistema não envia contexto vazio/desconexo ao modelo e emite o aviso padrão amigável de dados em indexação.

---

### User Story 3 - Cache de Alto Desempenho para Múltiplos Workers (Priority: P2)

Como mantenedor e usuário da plataforma, quero que consultas frequentes e idênticas sejam respondidas com baixa latência e sem custo repetitivo de processamento ou tokens de IA, funcionando de forma consistente entre múltiplos workers da aplicação.

**Why this priority**: A aplicação roda com 4 a 6 workers simultâneos e atende a consultas analíticas pesadas e repetitivas. Uma camada de cache distribuído acelera a experiência e reduz custos operacionais.

**Independent Test**: Executar a mesma consulta de chat em instâncias de workers distintas e comprovar que a segunda requisição é servida via cache com tempo de resposta quase instantâneo e sem novas invocações ao provedor de IA.

**Acceptance Scenarios**:

1. **Given** uma requisição de chat já processada anteriormente dentro do tempo de expiração configurado, **When** a mesma consulta for submetida, **Then** a resposta completa e seus metadados associados são retornados instantaneamente a partir do cache.
2. **Given** uma requisição de streaming via Server-Sent Events para uma consulta já cacheada, **When** o cliente consome o stream, **Then** os eventos estruturados (metadados, deltas e conclusão) são reproduzidos fielmente sem refazer as chamadas aos serviços de IA.
3. **Given** uma falha temporária ou indisponibilidade do serviço de cache, **When** uma consulta for realizada, **Then** o sistema deve realizar fallback gracioso para a execução normal sem gerar interrupção de serviço ao usuário.

---

### User Story 4 - Rastreabilidade e Tracing Contínuo da Pipeline de IA (Priority: P2)

Como engenheiro e mantenedor da plataforma, quero ter visibilidade e rastreabilidade sobre cada etapa da pipeline de IA (planejamento, busca híbrida, triagem de qualidade e síntese), para diagnosticar gargalos, analisar custos e promover melhorias contínuas.

**Why this priority**: Sem telemetria estruturada dos estágios da IA, é impossível quantificar a eficácia dos prompts, a latência de cada etapa ou a qualidade dos filtros gerados pelo planner.

**Independent Test**: Realizar uma interação de chat e verificar se os registros estruturados de observabilidade capturam os tempos de execução por estágio, status da linha de corte e status de cache.

**Acceptance Scenarios**:

1. **Given** a execução de uma consulta no chat, **When** a pipeline é processada, **Then** eventos estruturados registram a duração individual de cada fase (planejamento de consulta, busca híbrida vetorial, triagem de relevância e geração de resposta).
2. **Given** a ocorrência de erro em qualquer etapa da pipeline, **When** a falha é capturada, **Then** o rastreamento registra a causa, a etapa específica e emite mensagem de erro padronizada para o cliente.

---

### User Story 5 - Preservação Estrita de Contratos de API com o Frontend (Priority: P3)

Como usuário e desenvolvedor do frontend, quero que todos os formatos de dados, schemas e eventos de streaming já existentes continuem funcionando sem nenhuma alteração no cliente visual.

**Why this priority**: O frontend atual está em produção estável e não deve sofrer quebras de contrato.

**Independent Test**: Validar com a suíte de testes de integração e ponta a ponta se as respostas dos endpoints `/ai/chat/ask` e `/ai/chat/ask/stream` mantêm rigorosamente todos os campos e estruturas JSON esperadas.

**Acceptance Scenarios**:

1. **Given** uma requisição JSON para `/ai/chat/ask`, **When** a resposta é retornada, **Then** o payload contém exatamente os campos `answer`, `intent`, `filters_extracted`, `researchers`, `productions` e `sources`.
2. **Given** uma requisição para `/ai/chat/ask/stream`, **When** o fluxo é aberto, **Then** os eventos SSE seguem exatamente a tipagem `metadata`, `delta`, `done` e `error`.

---

### Edge Cases

- **Entrada com caracteres especiais ou saudações simples**: Consultas como "Olá", "Boa tarde", ou pontuações avulsas devem ser tratadas pelo planejador como intenção conversacional/geral sem disparar buscas vetoriais pesadas.
- **Consultas sobre instituições ou temas inexistentes na Bahia**: Quando filtros apontarem para dados não catalogados, o sistema deve acionar a tratativa amigável de base em processamento sem quebras.
- **Queda ou latência na conexão com Redis**: Em caso de timeout ou indisponibilidade do cache distribuído, a aplicação não deve travar, operando normalmente em modo direto (cache bypass) com emissão de warning no log.
- **Cancelamento de conexão do cliente durante o streaming**: Quando o usuário fecha o navegador ou interrompe o chat no meio da resposta, o backend deve tratar o encerramento suavemente sem deixar conexões ou tarefas órfãs.
- **Valores limítrofes na linha de corte**: Registros na fronteira exata do limiar de similaridade devem ser tratados com critérios consistentes e determinísticos.

---

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: O sistema DEVE disponibilizar documentação arquitetural e de fluxo operacional da pipeline da MarIA detalhando parâmetros de entrada, estágios de processamento, regras de inferência e modelos de saída.
- **FR-002**: A pipeline de IA DEVE emitir registros estruturados de observabilidade (tracing) contendo identificador de correlação, tempo por estágio (planejamento, busca, corte de qualidade, geração) e status de execução.
- **FR-003**: O prompt de síntese da MarIA DEVE adotar um tom amigável, profissional, empático e acolhedor, eliminando o comportamento mecânico de listagem pura e simples.
- **FR-004**: O sistema DEVE fornecer estratégias adaptativas de formatação de resposta com no mínimo quatro variações comportamentais baseadas no contexto:
  - *Variação A (Grande Volume)*: Síntese panorâmica executiva com destaques dos principais pesquisadores e tendências.
  - *Variação B (Volume Reduzido)*: Análise descritiva detalhada de cada registro relevante encontrado.
  - *Variação C (Heterogênea/Multidisciplinar)*: Agrupamento comparativo estruturado por instituições ou eixos temáticos.
  - *Variação D (Vazia ou Inconclusiva)*: Mensagem empática informando que a base de dados está em contínuo processamento e sugerindo refinamento.
- **FR-005**: O sistema DEVE aplicar uma linha de corte mínima de relevância semântica sobre os documentos recuperados por busca vetorial, descartando itens com distância excessiva ou baixa correspondência.
- **FR-006**: Quando nenhum documento atender ao limiar mínimo de relevância, o sistema DEVE retornar a resposta padrão de base em processamento, evitando alucinações ou resumos sobre dados irrelevantes.
- **FR-007**: O sistema DEVE implementar uma camada de cache distribuído baseada em Redis para armazenar respostas de chat e dados intermediários pesados.
- **FR-008**: O mecanismo de cache DEVE ser projetado com interface modular e extensível, permitindo que a mesma infraestrutura seja facilmente reaproveitada em outros endpoints da API no futuro.
- **FR-009**: O cache DEVE operar de forma confiável em ambientes com múltiplos workers concorrentes (4 a 6 workers) sem inconsistência de estado.
- **FR-010**: O sistema DEVE suportar cache tanto para respostas síncronas (`/ai/chat/ask`) quanto para fluxos em streaming (`/ai/chat/ask/stream`), preservando a emissão sequencial dos eventos SSE.
- **FR-011**: O cache DEVE possuir política de tempo de vida (TTL) configurável via variáveis de ambiente, além de mecanismo de graceful degradation caso o servidor Redis esteja inacessível.
- **FR-012**: O sistema DEVE manter estrita compatibilidade retroativa com os contratos de API consumidos pelo frontend atual, incluindo os schemas `ChatResponse`, `ChatStreamEvent`, `SearchUIMetadata` e endpoints legados.

---

### Key Entities *(include if feature involves data)*

- **Interação de Chat (ChatInteractionContext)**: Representa o contexto unificado de uma pergunta do usuário, incluindo identificador de sessão, texto da consulta, plano estruturado extraído, documentos recuperados e metadados de UI.
- **Plano de Consulta (QueryPlan)**: Estrutura semântica gerada pelo planejador com a intenção classificada (`intent`), termos conceituais (`semantic_query`) e filtros relacionais (`filters`).
- **Política de Corte de Qualidade (QualityCutoffPolicy)**: Conjunto de regras e limiares de distância cosseno/relevância que determinam se um documento é elegível para compor a síntese da resposta.
- **Registro de Rastreabilidade (AITraceRecord)**: Conjunto de métricas e marcas temporais que documenta a passagem da requisição pelos estágios da pipeline da IA.
- **Entrada de Cache (AICacheEntry)**: Payload serializado contendo o resultado da consulta, metadados de UI e sequência de blocos de resposta associados a uma chave canônica de busca.

---

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: Redução de pelo menos 60% na latência percebida em consultas idênticas e recorrentes por meio do reaproveitamento de respostas em cache.
- **SC-002**: Redução de no mínimo 40% no consumo de tokens e chamadas a modelos de linguagem externos para padrões de consultas repetitivas.
- **SC-003**: 100% das requisições com ausência de dados relevantes na base retornam uma mensagem empática e orientadora sobre a indexação progressiva dos dados, com 0% de respostas alucinadas com dados fora de escopo.
- **SC-004**: 100% de compatibilidade retroativa confirmada com o frontend, sem alteração de schemas públicos ou quebra de fluxo em streaming.
- **SC-005**: 100% das etapas da pipeline de IA geram registros estruturados de observabilidade e métricas de tempo por estágio.
- **SC-006**: Em caso de indisponibilidade total do Redis, 100% das requisições continuam sendo atendidas com fallback automático para execução direta.

---

## Assumptions

- O ambiente de execução possui acesso a um servidor Redis (local ou contêiner) acessível pelos múltiplos workers da API.
- A biblioteca cliente do Redis (`redis-py` com suporte assíncrono `redis.asyncio`) será adicionada como dependência de projeto.
- O limiar de corte de qualidade semântica pode ser parametrizado por configuração para ajuste fino baseado no feedback contínuo.
- O formato de Server-Sent Events (SSE) adotado pelo frontend espera os tipos de evento `metadata`, `delta`, `error` e `done`.
- A documentação gerada será escrita em português humanizado e disponibilizada na pasta `docs/` compatível com o MkDocs.
