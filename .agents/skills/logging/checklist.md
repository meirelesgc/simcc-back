# Checklist de Observabilidade

Utilize este checklist obrigatório antes de marcar qualquer tarefa ou implementação como concluída para garantir que os padrões de logging foram mantidos.

---

## 1. Modificações em Rotas / Endpoints HTTP (APIs)
*   [ ] O middleware de logging está ativo no arquivo [__init__.py](../../../src/simcc/__init__.py).
*   [ ] A rota HTTP emite `request.received` automaticamente na entrada.
*   [ ] A rota HTTP emite `request.finished` automaticamente na conclusão de sucesso (com duração do processamento).
*   [ ] Exceções não tratadas disparam `request.error` com a mensagem do erro.
*   [ ] O cabeçalho `X-Request-ID` é retornado na resposta HTTP do cliente.

---

## 2. Implementação de Novas Rotinas (Processamento em Lote)
*   [ ] A rotina foi criada e reside no diretório `scripts/routines/`.
*   [ ] A execução da rotina ocorre através do wrapper `python scripts/routines/run_routine.py <script.py>`.
*   [ ] O wrapper registra automaticamente o evento `routine.started`.
*   [ ] O wrapper registra automaticamente o evento `routine.finished` ao concluir (com medição de duração).
*   [ ] Exceções causam o registro automático de `routine.error` com detalhes da falha.

---

## 3. Acessos e Consultas ao Banco de Dados (SQLAlchemy)
*   [ ] Não há nenhum log informativo (`info`) registrando o SQL executado com sucesso.
*   [ ] Somente falhas de consulta disparam logs de erro (`query.error`).
*   [ ] O log de erro de banco expõe a operação lógica no formato `ClasseRepo.metodo` ou `ClasseQuery.metodo`.
*   [ ] O SQL completo **nunca** é registrado nos logs em produção (apenas em nível `DEBUG`).
*   [ ] Parâmetros sensíveis não são logados.

---

## 4. Criação de Novos Eventos ou Mapeamentos
*   [ ] O novo evento foi documentado no catálogo oficial em [events.md](events.md).
*   [ ] O evento foi adicionado no enum `LogEvent` de [constants.py](../../../src/simcc/core/logging/constants.py).
*   [ ] A nova função helper foi criada no arquivo [events.py](../../../src/simcc/core/logging/events.py).
*   [ ] Nenhuma string crua de evento foi usada no código de negócio.
