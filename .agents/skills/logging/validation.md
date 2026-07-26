# Guia de Validação e Verificação Técnica

Este documento serve como referência para os agentes validarem se uma implementação ou alteração de código está aderente ao padrão de observabilidade estruturado.

## 1. Validações Obrigatórias antes da Conclusão

Sempre que concluir o desenvolvimento de uma rota, rotina ou ajuste na persistência, o agente de IA deve executar as seguintes etapas de validação:

### A. Validação de Schema e Formato JSONL
*   Verificar se o arquivo de log gerado no dia (`logs/YYYY-MM-DD.jsonl`) contém registros estruturados.
*   Garantir que cada linha represente um JSON válido (sem quebras de linha dentro do objeto, apenas ao final).
*   Garantir a presença de todos os 9 campos obrigatórios de primeiro nível: `timestamp`, `level`, `application`, `category`, `event`, `message`, `request_id`, `duration`, `data`.

### B. Validação de Eventos e Categorias
*   Verificar se os nomes de categoria e evento emitidos estão exatamente presentes na lista oficial de **[events.md](events.md)**.
*   Garantir que nenhum evento foi disparado usando strings em chamadas avulsas do logger (ex: `logger.info("evento.customizado")`). Todos devem obrigatoriamente usar as funções expostas em `events.py`.

### C. Validação de ContextVars e Rastreabilidade
*   Executar testes integrados ou simular chamadas de API e verificar se o `request_id` gerado pelo middleware é compartilhado por todos os logs emitidos no fluxo de execução (como logs de erros de banco de dados internos).
*   Garantir que variáveis de contexto limpam-se de maneira segura ao fim da requisição.

### D. Validação de Banco de Dados e Não Exposição de SQL
*   Simular erros de banco de dados e garantir que a categoria do log seja `database` e o evento seja `query.error`.
*   Garantir que o SQL completo **não** apareça no JSON de log sob nenhum nível além do `debug`.

---

## 2. Como Rodar os Testes de Validação

O projeto possui um arquivo de testes dedicado à observabilidade. Execute-o sempre para garantir conformidade:

`poetry run pytest tests/test_logging.py`

### O que o teste valida:
1.  **test_logger_schema_and_file_creation**: Garante que o arquivo diário é criado automaticamente e segue estritamente o schema JSON global.
2.  **test_context_vars_propagation**: Valida se dados injetados em `contextvars` são repassados de forma transparente.
3.  **test_event_helpers**: Garante que as funções auxiliares em `events.py` aplicam os enums e mapeamentos corretos sem causar exceções no logger.
4.  **test_database_query_error_logging**: Valida a captura de falhas em banco e a não exposição do SQL padrão.
5.  **test_request_id_traceability_across_layers**: Simula uma rota HTTP real com erro interno em banco de dados e comprova a correlação de ID entre HTTP e Database.
