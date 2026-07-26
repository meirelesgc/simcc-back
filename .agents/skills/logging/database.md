# Padronização de Logs em Banco de Dados (Persistência)

Este guia estabelece os padrões e as convenções para monitoramento de banco de dados no SimCC.

## 1. Regra de Produção: Apenas Falhas

*   **Proibido registrar logs informativos de SQL em produção**: Consultas bem-sucedidas ocorrem em alta frequência e saturam o armazenamento. O volume de logs de banco deve ser o menor possível.
*   **Registrar somente falhas**: Apenas erros de sintaxe SQL, timeouts ou quedas de conexão devem gerar registros de log de categoria `database` (`query.error`).
*   **Segurança de Dados**: Parâmetros confidenciais e comandos SQL completos **não devem** ser inclusos no JSON de logs em produção.

---

## 2. Funcionamento do Interceptador de Erros do SQLAlchemy

A captura de erros é 100% automatizada e centralizada. Em vez de adicionar blocos `try/except` repetitivos em todas as classes repositórios, utilizamos ouvintes de eventos (listeners) do SQLAlchemy:

1.  **Medição de Tempo**: No evento `before_cursor_execute`, salvamos o timestamp de início no contexto da conexão.
2.  **Captura de Erro**: Se uma consulta falhar, o evento `handle_error` intercepta o problema e:
    *   Calcula a duração total em milissegundos.
    *   Obtém a mensagem de exceção gerada pelo driver.
    *   Descobre o nome lógico da operação via inspeção de pilha (call stack).
    *   Invoca `query_error()` para despachar o log estruturado.

---

## 3. Identificação do Nome Lógico da Operação

Para diagnosticar erros rapidamente, a chave `operation_name` dentro de `data` identifica qual repositório ou classe query iniciou a chamada. O interceptador inspeciona a pilha de execução (call stack) procurando por pacotes específicos:

*   Se o erro ocorrer em `src/simcc/repositories/researcher_repo.py` no método `find_by_filters`, a operação lógica é rotulada como: `ResearcherRepo.find_by_filters` ou `researcher_repo.find_by_filters`.
*   Se o erro ocorrer em `src/simcc/queries/metrics_query.py` no método `execute`, a operação é rotulada como: `MetricsQuery.execute`.
*   Caso a consulta falhe fora desses pacotes conhecidos, ela assume o valor genérico `database.query`.

---

## 4. Regras para o Uso de Modo DEBUG

*   **Modo DEBUG**: Caso a variável `LOG_LEVEL` da aplicação seja configurada como `debug` (geralmente em ambiente de desenvolvimento local), o interceptador incluirá a chave `sql` sob `data` contendo a query SQL completa que falhou para fins de debug rápido.
*   **Modo Produção (INFO/WARNING/ERROR)**: O campo `sql` **não é** enviado no JSON final, salvando o diagnóstico apenas com a mensagem do banco de dados (ex: `relation "users" does not exist`) e a operação lógica.
