# Gerenciamento de Contexto Assíncrono (ContextVars)

Este guia ensina a utilizar e gerenciar variáveis de contexto dinâmicas (`ContextVars`) para persistir e correlacionar metadados entre diferentes camadas.

## 1. Variáveis de Contexto Disponíveis

O arquivo **[context.py](../../../src/simcc/core/logging/context.py)** expõe as seguintes variáveis:

| Variável | Descrição | Escopo | Ciclo de Vida |
| :--- | :--- | :--- | :--- |
| `request_id_ctx` | Identificador único de rastreabilidade | HTTP / Rotina | Gerado pelo Middleware ou Wrapper de Rotina no início; limpo ao final. |
| `application_ctx`| Nome do serviço executando (padrão: `simcc`) | Global | Configurado na inicialização da aplicação a partir das configurações. |
| `environment_ctx`| Ambiente em execução (padrão: `development`) | Global | Configurado na inicialização a partir das configurações. |
| `hostname_ctx`   | Nome do host do servidor/container | Global | Capturado via biblioteca `socket` no boot da aplicação. |
| `user_id_ctx`    | ID do usuário logado se autenticado | HTTP | Setado dinamicamente durante a validação da autenticação. Limpo ao fim da request. |
| `route_ctx`      | Rota HTTP acionada (ex: `/researchers`) | HTTP | Setado no início pelo Middleware; limpo ao final. |
| `method_ctx`     | Método HTTP da requisição (ex: `POST`) | HTTP | Setado no início pelo Middleware; limpo ao final. |
| `routine_name_ctx`| Nome da rotina ativa (ex: `soap_lattes`) | Rotina | Setado no início pelo Wrapper; limpo ao final. |

---

## 2. Ciclo de Vida do Contexto

1.  **Criação**: No início de uma requisição HTTP ou script CLI, o respectivo orquestrador (middleware ou wrapper) cria e popula as variáveis locais (`request_id`, `route`, `method`, etc.).
2.  **Propagação**: Qualquer chamada a logs em subrotinas chamadas a partir desse fluxo assíncrono herdará implicitamente esses valores em `data` ou no raiz (como `request_id`).
3.  **Alteração**: Serviços e dependências podem alterar valores (como setar `user_id_ctx.set(user_id)` após autenticar o token).
4.  **Limpeza**: O middleware executa `clear_logging_context()` no bloco `finally` para garantir que o lixo de memória da requisição anterior não vaze para a próxima thread em reaproveitamento de conexões assíncronas.

---

## 3. Boas Práticas

*   **Nunca use `request_id_ctx` fora do escopo assíncrono**: certifique-se de que a variável de contexto é manuseada dentro de funções assíncronas (async/await) ou threads compatíveis.
*   **Limpeza explícita**: Garanta que qualquer alteração de variáveis de escopo instanciada por você (como em testes manuais ou novos controladores de threads) seja redefinida ou limpa ao terminar o processamento.

## 4. Como Adicionar um Novo Campo de Contexto

Se for necessário armazenar uma nova variável (exemplo: `tenant_id` para multi-inquilinos):
1.  **Declare o ContextVar em `context.py`**:
    ```python
    tenant_id_ctx: contextvars.ContextVar[Any] = contextvars.ContextVar('tenant_id', default=None)
    ```
2.  **Adicione no dicionário de leitura (`get_logging_context`)**:
    ```python
    def get_logging_context() -> Dict[str, Any]:
        return {
            ...
            'tenant_id': tenant_id_ctx.get(),
        }
    ```
3.  **Atualize o `clear_logging_context`**:
    ```python
    def clear_logging_context() -> None:
        ...
        tenant_id_ctx.set(None)
    ```
4.  **Atualize o mapeamento do schema em `config.py`**:
    Adicione o mapeamento dentro do dicionário `data_dict` no `format_schema_processor` para que a variável apareça automaticamente dentro de `data`:
    ```python
    data_dict = {
        ...
        'tenant_id': ctx.get('tenant_id'),
    }
    ```
