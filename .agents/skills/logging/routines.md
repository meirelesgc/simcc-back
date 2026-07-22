# Padronização de Logs em Rotas CLI (Routines)

Este documento descreve como criar, nomear e orquestrar rotinas e scripts de processamento em lote no SimCC para estarem aderentes à arquitetura de logging.

## 1. Visão Geral

Routines são scripts Python executados via linha de comando (ou containers temporários) para processar grandes volumes de dados (ex: leitura de XML do CNPq, importações de lattes, etc.).
Todos os scripts de rotina estão localizados no diretório [scripts/routines/](../../../scripts/routines/).

---

## 2. Como Criar e Nomear uma Nova Rotina

1.  **Criação do Script**: Crie o arquivo Python contendo uma função principal (geralmente `main()`) e o bloco protetor padrão:
    ```python
    def main():
        # Lógica de processamento
        pass

    if __name__ == '__main__':
        main()
    ```
2.  **Convenção de Nome**: O nome do arquivo Python (usando snake_case) será o nome de registro da rotina.
    *   Exemplo de arquivo: `scripts/routines/sync_research_lines.py`
    *   Nome lógico da rotina registrado no log: `sync_research_lines`

---

## 3. Como Utilizar o Wrapper (`run_routine.py`)

Para evitar adicionar código de logging e medição de tempo repetitivo e garantir que erros sejam capturados corretamente, **nunca** chame os scripts Python de rotina diretamente em ambientes de orquestração.

### Execução Correta:
Utilize o wrapper centralizado **[run_routine.py](../../../scripts/routines/run_routine.py)** passando o caminho relativo do script:

`python scripts/routines/run_routine.py sync_research_lines.py`

### O que o Wrapper faz automaticamente:
1.  **Configura o Contexto**: Seta a variável `routine_name_ctx` com o nome da rotina (ex: `sync_research_lines`).
2.  **Log de Início**: Dispara o evento `routine.started`.
3.  **Executa o Script**: Carrega o arquivo dinamicamente, mantendo argumentos extras da CLI (via `sys.argv`).
4.  **Log de Conclusão**: Se terminar com sucesso, calcula a duração e emite `routine.finished`.
5.  **Log de Erro**: Se uma exceção não tratada for lançada, captura o erro, calcula a duração e emite `routine.error` contendo os detalhes da exceção, saindo com código de erro `1` para o sistema operacional.

---

## 4. Integração com Scripts Shell (`pre_hop.sh` e `post_hop.sh`)

Sempre que adicionar um script à lista de rotinas executadas pelos shells de pre-processamento ou pós-processamento, declare-o na lista `ROUTINES` de **[pre_hop.sh](../../../scripts/routines/pre_hop.sh)** ou **[post_hop.sh](../../../scripts/routines/post_hop.sh)**. O loop de execução de ambos os arquivos já utiliza o wrapper `run_routine.py` automaticamente.
