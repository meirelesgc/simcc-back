# Skill: Logging Estruturado e Observabilidade

## Objetivo

Esta skill visa manter as convenções e padrões estabelecidos para a arquitetura de observabilidade do SimCC baseada em logs estruturados em JSONL. Ela garante que todas as partes do sistema gerem registros consistentes, rastreáveis e seguros.

## Quando utilizar esta skill

Esta skill deve ser ativada e consultada sempre que:
1. Uma nova funcionalidade ou rota HTTP for criada/alterada.
2. Uma nova rotina ou script de processamento em lote for implementada/alterada.
3. Ocorrer alteração na camada de banco de dados, persistência ou execução de consultas (SQLAlchemy).
4. Ocorrerem alterações na configuração central de log ou novos destinos de persistência (handlers) forem sugeridos.

## Quais arquivos consultar

Consulte a implementação de referência em:
*   [Módulo de Logging](../../../src/simcc/core/logging/)
*   [Middleware de API](../../../src/simcc/core/logging/middleware.py)
*   [Wrapper de Rotinas](../../../scripts/routines/run_routine.py)
*   [Configuração de Banco](../../../src/simcc/core/db/database.py)
*   [Arquivo de Testes](../../../tests/test_logging.py)

## Fluxo de leitura recomendado

Para compreender e validar implementações de observabilidade, siga este fluxo:
1.  **[architecture.md](architecture.md)**: Visão geral da arquitetura de fluxo de logs.
2.  **[schema.md](schema.md)**: Detalhe das chaves estruturadas obrigatórias e aninhamento sob `data`.
3.  **[events.md](events.md)**: Nomenclatura oficial de eventos e categorias.
4.  **[checklist.md](checklist.md)**: Guia rápido de verificação técnica antes da entrega de novas tarefas.

## Escopo de Responsabilidade

Esta skill é responsável apenas pelas convenções e validações do logging estruturado do sistema backend (incluindo rotinas e persistência), preparando as bases para correlacionar logs com o frontend futuramente.
