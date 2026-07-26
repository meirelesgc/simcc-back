# Skill: Implementação de Endpoints e Camadas de Arquitetura

## Objetivo

Esta skill estabelece os padrões e diretrizes para a criação, organização e validação de novos endpoints no backend do SimCC. Ela assegura consistência estrutural, isolamento de responsabilidades e cobertura robusta de testes para todas as futuras funcionalidades de rotas.

## Quando utilizar esta skill

Esta skill deve ser consultada sempre que:
1. Um novo endpoint ou grupo de rotas for adicionado à API.
2. A lógica de negócio de uma rota existente for modificada ou refatorada.
3. Novas consultas complexas ou integrações de banco de dados forem necessárias.
4. For preciso escrever ou estruturar testes de integração para validar o comportamento de um endpoint.

## Quais arquivos consultar

Consulte a implementação de referência em:
*   **Rotas (Router)**: [researcher.py](../../../src/simcc/routers/researcher.py)
*   **Serviços (Service)**: [researcher_service.py](../../../src/simcc/services/researcher_service.py)
*   **Repositórios (Repository)**: [researcher_repo.py](../../../src/simcc/repositories/researcher_repo.py)
*   **Queries (Query Builder)**: [researcher_query.py](../../../src/simcc/queries/researcher_query.py)
*   **Fábricas de Teste (Factories)**: [factories.py](../../../tests/factories.py)
*   **Geradores de Fixtures (Fixtures)**: [fixtures.py](../../../tests/fixtures.py)
*   **Estrutura de Testes**: [test_researcher.py](../../../tests/routers/test_researcher.py)

## Fluxo de leitura recomendado

Para implementar um novo endpoint com sucesso, siga este fluxo:
1.  **[architecture.md](architecture.md)**: Compreensão da divisão de responsabilidades de cada camada.
2.  **[implementation.md](implementation.md)**: Passo a passo de codificação (Router -> Service -> Repository -> Query Builder).
3.  **[validation.md](validation.md)**: Metodologia de testes usando factories de dados e fixtures criadoras assíncronas.
