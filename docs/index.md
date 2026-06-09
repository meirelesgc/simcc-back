# Simcc Documentation

Bem-vindo à documentação técnica do projeto SIMCC. Este guia é destinado a mantenedores e desenvolvedores que desejam entender a estrutura interna, os padrões arquiteturais e as práticas de testes adotadas.

## Como Iniciar

### Ambiente de Desenvolvimento
O projeto utiliza `poetry` para gerenciamento de dependências.

```bash
# Instalar dependências
poetry install

# Rodar servidor local (FastAPI)
poetry run uvicorn simcc:app --reload
```

### Rodando Testes
Sempre execute os testes antes de realizar um commit.

```bash
poetry run pytest
```
