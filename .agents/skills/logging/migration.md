# Guia de Migração de Código Legado

Este documento orienta desenvolvedores e agentes a migrar e adaptar logs antigos do sistema para a nova arquitetura estruturada.

## 1. Estratégia de Migração Incremental

A migração de logs não deve ser feita de forma invasiva e em lote em arquivos cruciais de uma só vez. A estratégia recomendada é a **migração incremental por entrega**:
*   Sempre que editar um repositório, serviço ou roteador para implementar uma nova funcionalidade, verifique se existem logs antigos nele e migre-os.
*   Valide cada etapa rodando os testes locais antes de prosseguir.

---

## 2. Substituição de Logs Antigos e Remoção de Strings

O objetivo principal é erradicar o uso de logs informativos que usam strings soltas ou que acessam diretamente bibliotecas externas de log.

### Antes (Inadequado):
```python
import logging

logger = logging.getLogger(__name__)

def update_researcher_data(researcher_id):
    logger.info(f"Iniciando atualizacao do pesquisador {researcher_id}")
    # ...
    logger.info("Atualizacao finalizada com sucesso")
```

### Depois (Adequado):
Utilize o wrapper de rotinas se o processamento for uma rotina completa, ou use logs de sistema centralizados se for uma operação geral. Se for uma rotina:

```python
# A execução do script todo é encapsulada em scripts/routines/run_routine.py
# Sem necessidade de logs internos repetitivos e poluentes!
```

Caso seja necessário logar um evento sistêmico específico dentro de uma classe de serviço:
1.  Verifique se o evento está mapeado em `constants.py` e se possui helper em `events.py`.
2.  Importe e chame o helper correspondente:
    ```python
    from simcc.core.logging.events import routine_started, routine_finished
    ```

---

## 3. Substituição de Blocos Try-Except de Banco nos Repositórios

Com os hooks do SQLAlchemy ativos globally, os blocos `try/except` que serviam apenas para registrar erros de banco devem ser limpos, pois o SQLAlchemy disparará o evento `query.error` de maneira automática.

### Antes (Inadequado):
```python
async def get_data(self):
    try:
        result = await self.session.execute(text("SELECT ..."))
        return result.mappings().all()
    except Exception as e:
        logger.error(f"Erro ao buscar dados: {str(e)}") # Log duplicado e não padronizado
        raise e
```

### Depois (Adequado):
```python
async def get_data(self):
    # O listener global de handle_error no SQLAlchemy já captura, mede tempo,
    # descobre o nome lógico da operação (ex: "ResearcherRepo.get_data") e loga!
    result = await self.session.execute(text("SELECT ..."))
    return result.mappings().all()
```
Isso simplifica o código de negócio drasticamente, eliminando redundâncias.
