from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from scripts.routines.gender_ai import (
    GPT_4O_MINI_INPUT_PRICE_PER_1M,
    GPT_4O_MINI_OUTPUT_PRICE_PER_1M,
    USD_TO_BRL_RATE,
    GenderInference,
    build_prompt,
    calculate_llm_cost,
    main,
    normalize_gender,
)


@pytest.mark.unit
def test_calculate_llm_cost_exact():
    # 100k input tokens, 10k output tokens
    input_tokens = 100_000
    output_tokens = 10_000

    expected_usd = (
        (input_tokens * GPT_4O_MINI_INPUT_PRICE_PER_1M)
        + (output_tokens * GPT_4O_MINI_OUTPUT_PRICE_PER_1M)
    ) / 1_000_000

    expected_brl = expected_usd * USD_TO_BRL_RATE

    cost_info = calculate_llm_cost(input_tokens, output_tokens)

    assert cost_info['input_tokens'] == input_tokens
    assert cost_info['output_tokens'] == output_tokens
    assert cost_info['total_tokens'] == 110_000
    assert pytest.approx(cost_info['cost_usd'], rel=1e-6) == expected_usd
    assert pytest.approx(cost_info['cost_brl'], rel=1e-6) == expected_brl


@pytest.mark.unit
def test_calculate_llm_cost_zero():
    cost_info = calculate_llm_cost(0, 0)
    assert cost_info['total_tokens'] == 0
    assert cost_info['cost_usd'] == 0.0
    assert cost_info['cost_brl'] == 0.0


@pytest.mark.unit
def test_normalize_gender_valid_and_invalid():
    assert normalize_gender('Homem Cis') == 'Homem Cis'
    assert normalize_gender('Mulher Cis') == 'Mulher Cis'
    assert normalize_gender('Homem Trans') == 'Homem Trans'
    assert normalize_gender('Mulher Trans') == 'Mulher Trans'
    assert normalize_gender('Não Informado') == 'Não Informado'
    assert normalize_gender('  Mulher Cis  ') == 'Mulher Cis'

    # Valores inválidos ou vazios
    assert normalize_gender('') == 'Não Informado'
    assert normalize_gender(None) == 'Não Informado'
    assert normalize_gender('Masculino') == 'Não Informado'
    assert normalize_gender('Feminino') == 'Não Informado'


@pytest.mark.unit
def test_build_prompt_contains_name_and_abstract():
    prompt = build_prompt('Maria Silva', 'Pesquisadora em Bioquímica na UFBA.')
    assert 'Maria Silva' in prompt
    assert 'Pesquisadora em Bioquímica na UFBA.' in prompt
    assert 'Homem Cis' in prompt
    assert 'Mulher Cis' in prompt


@pytest.mark.unit
def test_gender_ai_routine_with_mocked_llm(monkeypatch):
    mock_session = MagicMock()

    res1_id = uuid4()
    res2_id = uuid4()

    mock_researchers = [
        {
            'id': res1_id,
            'name': 'Carlos Silva',
            'lattes_id': '1111222233334444',
            'abstract': 'Professor titular de Engenharia.',
            'custom_attributes': None,
        },
        {
            'id': res2_id,
            'name': 'Ana Souza',
            'lattes_id': '5555666677778888',
            'abstract': 'Sem resumo disponível.',
            'custom_attributes': {'siape': '123456'},
        },
    ]

    mock_execute_result = MagicMock()
    mock_execute_result.mappings.return_value.all.return_value = mock_researchers
    mock_session.execute.return_value = mock_execute_result

    # Mock generator for get_sync_session
    def mock_get_sync_session():
        yield mock_session

    monkeypatch.setattr(
        'scripts.routines.gender_ai.get_sync_session',
        mock_get_sync_session,
    )

    # Mock ChatOpenAI structured_model
    mock_raw = MagicMock()
    mock_raw.usage_metadata = {'input_tokens': 150, 'output_tokens': 12}
    mock_parsed = GenderInference(gender='Homem Cis', reason='Indício claro.')

    mock_structured_model = MagicMock()
    mock_structured_model.invoke.return_value = {
        'parsed': mock_parsed,
        'raw': mock_raw,
    }

    with patch('scripts.routines.gender_ai.ChatOpenAI') as mock_chat_openai:
        mock_chat_instance = MagicMock()
        mock_chat_instance.with_structured_output.return_value = (
            mock_structured_model
        )
        mock_chat_openai.return_value = mock_chat_instance

        report = main(limit=2)

        assert report['items_found'] == 2
        assert report['items_succeeded'] == 2
        assert report['items_failed'] == 0
        assert report['cost_info']['input_tokens'] == 150
        assert report['cost_info']['output_tokens'] == 12
        assert report['cost_info']['total_tokens'] == 162
        assert report['cost_info']['cost_usd'] > 0

        # Verifica commits no banco
        assert mock_session.commit.call_count == 2
