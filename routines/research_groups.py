import os
import re
import time
from urllib.parse import parse_qs, urljoin, urlparse

import pandas as pd
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from unidecode import unidecode
from webdriver_manager.chrome import ChromeDriverManager

options = webdriver.ChromeOptions()
options.headless = True
driver = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()), options=options
)
session = requests.Session()
session.headers.update({
    'User-Agent': driver.execute_script('return navigator.userAgent;')
})


driver.get('http://dgp.cnpq.br/dgp/faces/consulta/consulta_parametrizada.jsf')

termo_busca = driver.find_element(
    By.ID, 'idFormConsultaParametrizada:idTextoFiltro'
)
termo_busca.send_keys('%')
botao_pesquisar = driver.find_element(
    By.ID, 'idFormConsultaParametrizada:idPesquisar'
)
botao_pesquisar.click()
time.sleep(20)


csv_file_grupos = 'storage/research_groups/research_groups.csvv'


def extrair_dados():
    items = driver.find_elements(By.CLASS_NAME, 'ui-datalist-item')
    dados_pagina = []
    pesquisadores_pagina = []
    for item in items:
        grupo = (
            item.find_elements(By.CLASS_NAME, 'control-group')[0]
            .find_element(By.CLASS_NAME, 'controls')
            .text.strip()
        )

        originalWin = driver.current_window_handle
        link_pagina_grupo_de_pesquisa = item.find_elements(
            By.CLASS_NAME, 'control-group'
        )[0].find_element(By.TAG_NAME, 'a')
        driver.execute_script(
            'arguments[0].scrollIntoView(true);', link_pagina_grupo_de_pesquisa
        )
        link_pagina_grupo_de_pesquisa.click()
        for window_handle in driver.window_handles:
            if window_handle != originalWin:
                pesquisador_win = window_handle
                break
        driver.switch_to.window(pesquisador_win)
        time.sleep(2)
        driver.get(driver.current_url)
        time.sleep(2)
        for c in driver.get_cookies():
            session.cookies.set(c['name'], c['value'])

        ua = driver.execute_script('return navigator.userAgent')
        session.headers.update({'User-Agent': ua, 'Referer': driver.current_url})
        tbody = driver.find_element(
            By.ID, 'idFormVisualizarGrupoPesquisa:j_idt271_data'
        )
        table_pesquisadores = tbody.find_elements(By.TAG_NAME, 'tr')
        driver.switch_to.window(driver.current_window_handle)
        for row in table_pesquisadores:
            dados = row.find_elements(By.TAG_NAME, 'td')
            nome_pesquisador = dados[0].text.strip()
            titulacao_pesquisador = dados[1].text.strip()
            data_inclusao_pesquisador = dados[2].text.strip()
            link_el = dados[3].find_element(By.TAG_NAME, 'a')
            onclick_js = link_el.get_attribute('onclick')
            form_id, param_name = extrai_form_e_param(onclick_js)

            form = driver.find_element(By.ID, form_id)
            action = form.get_attribute('action')
            action = urljoin(driver.current_url, action)

            hidden_inputs = form.find_elements(
                By.CSS_SELECTOR, "input[type='hidden']"
            )
            payload = {
                inp.get_attribute('name'): inp.get_attribute('value')
                for inp in hidden_inputs
            }
            payload[param_name] = param_name

            resp_post = session.post(action, data=payload, allow_redirects=False)

            loc = resp_post.headers.get('Location', '')

            resp_head = session.head(loc, allow_redirects=False)
            redirect = resp_head.headers.get('Location', '')
            parsed = urlparse(redirect)
            qs = parse_qs(parsed.query)
            lattes = qs.get('id', [''])[0]

            pesquisadores_pagina.append([
                grupo,
                nome_pesquisador,
                titulacao_pesquisador,
                data_inclusao_pesquisador,
                lattes,
            ])
        driver.close()
        time.sleep(5)
        driver.switch_to.window(originalWin)

        instituicao = (
            item.find_elements(By.CLASS_NAME, 'control-group')[1]
            .find_element(By.CLASS_NAME, 'controls')
            .text.strip()
        )

        lider1 = item.find_element(
            By.XPATH,
            ".//label[contains(text(), 'Líder(es):')]/following-sibling::div/a",
        ).text.strip()

        lider2 = ''
        try:
            lider2 = item.find_element(
                By.XPATH,
                ".//label[contains(text(), 'Líder(es):')]/following::div[@class='control-group'][1]//a",
            ).text.strip()
        except:
            pass

        area_element = item.find_elements(
            By.XPATH,
            ".//label[contains(text(), 'Área:')]/following-sibling::div",
        )
        area = area_element[0].text.strip() if area_element else ''

        dados_pagina.append([grupo, instituicao, lider1, lider2, area])

    return dados_pagina, pesquisadores_pagina


def sanitize_filename(s):
    try:
        s = unidecode(s)
    except ImportError:
        pass
    s = s.lower()
    s = re.sub(r'[^a-z0-9]+', '_', s)
    s = re.sub(r'(^_|_$)', '', s)
    return s


def extrai_form_e_param(onclick_js: str):
    form_id = re.search(r"getElementById\('([^']+)'\)", onclick_js).group(1)
    param = re.search(r"\{\s*'([^']+)'\s*:", onclick_js).group(1)
    return form_id, param


if not os.path.exists(csv_file_grupos):
    df_grupos = pd.DataFrame(
        columns=[
            'Grupo de Pesquisa',
            'Instituição',
            'Líder 1',
            'Líder 2',
            'Área',
        ]
    )
    df_grupos.to_csv(csv_file_grupos, index=False, encoding='utf-8-sig')

ultima_pagina = 1
if os.path.exists('ultima_pagina.txt'):
    with open('ultima_pagina.txt', 'r') as f:
        ultima_pagina = int(f.read().strip())

for _ in range(1, ultima_pagina):
    next_button = driver.find_element(By.CLASS_NAME, 'ui-paginator-next')
    if 'ui-state-disabled' in next_button.get_attribute('class'):
        break
    next_button.click()
    time.sleep(30)

pagina_atual = ultima_pagina
while True:
    try:
        dados_pagina, pesquisadores_pagina = extrair_dados()
        df_grupos = pd.DataFrame(
            dados_pagina,
            columns=[
                'Grupo de Pesquisa',
                'Instituição',
                'Líder 1',
                'Líder 2',
                'Área',
            ],
        )
        df_grupos.to_csv(
            csv_file_grupos,
            mode='a',
            header=False,
            index=False,
            encoding='utf-8-sig',
        )

        df_pesquisadores = pd.DataFrame(
            pesquisadores_pagina,
            columns=['Grupo', 'Nome', 'Titulação', 'Data de inclusão', 'Lattes'],
        )

        for nome_grupo, subdf in df_pesquisadores.groupby('Grupo'):
            safe = sanitize_filename(nome_grupo)
            arquivo = f'storage/research_groups/researchers_{safe}.csv'

            if not os.path.exists(arquivo):
                print('Entrei aqui')
                pd.DataFrame(
                    columns=['Nome', 'Titulação', 'Data de inclusão', 'Lattes']
                ).to_csv(arquivo, index=False, encoding='utf-8-sig')

            subdf[['Nome', 'Titulação', 'Data de inclusão', 'Lattes']].to_csv(
                arquivo,
                mode='a',
                header=False,
                index=False,
                encoding='utf-8-sig',
            )

        with open('ultima_pagina.txt', 'w') as f:
            f.write(str(pagina_atual))

        next_button = driver.find_element(By.CLASS_NAME, 'ui-paginator-next')
        if 'ui-state-disabled' in next_button.get_attribute('class'):
            break
        next_button.click()
        time.sleep(30)
        pagina_atual += 1
    except Exception as e:
        print(f'Erro: {e}')
        break

# Fechar o navegador
driver.quit()

print('Dados extraídos e exportados para grupos_pesquisa.csv')
