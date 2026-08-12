import argparse
import re
import time
from typing import List, Optional

import polars as pl
import scrapy
from lxml import etree, html
from scrapy.crawler import CrawlerProcess
from sqlalchemy import text

from simcc.core.db.database import get_admin_sync_session
from simcc.core.logging import logger
from simcc.core.logging.context import script_name_ctx
from simcc.core.logging.events import (
    script_finished,
    script_item_error,
    script_progress,
    script_started,
    script_step_finished,
    script_step_started,
)


class SucupiraDocenteSpider(scrapy.Spider):
    name = 'sucupira_docente'
    start_urls = [
        'https://sucupira-legado.capes.gov.br/sucupira/public/consultas/coleta/docente/listaDocente.jsf'
    ]

    def __init__(
        self,
        acronyms: List[str],
        year: int = 2026,
        limit_programs: Optional[int] = None,
        results_container: Optional[list] = None,
        stats_container: Optional[dict] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.acronyms = acronyms
        self.year = year
        self.limit_programs = limit_programs
        self.scraped_items = (
            results_container if results_container is not None else []
        )
        self.stats_container = (
            stats_container if stats_container is not None else {}
        )
        self.stats_container.setdefault('institutions', {})
        self.stats_container.setdefault('total_programs_scraped', 0)
        self.stats_container.setdefault('total_docentes_scraped', 0)
        self.programs_scraped_count = 0
        self.stopped = False

    def parse(self, response):
        if self.stopped:
            return

        inst_idx = response.meta.get('inst_idx', 0)
        if inst_idx >= len(self.acronyms):
            return

        acronym = self.acronyms[inst_idx]
        script_step_started(f'institution_lookup_{acronym}')
        print(f"🔎 [{acronym}] Conectando e pesquisando instituição na Sucupira...")

        view_state = response.xpath(
            '//input[@name="javax.faces.ViewState"]/@value'
        ).get()
        action_url = response.xpath('//form[@id="form"]/@action').get()
        if not action_url or not view_state:
            error_msg = 'Could not find initial ViewState or form action URL'
            print(f"❌ [{acronym}] Erro na página inicial da Sucupira: {error_msg}")
            script_item_error(acronym, error_msg)
            yield from self._next_institution_or_stop(inst_idx, response.url)
            return

        if not action_url.startswith('http'):
            action_url = response.urljoin(action_url)

        ajax_payload = {
            'form': 'form',
            'form:j_idt33:ano': str(self.year),
            'form:j_idt33:inst:input': acronym,
            'javax.faces.ViewState': view_state,
            'javax.faces.partial.ajax': 'true',
            'javax.faces.source': 'form:j_idt33:inst:input',
            'javax.faces.partial.execute': 'form:j_idt33:inst:input',
            'javax.faces.partial.render': 'form:j_idt33:inst:listbox',
            'javax.faces.behavior.event': 'valueChange',
        }

        headers = {
            'Faces-Request': 'partial/ajax',
            'X-Requested-With': 'XMLHttpRequest',
        }

        yield scrapy.FormRequest(
            url=action_url,
            formdata=ajax_payload,
            headers=headers,
            callback=self.parse_inst_lookup,
            meta={
                'acronym': acronym,
                'inst_idx': inst_idx,
                'view_state': view_state,
                'action_url': action_url,
                'cookiejar': inst_idx,
            },
            dont_filter=True,
        )

    def parse_inst_lookup(self, response):
        if self.stopped:
            return

        acronym = response.meta['acronym']
        inst_idx = response.meta['inst_idx']
        action_url = response.meta['action_url']
        view_state = response.meta['view_state']

        try:
            xml_doc = etree.fromstring(response.body)
        except Exception as e:
            err_msg = f'XML parse error: {e}'
            print(f"⚠️  [{acronym}] Erro ao analisar XML de resposta: {err_msg}")
            script_item_error(acronym, err_msg)
            self.stats_container['institutions'][acronym] = {
                'status': 'ERROR',
                'error': err_msg,
                'programs_found': 0,
                'programs_scraped': 0,
                'docentes_scraped': 0,
            }
            yield from self._next_institution_or_stop(inst_idx, action_url)
            return

        vs_update = xml_doc.xpath(
            '//update[@id="javax.faces.ViewState"]/text()'
        )
        if vs_update:
            view_state = vs_update[0]

        listbox_update = xml_doc.xpath(
            '//update[@id="form:j_idt33:inst:listbox"]/text()'
        )
        if not listbox_update or not listbox_update[0]:
            err_msg = 'No listbox update found in XML'
            print(f"⚠️  [{acronym}] Resposta XML sem opções de instituição.")
            script_item_error(acronym, err_msg)
            self.stats_container['institutions'][acronym] = {
                'status': 'NOT_FOUND',
                'full_name': None,
                'programs_found': 0,
                'programs_scraped': 0,
                'docentes_scraped': 0,
            }
            yield from self._next_institution_or_stop(inst_idx, action_url)
            return

        listbox_doc = html.fromstring(listbox_update[0])
        options = listbox_doc.xpath('//option')

        matched_option = None
        for opt in options:
            txt = opt.text or ''
            if f'({acronym})' in txt or acronym.upper() in txt.upper():
                matched_option = opt
                break

        if matched_option is None and options:
            matched_option = options[0]

        if matched_option is None:
            err_msg = f'No institution matching {acronym} found'
            print(f"❌ [{acronym}] Instituição NÃO foi encontrada na Sucupira.")
            logger.warning(f"Institution not found: {acronym}")
            script_item_error(acronym, err_msg)
            self.stats_container['institutions'][acronym] = {
                'status': 'NOT_FOUND',
                'full_name': None,
                'programs_found': 0,
                'programs_scraped': 0,
                'docentes_scraped': 0,
            }
            yield from self._next_institution_or_stop(inst_idx, action_url)
            return

        inst_value_id = matched_option.attrib.get('value')
        full_inst_text = (matched_option.text or '').strip()

        print(f"✅ [{acronym}] Instituição encontrada: '{full_inst_text}'")
        logger.info(f"Institution found: {acronym} -> {full_inst_text}")

        self.stats_container['institutions'][acronym] = {
            'status': 'FOUND',
            'full_name': full_inst_text,
            'programs_found': 0,
            'programs_scraped': 0,
            'docentes_scraped': 0,
        }

        # 2. Select institution and load programs
        ajax_payload = {
            'form': 'form',
            'form:j_idt33:ano': str(self.year),
            'form:j_idt33:inst:input': full_inst_text,
            'form:j_idt33:inst:valueId': inst_value_id,
            'form:j_idt33:inst:listbox': inst_value_id,
            'javax.faces.ViewState': view_state,
            'javax.faces.partial.ajax': 'true',
            'javax.faces.source': 'form:j_idt33:inst:listbox',
            'javax.faces.partial.execute': 'form:j_idt33:inst',
            'javax.faces.partial.render': 'form:j_idt33:inst:inst form:j_idt33:inst:valueId form:j_idt33:programa',
            'javax.faces.behavior.event': 'valueChange',
        }

        headers = {
            'Faces-Request': 'partial/ajax',
            'X-Requested-With': 'XMLHttpRequest',
        }

        yield scrapy.FormRequest(
            url=action_url,
            formdata=ajax_payload,
            headers=headers,
            callback=self.parse_programs,
            meta={
                'acronym': acronym,
                'inst_idx': inst_idx,
                'full_inst_text': full_inst_text,
                'inst_value_id': inst_value_id,
                'view_state': view_state,
                'action_url': action_url,
                'cookiejar': response.meta['cookiejar'],
            },
            dont_filter=True,
        )

    def parse_programs(self, response):
        if self.stopped:
            return

        acronym = response.meta['acronym']
        inst_idx = response.meta['inst_idx']
        action_url = response.meta['action_url']
        full_inst_text = response.meta['full_inst_text']
        inst_value_id = response.meta['inst_value_id']
        view_state = response.meta['view_state']

        try:
            xml_doc = etree.fromstring(response.body)
        except Exception as e:
            err_msg = f'XML parse error: {e}'
            print(f"⚠️  [{acronym}] Erro ao carregar programas (XML): {err_msg}")
            script_item_error(acronym, err_msg)
            yield from self._next_institution_or_stop(inst_idx, action_url)
            return

        vs_update = xml_doc.xpath(
            '//update[@id="javax.faces.ViewState"]/text()'
        )
        if vs_update:
            view_state = vs_update[0]

        prog_update = xml_doc.xpath(
            '//update[@id="form:j_idt33:programa"]/text()'
        )
        if not prog_update or not prog_update[0]:
            print(f"⚠️  [{acronym}] Nenhum bloco de programa retornado na Sucupira.")
            script_item_error(acronym, 'No program update found')
            yield from self._next_institution_or_stop(inst_idx, action_url)
            return

        prog_doc = html.fromstring(prog_update[0])
        selects = prog_doc.xpath('//select')
        if not selects:
            print(f"⚠️  [{acronym}] Select de programas não encontrado na página.")
            script_item_error(acronym, 'No program select element found')
            yield from self._next_institution_or_stop(inst_idx, action_url)
            return

        select_name = selects[0].attrib.get('name')
        options = selects[0].xpath('.//option')

        programs = []
        for opt in options:
            val = opt.attrib.get('value')
            txt = (opt.text or '').strip()
            if val and val != '-1':
                code_m = re.search(r'\b(\d+P\d+)\b', txt)
                if code_m:
                    prog_code = code_m.group(1).strip()
                    programs.append((val, prog_code, txt))
                elif '(' in txt:
                    fallback_m = re.search(r'\((.*?)\)', txt)
                    if fallback_m:
                        prog_code = fallback_m.group(1).strip()
                        programs.append((val, prog_code, txt))

        script_step_finished(
            f'institution_lookup_{acronym}',
            programs_found=len(programs),
        )

        if acronym in self.stats_container['institutions']:
            self.stats_container['institutions'][acronym]['programs_found'] = len(programs)

        if not programs:
            print(f"⚠️  [{acronym}] 0 programas de pós-graduação encontrados.")
            logger.warning(f"No programs found for institution {acronym}")
            yield from self._next_institution_or_stop(inst_idx, action_url)
            return

        print(f"📚 [{acronym}] {len(programs)} programa(s) de pós-graduação encontrado(s):")
        for _, prog_code, prog_text in programs:
            print(f"   • [{prog_code}] {prog_text}")
        logger.info(f"Programs found for {acronym}: {len(programs)}", acronym=acronym, count=len(programs))

        # Start querying programs sequentially for this institution
        first_prog_val, first_prog_code, first_prog_text = programs[0]
        yield self._make_consult_request(
            action_url=action_url,
            full_inst_text=full_inst_text,
            inst_value_id=inst_value_id,
            select_name=select_name,
            prog_val=first_prog_val,
            prog_code=first_prog_code,
            prog_text=first_prog_text,
            view_state=view_state,
            programs=programs,
            prog_index=0,
            acronym=acronym,
            inst_idx=inst_idx,
            cookiejar=response.meta['cookiejar'],
        )

    def _make_consult_request(
        self,
        action_url,
        full_inst_text,
        inst_value_id,
        select_name,
        prog_val,
        prog_code,
        prog_text,
        view_state,
        programs,
        prog_index,
        acronym,
        inst_idx,
        cookiejar,
    ):
        print(f"🔄 [{acronym}] Coletando programa ({prog_index + 1}/{len(programs)}): [{prog_code}] {prog_text}")

        ajax_payload = {
            'form': 'form',
            'form:j_idt33:ano': str(self.year),
            'form:j_idt33:inst:input': full_inst_text,
            'form:j_idt33:inst:valueId': inst_value_id,
            select_name: prog_val,
            'form:consultar': 'Consultar',
            'org.richfaces.ajax.component': 'form:consultar',
            'javax.faces.source': 'form:consultar',
            'javax.faces.partial.ajax': 'true',
            'javax.faces.partial.execute': '@all',
            'javax.faces.partial.render': 'form',
            'javax.faces.behavior.event': 'action',
            'javax.faces.ViewState': view_state,
        }

        headers = {
            'Faces-Request': 'partial/ajax',
            'X-Requested-With': 'XMLHttpRequest',
        }

        return scrapy.FormRequest(
            url=action_url,
            formdata=ajax_payload,
            headers=headers,
            callback=self.parse_consult_result,
            meta={
                'acronym': acronym,
                'inst_idx': inst_idx,
                'action_url': action_url,
                'full_inst_text': full_inst_text,
                'inst_value_id': inst_value_id,
                'select_name': select_name,
                'prog_code': prog_code,
                'prog_text': prog_text,
                'view_state': view_state,
                'programs': programs,
                'prog_index': prog_index,
                'cookiejar': cookiejar,
            },
            dont_filter=True,
        )

    def parse_consult_result(self, response):
        if self.stopped:
            return

        acronym = response.meta['acronym']
        inst_idx = response.meta['inst_idx']
        action_url = response.meta['action_url']
        full_inst_text = response.meta['full_inst_text']
        inst_value_id = response.meta['inst_value_id']
        select_name = response.meta['select_name']
        prog_code = response.meta['prog_code']
        view_state = response.meta['view_state']
        programs = response.meta['programs']
        prog_index = response.meta['prog_index']

        try:
            xml_doc = etree.fromstring(response.body)
        except Exception as e:
            err_msg = f'XML parse error: {e}'
            print(f"⚠️  [{acronym} | {prog_code}] Erro no XML da consulta: {err_msg}")
            script_item_error(
                f'{acronym}_{prog_code}', err_msg
            )
            yield from self._next_institution_or_stop(inst_idx, action_url)
            return

        vs_update = xml_doc.xpath(
            '//update[@id="javax.faces.ViewState"]/text()'
        )
        if vs_update:
            view_state = vs_update[0]

        listagem_update = xml_doc.xpath(
            '//update[@id="form:listagemDocentes"]/text()'
        )
        docentes_count = 0
        if listagem_update and listagem_update[0]:
            doc_html = html.fromstring(listagem_update[0])
            rows = doc_html.xpath('//tbody/tr')
            for tr in rows:
                cols = tr.xpath('.//td')
                if len(cols) >= 2:
                    docente_name = cols[0].text_content().strip()
                    categoria = cols[1].text_content().strip()
                    if docente_name:
                        item = {
                            'docente': docente_name,
                            'categoria': categoria,
                            'codigo_programa': prog_code,
                        }
                        self.scraped_items.append(item)
                        docentes_count += 1
                        yield item

        self.programs_scraped_count += 1
        self.stats_container['programs_scraped_count'] = (
            self.programs_scraped_count
        )
        self.stats_container['total_programs_scraped'] = self.programs_scraped_count
        self.stats_container['total_docentes_scraped'] = len(self.scraped_items)

        if acronym in self.stats_container['institutions']:
            self.stats_container['institutions'][acronym]['programs_scraped'] += 1
            self.stats_container['institutions'][acronym]['docentes_scraped'] += docentes_count

        print(f"   └─ 👥 [{acronym} | {prog_code}] {docentes_count} docente(s) coletado(s)")
        logger.info(
            f"Program {prog_code} scraped: {docentes_count} docentes",
            acronym=acronym,
            prog_code=prog_code,
            docentes_count=docentes_count,
        )

        script_progress(
            step_name=f'scrape_program_{prog_code}',
            current=self.programs_scraped_count,
            total=self.limit_programs or len(programs),
            succeeded=self.programs_scraped_count,
            failed=0,
            docentes_count=docentes_count,
            acronym=acronym,
        )

        inst_programs_scraped = self.stats_container['institutions'][acronym]['programs_scraped']
        if (
            self.limit_programs
            and inst_programs_scraped >= self.limit_programs
        ):
            print(f"🛑 Limite de {self.limit_programs} programa(s) atingido para [{acronym}]. Avançando para próxima instituição...")
            logger.info(
                'Reached limit of programs scraped for institution',
                acronym=acronym,
                programs_scraped=inst_programs_scraped,
                limit=self.limit_programs,
            )
            yield from self._next_institution_or_stop(inst_idx, action_url)
            return

        next_idx = prog_index + 1
        if next_idx < len(programs) and not self.stopped:
            next_val, next_code, next_txt = programs[next_idx]
            yield self._make_consult_request(
                action_url=action_url,
                full_inst_text=full_inst_text,
                inst_value_id=inst_value_id,
                select_name=select_name,
                prog_val=next_val,
                prog_code=next_code,
                prog_text=next_txt,
                view_state=view_state,
                programs=programs,
                prog_index=next_idx,
                acronym=acronym,
                inst_idx=inst_idx,
                cookiejar=response.meta['cookiejar'],
            )
        else:
            yield from self._next_institution_or_stop(inst_idx, action_url)

    def _next_institution_or_stop(self, current_inst_idx: int, url: str):
        next_idx = current_inst_idx + 1
        if next_idx < len(self.acronyms) and not self.stopped:
            yield scrapy.Request(
                url=self.start_urls[0],
                callback=self.parse,
                meta={
                    'inst_idx': next_idx,
                    'cookiejar': next_idx,
                },
                dont_filter=True,
            )


def fetch_institution_acronyms() -> List[str]:
    try:
        for session in get_admin_sync_session():
            res = session.execute(
                text(
                    'SELECT DISTINCT acronym FROM institution '
                    "WHERE acronym IS NOT NULL AND acronym != ''"
                )
            )
            acronyms = [
                row.acronym.strip()
                for row in res
                if row.acronym and row.acronym.strip()
            ]
            if acronyms:
                return acronyms
    except Exception as e:
        logger.warning(f'Could not fetch institution acronyms from DB: {e}')

    return []


def main():
    parser = argparse.ArgumentParser(
        description='Scrape graduate program researchers from Sucupira Capes.'
    )
    parser.add_argument(
        '--acronyms',
        type=str,
        default=None,
        help='Comma-separated institution acronyms (e.g. UFBA,UFMG)',
    )
    parser.add_argument(
        '--output',
        type=str,
        default='storage/seed/program_researchers.csv',
        help='Output CSV file path',
    )
    parser.add_argument(
        '--limit-programs',
        type=int,
        default=3,
        help='Limit number of programs to scrape (default 3, 0 for unlimited)',
    )
    args = parser.parse_args()

    script_name = 'scraping_researchers_pos'
    script_name_ctx.set(script_name)
    script_started(script_name)

    start_time = time.perf_counter()

    print("=" * 80)
    print("🚀 Scraping de Docentes de Pós-Graduação (Sucupira / CAPES)")
    print("=" * 80)

    db_acronyms = fetch_institution_acronyms()

    if not db_acronyms:
        logger.warning('No institution acronyms found in administrative DB.')
        print("⚠️  Nenhuma sigla de instituição encontrada no banco administrativo DB.")

    if args.acronyms:
        requested_acronyms = [
            a.strip() for a in args.acronyms.split(',') if a.strip()
        ]
        db_acronyms_upper = {a.upper() for a in db_acronyms}
        acronyms = [
            a for a in requested_acronyms if a.upper() in db_acronyms_upper
        ]
        ignored = [
            a for a in requested_acronyms if a.upper() not in db_acronyms_upper
        ]
        if ignored:
            logger.warning(
                'Some requested acronyms were not found in administrative DB and will be ignored',
                ignored_acronyms=ignored,
            )
            print(f"⚠️  Siglas ignoradas por não constarem no banco DB: {', '.join(ignored)}")
    else:
        acronyms = db_acronyms

    print(f"📌 Siglas para raspagem ({len(acronyms)}): {', '.join(acronyms) if acronyms else 'Nenhuma'}")
    print(f"📌 Arquivo de saída: {args.output}")
    print(f"📌 Limite de programas: {args.limit_programs if args.limit_programs > 0 else 'Sem limite'}")
    print("=" * 80 + "\n")

    if not acronyms:
        logger.warning('No matching institution acronyms to scrape.')
        print("❌ Nenhuma sigla válida para raspagem. Encerrando.")
        duration_ms = (time.perf_counter() - start_time) * 1000.0
        script_finished(
            script_name,
            duration_ms,
            items_found=0,
            items_succeeded=0,
            items_failed=0,
            programs_scraped=0,
        )
        return

    logger.info('Acronyms to scrape', acronyms=acronyms)

    scraped_results = []
    stats_results = {}

    process = CrawlerProcess(
        settings={
            'USER_AGENT': (
                'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                'AppleWebKit/537.36 (KHTML, like Gecko) '
                'Chrome/120.0.0.0 Safari/537.36'
            ),
            'ROBOTSTXT_OBEY': False,
            'LOG_LEVEL': 'WARNING',
            'DOWNLOADER_CLIENT_CONTEXTFACTORY': (
                'scrapy.core.downloader.contextfactory.InsecureClientContextFactory'
            ),
            'REQUEST_FINGERPRINTER_IMPLEMENTATION': '2.7',
        }
    )

    process.crawl(
        SucupiraDocenteSpider,
        acronyms=acronyms,
        limit_programs=args.limit_programs
        if args.limit_programs > 0
        else None,
        results_container=scraped_results,
        stats_container=stats_results,
    )
    process.start()

    duration_sec = time.perf_counter() - start_time
    duration_ms = duration_sec * 1000.0

    if scraped_results:
        df = pl.DataFrame(scraped_results)
        df.write_csv(args.output)
        logger.info(
            f'Saved {len(scraped_results)} records to {args.output}',
            items_count=len(scraped_results),
            output_file=args.output,
        )
    else:
        df = pl.DataFrame(
            schema={
                'docente': pl.String,
                'categoria': pl.String,
                'codigo_programa': pl.String,
            }
        )
        df.write_csv(args.output)
        logger.warning('No items scraped', output_file=args.output)

    inst_stats = stats_results.get('institutions', {})

    print("\n" + "=" * 80)
    print("📊 RESUMO FINAL DA EXECUÇÃO DA RASPAGEM")
    print("=" * 80)
    print(f"⏱️  Tempo total de execução: {duration_sec:.2f}s")
    print(f"📦 Total de docentes coletados: {len(scraped_results)}")
    print(f"📚 Total de programas raspados: {stats_results.get('total_programs_scraped', 0)}")
    print("-" * 80)
    print("📋 Detalhamento por Instituição:")

    for acr in acronyms:
        info = inst_stats.get(acr)
        if not info:
            print(f"  • {acr}: ❓ Não foi possível consultar")
            continue
        status = info.get('status')
        if status == 'FOUND':
            full_name = info.get('full_name')
            p_found = info.get('programs_found', 0)
            p_scraped = info.get('programs_scraped', 0)
            d_scraped = info.get('docentes_scraped', 0)
            print(f"  • {acr}: ✅ Encontrada ('{full_name}')")
            print(f"    ├─ Programas encontrados: {p_found}")
            print(f"    ├─ Programas raspados: {p_scraped}")
            print(f"    └─ Docentes coletados: {d_scraped}")
        elif status == 'NOT_FOUND':
            print(f"  • {acr}: ❌ NÃO foi encontrada no sistema Sucupira")
        else:
            err = info.get('error', 'Erro desconhecido')
            print(f"  • {acr}: ⚠️  Erro durante a consulta ({err})")

    print("-" * 80)
    print(f"💾 Registros salvos no arquivo: {args.output}")
    print("=" * 80 + "\n")

    script_finished(
        script_name,
        duration_ms,
        items_found=len(scraped_results),
        items_succeeded=len(scraped_results),
        items_failed=0,
        programs_scraped=stats_results.get('programs_scraped_count', 0),
    )


if __name__ == '__main__':
    main()
