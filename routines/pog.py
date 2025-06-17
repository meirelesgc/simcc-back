import logging
import os

from routines.logger import logger_routine
from simcc.repositories import conn

LOG_PATH = 'logs'

if __name__ == '__main__':
    log_format = '%(levelname)s | %(asctime)s - %(message)s'

    logging.basicConfig(
        filename=os.path.join(LOG_PATH, 'pog.log'),
        filemode='w',
        format=log_format,
        level=logging.DEBUG,
    )

    logger = logging.getLogger(__name__)

    for directory in [LOG_PATH]:
        if not os.path.exists(directory):
            os.makedirs(directory)

    SCRIPT_SQL = """
        UPDATE bibliographic_production_article ba
        SET qualis = 'A4'
        WHERE ba.issn = '17412242';
        """

    conn.exec(SCRIPT_SQL)
    logger.info('Qualis updated for ISSN 17412242')

    SCRIPT_SQL = """
        UPDATE researcher
        SET docente = true
        WHERE id IN (SELECT researcher_id FROM graduate_program_researcher);
        """

    conn.exec(SCRIPT_SQL)
    logger.info('Docentes updated for graduate programs')

    SCRIPT_SQL = """
        UPDATE bibliographic_production_article p
        SET jcr=(subquery.jif2019), jcr_link=url_revista
        FROM (SELECT jif2019, eissn, url_revista FROM JCR) AS subquery
        WHERE translate(subquery.eissn,'-','') = p.issn
        """

    conn.exec(SCRIPT_SQL)
    logger.info('JCR updated for articles')

    SCRIPT_SQL = """
        UPDATE bibliographic_production_article p
        SET jcr = (subquery.jif2019), jcr_link=url_revista
        FROM (SELECT jif2019, issn, url_revista FROM JCR) AS subquery
        WHERE translate(subquery.issn,'-','') = p.issn;
        """

    conn.exec(SCRIPT_SQL)
    logger.info('JCR updated for articles')

    SCRIPT_SQL = """
        UPDATE bibliographic_production
        SET YEAR_ = YEAR::INTEGER
        """

    conn.exec(SCRIPT_SQL)
    logger.info('Year updated for publications')

    SCRIPT_SQL = """
        UPDATE bibliographic_production_article
        SET qualis='B2'
        WHERE issn='26748568' OR issn='2764622'
        """

    conn.exec(SCRIPT_SQL)
    logger.info('Qualis updated for articles')

    SCRIPT_SQL = """
        UPDATE bibliographic_production
        SET title = translate(title, '''', ' ')
        """

    conn.exec(SCRIPT_SQL)
    logger.info('Title updated for publications')

    SCRIPT_SQL = """
        UPDATE periodical_magazine pm
        SET JCR = jcr.JIF2019
        FROM jcr
        WHERE REPLACE(jcr.issn, '-', '') = pm.issn
        AND pm.issn IS NOT NULL;
        """

    conn.exec(SCRIPT_SQL)

    logger.info('Fill periodical magazine JCR column')

    SCRIPT_SQL = """
        UPDATE researcher_production
            SET great_area_ = STRING_TO_ARRAY(great_area, ';');
        """
    conn.exec(SCRIPT_SQL)

    SCRIPT_SQL = """
        WITH ranked AS (
        SELECT id,
                ROW_NUMBER() OVER (
                PARTITION BY researcher_id, enterprise, start_year, end_year,
                                employment_type, other_employment_type,
                                functional_classification, other_functional_classification,
                                workload_hours_weekly, exclusive_dedication, additional_info
                ORDER BY id
                ) AS rn
        FROM public.researcher_professional_experience
        )
        DELETE FROM public.researcher_professional_experience
        WHERE id IN (
        SELECT id
        FROM ranked
        WHERE rn > 1
        );
        """
    conn.exec(SCRIPT_SQL)

    SCRIPT_SQL = """
        UPDATE guidance
        SET title = regexp_replace(title, '<a[^>]*>|</a>', '', 'gi');
        """
    conn.exec(SCRIPT_SQL)

    logger_routine('POG', False)
