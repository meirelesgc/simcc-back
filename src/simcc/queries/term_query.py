from simcc.queries.base import BaseQuery


class OriginalWordsQuery(BaseQuery):
    def __init__(self, session, initials: str, type_: str):
        super().__init__(session)
        self.initials = initials.lower()
        self.type_ = type_.upper()
        self.params['initials'] = self.initials

    def build_sql(self) -> str:
        if self.type_ == 'NAME':
            return """
                SELECT
                    name AS term,
                    0 AS frequency,
                    '0' AS type
                FROM
                    researcher
                WHERE
                    unaccent(LOWER(name)) LIKE unaccent(:initials) || '%'
                ORDER BY name
                LIMIT 300
            """
        elif self.type_ == 'AREA':
            return """
                SELECT name AS term,
                    COUNT(*) AS frequency,
                    'AREA_SPECIALTY' AS type
                FROM public.area_specialty
                WHERE unaccent(LOWER(name)) LIKE unaccent(:initials) || '%'
                GROUP BY name

                UNION

                SELECT name AS term,
                    COUNT(*) AS frequency,
                    'AREA_EXPERTISE' AS type
                FROM public.area_expertise
                WHERE unaccent(LOWER(name)) LIKE unaccent(:initials) || '%'
                GROUP BY name

                UNION

                SELECT name AS term,
                    COUNT(*) AS frequency,
                    'SUB_AREA_EXPERTISE' AS type
                FROM public.sub_area_expertise
                WHERE unaccent(LOWER(name)) LIKE unaccent(:initials) || '%'
                GROUP BY name
                ORDER BY frequency DESC
                LIMIT 300;
            """
        else:
            filter_type = 'type_ = :type'
            if self.type_ == 'BOOK':
                filter_type = "(type_ = 'BOOK' OR type_ = 'BOOK_CHAPTER')"

            self.params['type'] = self.type_

            return f"""
                SELECT DISTINCT unaccent(term) AS term, COUNT(frequency) AS frequency, type_ AS type
                FROM research_dictionary r
                WHERE 
                    {filter_type}
                    AND unaccent(LOWER(term)) LIKE unaccent(:initials) || '%'
                    AND term ~ '^[^0-9]+$'
                GROUP BY 
                    unaccent(term), type_
                ORDER BY 
                    frequency DESC
                FETCH FIRST 300 ROWS ONLY
            """
