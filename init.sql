BEGIN;

CREATE TABLE alembic_version (
    version_num VARCHAR(32) NOT NULL, 
    CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
);

-- Running upgrade  -> ebd881ecc239

CREATE SCHEMA IF NOT EXISTS admin_ufmg;

CREATE SCHEMA IF NOT EXISTS admin;

CREATE SCHEMA IF NOT EXISTS ufmg;

CREATE SCHEMA IF NOT EXISTS logs;

CREATE EXTENSION IF NOT EXISTS unaccent;

CREATE TABLE admin.feedback (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    name VARCHAR NOT NULL, 
    email VARCHAR NOT NULL, 
    rating INTEGER NOT NULL, 
    description TEXT, 
    PRIMARY KEY (id)
);

CREATE TABLE admin.institution (
    institution_id UUID DEFAULT gen_random_uuid() NOT NULL, 
    name VARCHAR NOT NULL, 
    acronym VARCHAR, 
    lattes_id VARCHAR, 
    PRIMARY KEY (institution_id), 
    UNIQUE (acronym)
);

CREATE TABLE admin.newsletter_subscribers (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    email VARCHAR NOT NULL, 
    subscribed_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(), 
    PRIMARY KEY (id), 
    UNIQUE (email)
);

CREATE TABLE admin.researcher (
    researcher_id UUID DEFAULT gen_random_uuid() NOT NULL, 
    name VARCHAR NOT NULL, 
    lattes_id VARCHAR, 
    extra_field VARCHAR, 
    status BOOLEAN NOT NULL, 
    PRIMARY KEY (researcher_id), 
    UNIQUE (lattes_id)
);

CREATE TABLE admin.roles (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    role VARCHAR NOT NULL, 
    PRIMARY KEY (id), 
    UNIQUE (role)
);

CREATE TABLE admin_ufmg.department (
    dep_id VARCHAR NOT NULL, 
    org_cod VARCHAR, 
    dep_nom VARCHAR, 
    dep_des TEXT, 
    dep_email VARCHAR, 
    dep_site VARCHAR, 
    dep_sigla VARCHAR, 
    dep_tel VARCHAR, 
    img_data VARCHAR, 
    PRIMARY KEY (dep_id)
);

CREATE TABLE admin_ufmg.technician (
    technician_id UUID DEFAULT gen_random_uuid() NOT NULL, 
    full_name VARCHAR, 
    gender VARCHAR, 
    status_code VARCHAR, 
    work_regime VARCHAR, 
    job_class VARCHAR, 
    job_title VARCHAR, 
    job_rank VARCHAR, 
    job_reference_code VARCHAR, 
    academic_degree VARCHAR, 
    organization_entry_date DATE, 
    last_promotion_date DATE, 
    employment_status_description VARCHAR, 
    department_name VARCHAR, 
    career_category VARCHAR, 
    academic_unit VARCHAR, 
    unit_code VARCHAR, 
    function_code VARCHAR, 
    position_code VARCHAR, 
    leadership_start_date DATE, 
    leadership_end_date DATE, 
    current_function_name VARCHAR, 
    function_location VARCHAR, 
    registration_number VARCHAR, 
    ufmg_registration_number VARCHAR, 
    semester_reference VARCHAR, 
    PRIMARY KEY (technician_id)
);

CREATE TABLE country (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    name VARCHAR NOT NULL, 
    name_pt VARCHAR NOT NULL, 
    alpha_2_code VARCHAR(2), 
    alpha_3_code VARCHAR(3), 
    PRIMARY KEY (id), 
    UNIQUE (alpha_2_code), 
    UNIQUE (alpha_3_code), 
    UNIQUE (name), 
    UNIQUE (name_pt)
);

CREATE TABLE graduate_program_ind_prod (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    graduate_program_id UUID NOT NULL, 
    year INTEGER NOT NULL, 
    ind_prod_article FLOAT, 
    ind_prod_book FLOAT, 
    ind_prod_book_chapter FLOAT, 
    ind_prod_software FLOAT, 
    ind_prod_report FLOAT, 
    ind_prod_granted_patent FLOAT, 
    ind_prod_not_granted_patent FLOAT, 
    ind_prod_guidance FLOAT, 
    PRIMARY KEY (id)
);

CREATE TABLE great_area_expertise (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    name VARCHAR NOT NULL, 
    PRIMARY KEY (id)
);

CREATE TABLE institution (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    name VARCHAR NOT NULL, 
    acronym VARCHAR, 
    description VARCHAR, 
    lattes_id VARCHAR, 
    cnpj VARCHAR, 
    image VARCHAR, 
    latitude FLOAT, 
    longitude FLOAT, 
    PRIMARY KEY (id), 
    UNIQUE (acronym), 
    UNIQUE (cnpj), 
    UNIQUE (name)
);

CREATE TABLE jcr (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    rank VARCHAR, 
    journalname VARCHAR, 
    jcryear VARCHAR, 
    abbrjournal VARCHAR, 
    issn VARCHAR, 
    eissn VARCHAR, 
    totalcites VARCHAR, 
    totalarticles VARCHAR, 
    citableitems VARCHAR, 
    citedhalflife VARCHAR, 
    citinghalflife VARCHAR, 
    jif2019 FLOAT, 
    url_revista VARCHAR, 
    PRIMARY KEY (id)
);

CREATE TABLE logs.routine (
    type VARCHAR NOT NULL, 
    error BOOLEAN, 
    detail TEXT, 
    PRIMARY KEY (type)
);

CREATE TABLE periodical_magazine (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    name VARCHAR, 
    issn VARCHAR, 
    qualis VARCHAR, 
    jcr VARCHAR, 
    jcr_link VARCHAR, 
    reference_period VARCHAR, 
    PRIMARY KEY (id)
);

CREATE TABLE research_dictionary (
    research_dictionary_id UUID DEFAULT gen_random_uuid() NOT NULL, 
    term VARCHAR, 
    frequency INTEGER, 
    type_ VARCHAR, 
    PRIMARY KEY (research_dictionary_id)
);

CREATE TABLE research_group (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    name VARCHAR, 
    institution VARCHAR, 
    first_leader VARCHAR, 
    first_leader_id UUID, 
    second_leader VARCHAR, 
    second_leader_id UUID, 
    area VARCHAR, 
    census INTEGER, 
    start_of_collection VARCHAR, 
    end_of_collection VARCHAR, 
    group_identifier VARCHAR, 
    year INTEGER, 
    institution_name VARCHAR, 
    category VARCHAR, 
    PRIMARY KEY (id), 
    UNIQUE (group_identifier), 
    CONSTRAINT uq_research_group_name_institution UNIQUE (name, institution)
);

CREATE TABLE sdg (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    number INTEGER NOT NULL, 
    name VARCHAR NOT NULL, 
    PRIMARY KEY (id)
);

CREATE TABLE seeder_versions (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    table_name VARCHAR NOT NULL, 
    executed_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    PRIMARY KEY (id), 
    UNIQUE (table_name)
);

CREATE TABLE ufmg.departament (
    dep_id VARCHAR NOT NULL, 
    org_cod VARCHAR, 
    dep_nom VARCHAR, 
    dep_des TEXT, 
    dep_email VARCHAR, 
    dep_site TEXT, 
    dep_sigla VARCHAR, 
    dep_tel VARCHAR, 
    PRIMARY KEY (dep_id)
);

CREATE TABLE ufmg.departament_researcher (
    dep_id VARCHAR NOT NULL, 
    researcher_id UUID NOT NULL, 
    PRIMARY KEY (dep_id, researcher_id)
);

CREATE TABLE ufmg.mandate (
    member VARCHAR NOT NULL, 
    departament VARCHAR NOT NULL, 
    mandate VARCHAR, 
    email VARCHAR, 
    phone VARCHAR, 
    PRIMARY KEY (member, departament)
);

CREATE TABLE ufmg.researcher (
    researcher_id UUID NOT NULL, 
    full_name VARCHAR, 
    gender VARCHAR, 
    status_code VARCHAR, 
    work_regime VARCHAR, 
    job_class VARCHAR, 
    job_title VARCHAR, 
    job_rank VARCHAR, 
    job_reference_code VARCHAR, 
    academic_degree VARCHAR, 
    organization_entry_date DATE, 
    last_promotion_date DATE, 
    employment_status_description VARCHAR, 
    department_name VARCHAR, 
    career_category VARCHAR, 
    academic_unit VARCHAR, 
    unit_code VARCHAR, 
    function_code VARCHAR, 
    position_code VARCHAR, 
    leadership_start_date DATE, 
    leadership_end_date DATE, 
    current_function_name VARCHAR, 
    function_location VARCHAR, 
    registration_number VARCHAR, 
    ufmg_registration_number VARCHAR, 
    semester_reference VARCHAR, 
    PRIMARY KEY (researcher_id)
);

CREATE TABLE ufmg.researcher_data (
    cpf VARCHAR NOT NULL, 
    nome VARCHAR, 
    classe INTEGER, 
    nivel INTEGER, 
    inicio TIMESTAMP WITHOUT TIME ZONE, 
    fim TIMESTAMP WITHOUT TIME ZONE, 
    tempo_nivel INTEGER, 
    tempo_acumulado INTEGER, 
    arquivo VARCHAR, 
    PRIMARY KEY (cpf)
);

CREATE TABLE ufmg.technician (
    technician_id UUID DEFAULT gen_random_uuid() NOT NULL, 
    full_name VARCHAR, 
    gender VARCHAR, 
    status_code VARCHAR, 
    work_regime VARCHAR, 
    job_class VARCHAR, 
    job_title VARCHAR, 
    job_rank VARCHAR, 
    job_reference_code VARCHAR, 
    academic_degree VARCHAR, 
    organization_entry_date DATE, 
    last_promotion_date DATE, 
    employment_status_description VARCHAR, 
    department_name VARCHAR, 
    career_category VARCHAR, 
    academic_unit VARCHAR, 
    unit_code VARCHAR, 
    function_code VARCHAR, 
    position_code VARCHAR, 
    leadership_start_date DATE, 
    leadership_end_date DATE, 
    current_function_name VARCHAR, 
    function_location VARCHAR, 
    registration_number VARCHAR, 
    ufmg_registration_number VARCHAR, 
    semester_reference VARCHAR, 
    PRIMARY KEY (technician_id)
);

CREATE TABLE admin.graduate_program (
    graduate_program_id UUID DEFAULT gen_random_uuid() NOT NULL, 
    name VARCHAR NOT NULL, 
    area VARCHAR NOT NULL, 
    modality VARCHAR NOT NULL, 
    institution_id UUID NOT NULL, 
    code VARCHAR, 
    type VARCHAR, 
    rating VARCHAR, 
    state VARCHAR, 
    city VARCHAR, 
    region VARCHAR, 
    url_image VARCHAR, 
    acronym VARCHAR, 
    description TEXT, 
    visible BOOLEAN, 
    site TEXT, 
    menagers VARCHAR[], 
    PRIMARY KEY (graduate_program_id), 
    FOREIGN KEY(institution_id) REFERENCES admin.institution (institution_id), 
    UNIQUE (code)
);

CREATE TABLE admin.permission (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    role_id UUID NOT NULL, 
    permission VARCHAR NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(role_id) REFERENCES admin.roles (id)
);

CREATE TABLE admin.researcher_institution (
    researcher_institution_id UUID DEFAULT gen_random_uuid() NOT NULL, 
    researcher_id UUID NOT NULL, 
    institution_id UUID NOT NULL, 
    start_date DATE, 
    end_date DATE, 
    is_current BOOLEAN, 
    PRIMARY KEY (researcher_institution_id), 
    FOREIGN KEY(institution_id) REFERENCES admin.institution (institution_id), 
    FOREIGN KEY(researcher_id) REFERENCES admin.researcher (researcher_id)
);

CREATE TABLE admin.users (
    user_id UUID DEFAULT gen_random_uuid() NOT NULL, 
    display_name VARCHAR NOT NULL, 
    email VARCHAR NOT NULL, 
    uid VARCHAR NOT NULL, 
    photo_url TEXT, 
    lattes_id VARCHAR, 
    institution_id UUID, 
    provider VARCHAR, 
    linkedin VARCHAR, 
    verify BOOLEAN, 
    shib_id VARCHAR, 
    shib_code VARCHAR, 
    birth_date VARCHAR, 
    course_level VARCHAR, 
    first_name VARCHAR, 
    registration VARCHAR, 
    gender VARCHAR, 
    last_name VARCHAR, 
    email_status VARCHAR, 
    visible_email BOOLEAN, 
    PRIMARY KEY (user_id), 
    FOREIGN KEY(institution_id) REFERENCES admin.institution (institution_id), 
    UNIQUE (email), 
    UNIQUE (uid)
);

CREATE TABLE admin.weights (
    institution_id UUID NOT NULL, 
    a1 FLOAT, 
    a2 FLOAT, 
    a3 FLOAT, 
    a4 FLOAT, 
    b1 FLOAT, 
    b2 FLOAT, 
    b3 FLOAT, 
    b4 FLOAT, 
    c FLOAT, 
    sq FLOAT, 
    book FLOAT, 
    book_chapter FLOAT, 
    software VARCHAR, 
    patent_granted VARCHAR, 
    patent_not_granted VARCHAR, 
    report VARCHAR, 
    f1 FLOAT, 
    f2 FLOAT, 
    f3 FLOAT, 
    f4 FLOAT, 
    f5 FLOAT, 
    PRIMARY KEY (institution_id), 
    FOREIGN KEY(institution_id) REFERENCES admin.institution (institution_id)
);

CREATE TABLE admin_ufmg.department_researcher (
    dep_id VARCHAR NOT NULL, 
    researcher_id UUID NOT NULL, 
    PRIMARY KEY (dep_id, researcher_id), 
    FOREIGN KEY(dep_id) REFERENCES admin_ufmg.department (dep_id), 
    FOREIGN KEY(researcher_id) REFERENCES admin.researcher (researcher_id)
);

CREATE TABLE admin_ufmg.department_technician (
    dep_id VARCHAR NOT NULL, 
    technician_id UUID NOT NULL, 
    PRIMARY KEY (dep_id, technician_id), 
    FOREIGN KEY(dep_id) REFERENCES admin_ufmg.department (dep_id), 
    FOREIGN KEY(technician_id) REFERENCES admin_ufmg.technician (technician_id)
);

CREATE TABLE admin_ufmg.disciplines (
    discipline_id UUID DEFAULT gen_random_uuid() NOT NULL, 
    dep_id VARCHAR, 
    semester VARCHAR, 
    department VARCHAR, 
    academic_activity_code VARCHAR, 
    academic_activity_name VARCHAR, 
    academic_activity_ch VARCHAR, 
    demanding_courses VARCHAR, 
    oft VARCHAR, 
    available_slots VARCHAR, 
    occupied_slots VARCHAR, 
    percent_occupied_slots VARCHAR, 
    schedule VARCHAR, 
    language VARCHAR, 
    researcher_id UUID[], 
    researcher_name VARCHAR[], 
    status VARCHAR, 
    workload VARCHAR[], 
    PRIMARY KEY (discipline_id), 
    FOREIGN KEY(dep_id) REFERENCES admin_ufmg.department (dep_id)
);

CREATE TABLE admin_ufmg.researcher (
    researcher_id UUID NOT NULL, 
    full_name VARCHAR, 
    gender VARCHAR, 
    status_code VARCHAR, 
    work_regime VARCHAR, 
    job_class VARCHAR, 
    job_title VARCHAR, 
    job_rank VARCHAR, 
    job_reference_code VARCHAR, 
    academic_degree VARCHAR, 
    organization_entry_date DATE, 
    last_promotion_date DATE, 
    employment_status_description VARCHAR, 
    department_name VARCHAR, 
    career_category VARCHAR, 
    academic_unit VARCHAR, 
    unit_code VARCHAR, 
    function_code VARCHAR, 
    position_code VARCHAR, 
    leadership_start_date DATE, 
    leadership_end_date DATE, 
    current_function_name VARCHAR, 
    function_location VARCHAR, 
    registration_number VARCHAR, 
    ufmg_registration_number VARCHAR, 
    semester_reference VARCHAR, 
    PRIMARY KEY (researcher_id), 
    FOREIGN KEY(researcher_id) REFERENCES admin.researcher (researcher_id)
);

CREATE TABLE area_expertise (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    name VARCHAR NOT NULL, 
    great_area_expertise_id UUID, 
    PRIMARY KEY (id), 
    FOREIGN KEY(great_area_expertise_id) REFERENCES great_area_expertise (id)
);

CREATE TABLE graduate_program (
    graduate_program_id UUID DEFAULT gen_random_uuid() NOT NULL, 
    name VARCHAR NOT NULL, 
    area VARCHAR NOT NULL, 
    modality VARCHAR NOT NULL, 
    institution_id UUID NOT NULL, 
    code VARCHAR, 
    name_en VARCHAR, 
    basic_area VARCHAR, 
    cooperation_project VARCHAR, 
    type VARCHAR, 
    rating VARCHAR, 
    state VARCHAR, 
    city VARCHAR, 
    region VARCHAR, 
    url_image VARCHAR, 
    acronym VARCHAR, 
    description TEXT, 
    visible BOOLEAN, 
    site TEXT, 
    coordinator VARCHAR, 
    email VARCHAR, 
    start DATE, 
    phone VARCHAR, 
    periodicity VARCHAR, 
    PRIMARY KEY (graduate_program_id), 
    FOREIGN KEY(institution_id) REFERENCES institution (id)
);

CREATE TABLE research_lines (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    research_group_id UUID, 
    title TEXT, 
    objective TEXT, 
    keyword VARCHAR, 
    group_identifier VARCHAR, 
    year INTEGER, 
    predominant_major_area VARCHAR, 
    predominant_area VARCHAR, 
    PRIMARY KEY (id), 
    FOREIGN KEY(research_group_id) REFERENCES research_group (id)
);

CREATE TABLE sdg_alignment (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    reference_id UUID NOT NULL, 
    type VARCHAR NOT NULL, 
    sdg_id UUID NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(sdg_id) REFERENCES sdg (id)
);

CREATE TABLE state (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    name VARCHAR NOT NULL, 
    country_id UUID NOT NULL, 
    abbreviation VARCHAR, 
    PRIMARY KEY (id), 
    FOREIGN KEY(country_id) REFERENCES country (id), 
    UNIQUE (abbreviation), 
    UNIQUE (name)
);

CREATE TABLE ufmg.departament_technician (
    dep_id VARCHAR NOT NULL, 
    technician_id UUID NOT NULL, 
    PRIMARY KEY (dep_id, technician_id), 
    FOREIGN KEY(dep_id) REFERENCES ufmg.departament (dep_id), 
    FOREIGN KEY(technician_id) REFERENCES ufmg.technician (technician_id)
);

CREATE TABLE admin.graduate_program_researcher (
    graduate_program_id UUID NOT NULL, 
    researcher_id UUID NOT NULL, 
    year INTEGER[], 
    type_ VARCHAR, 
    PRIMARY KEY (graduate_program_id, researcher_id), 
    FOREIGN KEY(graduate_program_id) REFERENCES admin.graduate_program (graduate_program_id), 
    FOREIGN KEY(researcher_id) REFERENCES admin.researcher (researcher_id)
);

CREATE TABLE admin.graduate_program_student (
    graduate_program_id UUID NOT NULL, 
    researcher_id UUID NOT NULL, 
    year INTEGER[] NOT NULL, 
    PRIMARY KEY (graduate_program_id, researcher_id, year), 
    FOREIGN KEY(graduate_program_id) REFERENCES admin.graduate_program (graduate_program_id), 
    FOREIGN KEY(researcher_id) REFERENCES admin.researcher (researcher_id)
);

CREATE TABLE admin.users_roles (
    role_id UUID NOT NULL, 
    user_id UUID NOT NULL, 
    PRIMARY KEY (role_id, user_id), 
    FOREIGN KEY(role_id) REFERENCES admin.roles (id), 
    FOREIGN KEY(user_id) REFERENCES admin.users (user_id)
);

CREATE TABLE city (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    name VARCHAR NOT NULL, 
    country_id UUID NOT NULL, 
    state_id UUID, 
    PRIMARY KEY (id), 
    FOREIGN KEY(country_id) REFERENCES country (id), 
    FOREIGN KEY(state_id) REFERENCES state (id)
);

CREATE TABLE research_lines_programs (
    graduate_program_id UUID NOT NULL, 
    name TEXT NOT NULL, 
    area VARCHAR NOT NULL, 
    start_year INTEGER, 
    end_year INTEGER, 
    PRIMARY KEY (graduate_program_id, name), 
    FOREIGN KEY(graduate_program_id) REFERENCES graduate_program (graduate_program_id)
);

CREATE TABLE sub_area_expertise (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    name VARCHAR NOT NULL, 
    area_expertise_id UUID NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(area_expertise_id) REFERENCES area_expertise (id)
);

CREATE TABLE area_specialty (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    name VARCHAR NOT NULL, 
    sub_area_expertise_id UUID NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(sub_area_expertise_id) REFERENCES sub_area_expertise (id)
);

CREATE TABLE researcher (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    name VARCHAR NOT NULL, 
    lattes_id VARCHAR, 
    lattes_10_id VARCHAR, 
    last_update TIMESTAMP WITHOUT TIME ZONE DEFAULT now() NOT NULL, 
    has_image BOOLEAN DEFAULT false NOT NULL, 
    citations VARCHAR, 
    orcid VARCHAR, 
    abstract TEXT, 
    abstract_en TEXT, 
    abstract_ai TEXT, 
    other_information VARCHAR, 
    city_id UUID, 
    country_id UUID, 
    qtt_publications INTEGER, 
    institution_id UUID, 
    graduate_program VARCHAR, 
    graduation VARCHAR, 
    status BOOLEAN DEFAULT true NOT NULL, 
    classification VARCHAR, 
    stars INTEGER DEFAULT 0 NOT NULL, 
    update_abstract BOOLEAN, 
    PRIMARY KEY (id), 
    FOREIGN KEY(city_id) REFERENCES city (id), 
    FOREIGN KEY(country_id) REFERENCES country (id), 
    FOREIGN KEY(institution_id) REFERENCES institution (id), 
    UNIQUE (lattes_10_id), 
    UNIQUE (lattes_id)
);

CREATE TABLE advisory_activity (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    researcher_id UUID NOT NULL, 
    organ_name VARCHAR, 
    start_year VARCHAR, 
    sequence_id INTEGER, 
    organ_code VARCHAR, 
    unit_code VARCHAR, 
    unit_name VARCHAR, 
    specification TEXT, 
    is_current VARCHAR, 
    start_month VARCHAR, 
    end_month VARCHAR, 
    end_year VARCHAR, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE artistic_production (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    researcher_id UUID NOT NULL, 
    title TEXT NOT NULL, 
    type TEXT NOT NULL, 
    year INTEGER, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE bibliographic_production (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    title VARCHAR NOT NULL, 
    type VARCHAR NOT NULL, 
    title_en VARCHAR, 
    doi VARCHAR, 
    nature VARCHAR, 
    year VARCHAR, 
    country_id UUID, 
    language VARCHAR, 
    means_divulgation VARCHAR, 
    homepage VARCHAR, 
    relevance BOOLEAN DEFAULT false NOT NULL, 
    has_image BOOLEAN DEFAULT false NOT NULL, 
    scientific_divulgation BOOLEAN, 
    researcher_id UUID, 
    authors VARCHAR, 
    year_ INTEGER, 
    is_new BOOLEAN, 
    PRIMARY KEY (id), 
    FOREIGN KEY(country_id) REFERENCES country (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE brand (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    title VARCHAR, 
    relevance BOOLEAN DEFAULT false NOT NULL, 
    has_image BOOLEAN DEFAULT false NOT NULL, 
    goal VARCHAR, 
    nature VARCHAR, 
    researcher_id UUID, 
    year INTEGER, 
    is_new BOOLEAN DEFAULT true, 
    stars INTEGER DEFAULT 0, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE didactic_material (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    researcher_id UUID NOT NULL, 
    title TEXT NOT NULL, 
    country VARCHAR, 
    nature VARCHAR, 
    description TEXT, 
    year INTEGER, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE education (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    researcher_id UUID NOT NULL, 
    degree VARCHAR NOT NULL, 
    education_name VARCHAR, 
    education_start INTEGER, 
    education_end INTEGER, 
    key_words VARCHAR, 
    institution VARCHAR, 
    status VARCHAR, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE event_organization (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    title VARCHAR, 
    promoter_institution VARCHAR, 
    nature VARCHAR, 
    researcher_id UUID, 
    local VARCHAR, 
    duration_in_weeks INTEGER, 
    year INTEGER, 
    is_new BOOLEAN DEFAULT true, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE foment (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    researcher_id UUID, 
    modality_code VARCHAR, 
    modality_name VARCHAR, 
    call_title VARCHAR, 
    category_level_code VARCHAR, 
    funding_program_name VARCHAR, 
    institute_name VARCHAR, 
    aid_quantity INTEGER, 
    scholarship_quantity INTEGER, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE graduate_program_researcher (
    graduate_program_id UUID NOT NULL, 
    researcher_id UUID NOT NULL, 
    year INTEGER[], 
    type_ VARCHAR, 
    PRIMARY KEY (graduate_program_id, researcher_id), 
    FOREIGN KEY(graduate_program_id) REFERENCES graduate_program (graduate_program_id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE graduate_program_student (
    graduate_program_id UUID NOT NULL, 
    researcher_id UUID NOT NULL, 
    year INTEGER[] NOT NULL, 
    PRIMARY KEY (graduate_program_id, researcher_id, year), 
    FOREIGN KEY(graduate_program_id) REFERENCES graduate_program (graduate_program_id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE guidance (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    researcher_id UUID, 
    title VARCHAR, 
    nature VARCHAR, 
    oriented VARCHAR, 
    type VARCHAR, 
    status VARCHAR, 
    year INTEGER, 
    is_new BOOLEAN DEFAULT true, 
    stars INTEGER DEFAULT 0, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE industrial_design (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    title VARCHAR NOT NULL, 
    researcher_id UUID, 
    production_sequence INTEGER, 
    title_en VARCHAR, 
    year VARCHAR, 
    country VARCHAR, 
    language VARCHAR, 
    dissemination_medium VARCHAR, 
    homepage VARCHAR, 
    doi VARCHAR, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE labs (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    hashed_id VARCHAR NOT NULL, 
    type VARCHAR NOT NULL, 
    name TEXT NOT NULL, 
    location TEXT, 
    description TEXT, 
    website TEXT, 
    activities TEXT, 
    areas TEXT, 
    campus TEXT, 
    institution_id UUID, 
    researcher_id UUID, 
    responsible TEXT, 
    PRIMARY KEY (id), 
    FOREIGN KEY(institution_id) REFERENCES institution (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE letter_map_or_similar (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    title VARCHAR NOT NULL, 
    researcher_id UUID, 
    production_sequence INTEGER, 
    title_en VARCHAR, 
    year VARCHAR, 
    country VARCHAR, 
    language VARCHAR, 
    dissemination_medium VARCHAR, 
    homepage VARCHAR, 
    doi VARCHAR, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE maintenance_artistic_work (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    title VARCHAR NOT NULL, 
    researcher_id UUID, 
    production_sequence INTEGER, 
    title_en VARCHAR, 
    year VARCHAR, 
    country VARCHAR, 
    language VARCHAR, 
    dissemination_medium VARCHAR, 
    homepage VARCHAR, 
    doi VARCHAR, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE mockup (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    title VARCHAR NOT NULL, 
    researcher_id UUID, 
    production_sequence INTEGER, 
    title_en VARCHAR, 
    year VARCHAR, 
    country VARCHAR, 
    language VARCHAR, 
    dissemination_medium VARCHAR, 
    homepage VARCHAR, 
    doi VARCHAR, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE openalex_researcher (
    researcher_id UUID NOT NULL, 
    h_index INTEGER, 
    relevance_score FLOAT, 
    works_count INTEGER, 
    cited_by_count INTEGER, 
    i10_index INTEGER, 
    scopus VARCHAR(255), 
    orcid VARCHAR(255), 
    openalex VARCHAR(255), 
    PRIMARY KEY (researcher_id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE other_technical_production (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    title VARCHAR NOT NULL, 
    researcher_id UUID, 
    production_sequence INTEGER, 
    title_en VARCHAR, 
    year VARCHAR, 
    country VARCHAR, 
    language VARCHAR, 
    dissemination_medium VARCHAR, 
    homepage VARCHAR, 
    doi VARCHAR, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE participation_events (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    title VARCHAR, 
    event_name VARCHAR, 
    nature VARCHAR, 
    form_participation VARCHAR, 
    type_participation VARCHAR, 
    researcher_id UUID, 
    year INTEGER, 
    is_new BOOLEAN DEFAULT true, 
    stars INTEGER DEFAULT 0, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE patent (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    title VARCHAR, 
    category VARCHAR, 
    relevance BOOLEAN DEFAULT false NOT NULL, 
    has_image BOOLEAN DEFAULT false NOT NULL, 
    development_year VARCHAR, 
    details TEXT, 
    researcher_id UUID, 
    code VARCHAR, 
    grant_date TIMESTAMP WITHOUT TIME ZONE, 
    deposit_date VARCHAR, 
    is_new BOOLEAN DEFAULT true, 
    stars INTEGER DEFAULT 0, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE process_or_technique (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    researcher_id UUID NOT NULL, 
    title TEXT NOT NULL, 
    sequence_id INTEGER, 
    nature VARCHAR, 
    title_en TEXT, 
    year VARCHAR, 
    country VARCHAR, 
    language VARCHAR, 
    dissemination_medium VARCHAR, 
    home_page TEXT, 
    doi VARCHAR, 
    is_relevant BOOLEAN DEFAULT false NOT NULL, 
    has_innovation_potential VARCHAR, 
    purpose TEXT, 
    purpose_en TEXT, 
    availability VARCHAR, 
    funding_institution VARCHAR, 
    city VARCHAR, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE publishing (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    title VARCHAR NOT NULL, 
    researcher_id UUID, 
    production_sequence INTEGER, 
    title_en VARCHAR, 
    year VARCHAR, 
    country VARCHAR, 
    language VARCHAR, 
    dissemination_medium VARCHAR, 
    homepage VARCHAR, 
    doi VARCHAR, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE radio_or_tv_program (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    title VARCHAR NOT NULL, 
    researcher_id UUID, 
    production_sequence INTEGER, 
    title_en VARCHAR, 
    year VARCHAR, 
    country VARCHAR, 
    language VARCHAR, 
    dissemination_medium VARCHAR, 
    homepage VARCHAR, 
    doi VARCHAR, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE registered_cultivar (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    researcher_id UUID NOT NULL, 
    denomination VARCHAR, 
    denomination_en VARCHAR, 
    year INTEGER, 
    country VARCHAR, 
    code VARCHAR, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id), 
    UNIQUE (code)
);

CREATE TABLE relevant_production (
    researcher_id UUID NOT NULL, 
    production_id UUID NOT NULL, 
    type VARCHAR NOT NULL, 
    has_image BOOLEAN NOT NULL, 
    PRIMARY KEY (researcher_id, production_id, type), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE research_group_researcher (
    research_group_id UUID NOT NULL, 
    researcher_id UUID NOT NULL, 
    PRIMARY KEY (research_group_id, researcher_id), 
    FOREIGN KEY(research_group_id) REFERENCES research_group (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE research_project (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    researcher_id UUID NOT NULL, 
    start_year INTEGER, 
    end_year INTEGER, 
    agency_code VARCHAR, 
    agency_name VARCHAR, 
    project_name TEXT, 
    status VARCHAR, 
    nature VARCHAR, 
    number_undergraduates INTEGER DEFAULT 0, 
    number_specialists INTEGER DEFAULT 0, 
    number_academic_masters INTEGER DEFAULT 0, 
    number_phd INTEGER DEFAULT 0, 
    description TEXT, 
    stars INTEGER DEFAULT 0, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE research_report (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    researcher_id UUID, 
    title VARCHAR, 
    project_name VARCHAR, 
    financing_institutionc VARCHAR, 
    year INTEGER, 
    is_new BOOLEAN DEFAULT true, 
    stars INTEGER DEFAULT 0, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE researcher_address (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    researcher_id UUID NOT NULL, 
    city VARCHAR, 
    organ VARCHAR, 
    unity VARCHAR, 
    institution VARCHAR, 
    public_place VARCHAR, 
    district VARCHAR, 
    cep VARCHAR, 
    mailbox VARCHAR, 
    fax VARCHAR, 
    url_homepage VARCHAR, 
    telephone VARCHAR, 
    country VARCHAR, 
    uf VARCHAR, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE researcher_area_expertise (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    researcher_id UUID NOT NULL, 
    sub_area_expertise_id UUID NOT NULL, 
    "order" INTEGER, 
    area_expertise_id UUID, 
    great_area_expertise_id UUID, 
    area_specialty_id UUID, 
    PRIMARY KEY (id), 
    FOREIGN KEY(area_expertise_id) REFERENCES area_expertise (id), 
    FOREIGN KEY(area_specialty_id) REFERENCES area_specialty (id), 
    FOREIGN KEY(great_area_expertise_id) REFERENCES great_area_expertise (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id), 
    FOREIGN KEY(sub_area_expertise_id) REFERENCES sub_area_expertise (id)
);

CREATE TABLE researcher_ind_prod (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    researcher_id UUID NOT NULL, 
    year INTEGER NOT NULL, 
    ind_prod_article FLOAT, 
    ind_prod_book FLOAT, 
    ind_prod_book_chapter FLOAT, 
    ind_prod_software FLOAT, 
    ind_prod_report FLOAT, 
    ind_prod_granted_patent FLOAT, 
    ind_prod_not_granted_patent FLOAT, 
    ind_prod_guidance FLOAT, 
    PRIMARY KEY (researcher_id, year), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE researcher_production (
    researcher_id UUID NOT NULL, 
    city VARCHAR, 
    great_area VARCHAR, 
    great_area_ VARCHAR[], 
    articles INTEGER, 
    book_chapters INTEGER, 
    book INTEGER, 
    patent INTEGER, 
    software INTEGER, 
    brand INTEGER, 
    PRIMARY KEY (researcher_id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE researcher_professional_experience (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    researcher_id UUID NOT NULL, 
    enterprise VARCHAR NOT NULL, 
    start_year INTEGER NOT NULL, 
    end_year INTEGER, 
    employment_type VARCHAR, 
    other_employment_type VARCHAR, 
    functional_classification VARCHAR, 
    other_functional_classification VARCHAR, 
    workload_hours_weekly INTEGER, 
    exclusive_dedication BOOLEAN, 
    additional_info TEXT, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE short_course (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    title VARCHAR NOT NULL, 
    researcher_id UUID, 
    production_sequence INTEGER, 
    title_en VARCHAR, 
    year VARCHAR, 
    country VARCHAR, 
    language VARCHAR, 
    dissemination_medium VARCHAR, 
    homepage VARCHAR, 
    doi VARCHAR, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE short_course_taught (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    title VARCHAR NOT NULL, 
    researcher_id UUID, 
    production_sequence INTEGER, 
    title_en VARCHAR, 
    year VARCHAR, 
    country VARCHAR, 
    language VARCHAR, 
    dissemination_medium VARCHAR, 
    homepage VARCHAR, 
    doi VARCHAR, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE social_media_website_blog (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    title VARCHAR NOT NULL, 
    researcher_id UUID, 
    production_sequence INTEGER, 
    title_en VARCHAR, 
    year VARCHAR, 
    country VARCHAR, 
    language VARCHAR, 
    dissemination_medium VARCHAR, 
    homepage VARCHAR, 
    doi VARCHAR, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE software (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    title VARCHAR, 
    platform VARCHAR, 
    goal VARCHAR, 
    relevance BOOLEAN DEFAULT false NOT NULL, 
    has_image BOOLEAN DEFAULT false NOT NULL, 
    environment VARCHAR, 
    availability VARCHAR, 
    financing_institutionc VARCHAR, 
    researcher_id UUID, 
    year INTEGER, 
    is_new BOOLEAN DEFAULT true, 
    stars INTEGER DEFAULT 0, 
    code VARCHAR, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE technical_work (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    researcher_id UUID NOT NULL, 
    title TEXT NOT NULL, 
    country VARCHAR, 
    nature VARCHAR, 
    funding_institution VARCHAR, 
    duration INTEGER, 
    year INTEGER, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE technical_work_presentation (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    researcher_id UUID NOT NULL, 
    title TEXT NOT NULL, 
    country VARCHAR, 
    nature VARCHAR, 
    year INTEGER, 
    event_name VARCHAR, 
    promoting_institution VARCHAR, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE technical_work_program (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    researcher_id UUID NOT NULL, 
    title TEXT NOT NULL, 
    country VARCHAR, 
    nature VARCHAR, 
    year INTEGER, 
    theme VARCHAR, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE technological_product (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    researcher_id UUID NOT NULL, 
    title TEXT NOT NULL, 
    country VARCHAR, 
    nature VARCHAR, 
    type VARCHAR, 
    year INTEGER, 
    PRIMARY KEY (id), 
    FOREIGN KEY(researcher_id) REFERENCES researcher (id)
);

CREATE TABLE bibliographic_production_article (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    bibliographic_production_id UUID NOT NULL, 
    periodical_magazine_id UUID NOT NULL, 
    volume VARCHAR, 
    fascicle VARCHAR, 
    series VARCHAR, 
    start_page VARCHAR, 
    end_page VARCHAR, 
    place_publication VARCHAR, 
    periodical_magazine_name VARCHAR, 
    issn VARCHAR, 
    qualis VARCHAR, 
    jcr VARCHAR, 
    jcr_link VARCHAR, 
    stars INTEGER, 
    PRIMARY KEY (id), 
    FOREIGN KEY(bibliographic_production_id) REFERENCES bibliographic_production (id), 
    FOREIGN KEY(periodical_magazine_id) REFERENCES periodical_magazine (id)
);

CREATE TABLE bibliographic_production_book (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    bibliographic_production_id UUID NOT NULL, 
    isbn VARCHAR, 
    qtt_volume VARCHAR, 
    qtt_pages VARCHAR, 
    num_edition_revision VARCHAR, 
    num_series VARCHAR, 
    publishing_company VARCHAR, 
    publishing_company_city VARCHAR, 
    stars INTEGER, 
    PRIMARY KEY (id), 
    FOREIGN KEY(bibliographic_production_id) REFERENCES bibliographic_production (id)
);

CREATE TABLE bibliographic_production_book_chapter (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    bibliographic_production_id UUID NOT NULL, 
    book_title VARCHAR, 
    isbn VARCHAR, 
    start_page VARCHAR, 
    end_page VARCHAR, 
    qtt_volume VARCHAR, 
    organizers VARCHAR, 
    num_edition_revision VARCHAR, 
    num_series VARCHAR, 
    publishing_company VARCHAR, 
    publishing_company_city VARCHAR, 
    stars INTEGER, 
    PRIMARY KEY (id), 
    FOREIGN KEY(bibliographic_production_id) REFERENCES bibliographic_production (id)
);

CREATE TABLE bibliographic_production_work_in_event (
    bibliographic_production_id UUID NOT NULL, 
    event_classification VARCHAR, 
    event_name VARCHAR, 
    event_city VARCHAR, 
    event_year INTEGER, 
    proceedings_title VARCHAR, 
    volume VARCHAR, 
    issue VARCHAR, 
    series VARCHAR, 
    start_page VARCHAR, 
    end_page VARCHAR, 
    publisher_name VARCHAR, 
    publisher_city VARCHAR, 
    event_name_english VARCHAR, 
    identifier_number VARCHAR, 
    isbn VARCHAR, 
    stars INTEGER DEFAULT 0, 
    PRIMARY KEY (bibliographic_production_id), 
    FOREIGN KEY(bibliographic_production_id) REFERENCES bibliographic_production (id)
);

CREATE TABLE openalex_article (
    article_id UUID NOT NULL, 
    id UUID NOT NULL, 
    article_institution VARCHAR, 
    issn VARCHAR, 
    authors_institution VARCHAR, 
    abstract TEXT, 
    authors VARCHAR, 
    language VARCHAR, 
    citations_count INTEGER, 
    pdf VARCHAR, 
    landing_page_url VARCHAR, 
    keywords VARCHAR, 
    PRIMARY KEY (article_id), 
    FOREIGN KEY(article_id) REFERENCES bibliographic_production (id)
);

CREATE TABLE process_author (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    process_id UUID NOT NULL, 
    full_name VARCHAR NOT NULL, 
    citation_name VARCHAR, 
    author_order INTEGER, 
    cnpq_id VARCHAR, 
    PRIMARY KEY (id), 
    FOREIGN KEY(process_id) REFERENCES process_or_technique (id)
);

CREATE TABLE process_keyword (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    process_id UUID NOT NULL, 
    keyword TEXT NOT NULL, 
    "order" INTEGER, 
    PRIMARY KEY (id), 
    FOREIGN KEY(process_id) REFERENCES process_or_technique (id)
);

CREATE TABLE process_knowledge_area (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    process_id UUID NOT NULL, 
    major_area VARCHAR, 
    area_name VARCHAR, 
    sub_area_name VARCHAR, 
    specialty_name VARCHAR, 
    "order" INTEGER, 
    PRIMARY KEY (id), 
    FOREIGN KEY(process_id) REFERENCES process_or_technique (id)
);

CREATE TABLE research_project_components (
    id UUID DEFAULT gen_random_uuid() NOT NULL, 
    project_id UUID NOT NULL, 
    name VARCHAR, 
    lattes_id VARCHAR, 
    citations VARCHAR, 
    coordinator BOOLEAN DEFAULT false NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(project_id) REFERENCES research_project (id)
);

CREATE TABLE research_project_foment (
    project_id UUID NOT NULL, 
    agency_name VARCHAR, 
    agency_code VARCHAR, 
    nature VARCHAR, 
    PRIMARY KEY (project_id), 
    FOREIGN KEY(project_id) REFERENCES research_project (id)
);

CREATE TABLE research_project_production (
    project_id UUID NOT NULL, 
    title TEXT, 
    type VARCHAR, 
    PRIMARY KEY (project_id), 
    FOREIGN KEY(project_id) REFERENCES research_project (id)
);

INSERT INTO alembic_version (version_num) VALUES ('ebd881ecc239') RETURNING alembic_version.version_num;

-- Running upgrade ebd881ecc239 -> 7cd5403cc34d

UPDATE alembic_version SET version_num='7cd5403cc34d' WHERE alembic_version.version_num = 'ebd881ecc239';

-- Running upgrade 7cd5403cc34d -> a8f1e23a1104

DROP TABLE seeder_versions;

UPDATE alembic_version SET version_num='a8f1e23a1104' WHERE alembic_version.version_num = '7cd5403cc34d';

-- Running upgrade a8f1e23a1104 -> 137dfa8f8771

ALTER TABLE researcher ADD COLUMN docente BOOLEAN;

UPDATE alembic_version SET version_num='137dfa8f8771' WHERE alembic_version.version_num = 'a8f1e23a1104';

-- Running upgrade 137dfa8f8771 -> 6cc7cda43d27

ALTER TABLE researcher_production ADD COLUMN work_in_event INTEGER;

UPDATE alembic_version SET version_num='6cc7cda43d27' WHERE alembic_version.version_num = '137dfa8f8771';

-- Running upgrade 6cc7cda43d27 -> 5d86e7e7fae8

UPDATE alembic_version SET version_num='5d86e7e7fae8' WHERE alembic_version.version_num = '6cc7cda43d27';

-- Running upgrade 5d86e7e7fae8 -> 0b48abec391b

ALTER TABLE researcher_production ADD COLUMN area_specialty VARCHAR;

UPDATE alembic_version SET version_num='0b48abec391b' WHERE alembic_version.version_num = '5d86e7e7fae8';

-- Running upgrade 0b48abec391b -> 6b6a0bb12051

ALTER TABLE researcher_production ADD COLUMN organ VARCHAR;

UPDATE alembic_version SET version_num='6b6a0bb12051' WHERE alembic_version.version_num = '0b48abec391b';

-- Running upgrade 6b6a0bb12051 -> af7a03c3482f

ALTER TABLE graduate_program_researcher ADD COLUMN tag VARCHAR;

ALTER TABLE graduate_program_researcher DROP CONSTRAINT graduate_program_researcher_graduate_program_id_fkey;

ALTER TABLE graduate_program_researcher DROP CONSTRAINT graduate_program_researcher_researcher_id_fkey;

ALTER TABLE graduate_program_researcher ADD FOREIGN KEY(researcher_id) REFERENCES researcher (id) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE graduate_program_researcher ADD FOREIGN KEY(graduate_program_id) REFERENCES graduate_program (graduate_program_id) ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE researcher ADD COLUMN extra_field VARCHAR;

UPDATE alembic_version SET version_num='af7a03c3482f' WHERE alembic_version.version_num = '6b6a0bb12051';

-- Running upgrade af7a03c3482f -> ddff76fe9416

ALTER TABLE graduate_program_researcher
        ALTER COLUMN year TYPE INT USING year[1];;

UPDATE alembic_version SET version_num='ddff76fe9416' WHERE alembic_version.version_num = 'af7a03c3482f';

-- Running upgrade ddff76fe9416 -> 5b9d6478ca92

ALTER TABLE openalex_article ADD COLUMN created_at TIMESTAMP WITH TIME ZONE NOT NULL;

ALTER TABLE openalex_article ADD COLUMN updated_at TIMESTAMP WITH TIME ZONE NOT NULL;

ALTER TABLE openalex_article ALTER COLUMN article_institution TYPE TEXT;

ALTER TABLE openalex_article ALTER COLUMN authors_institution TYPE TEXT;

ALTER TABLE openalex_article ALTER COLUMN authors TYPE TEXT;

ALTER TABLE openalex_article ALTER COLUMN pdf TYPE TEXT;

ALTER TABLE openalex_article ALTER COLUMN landing_page_url TYPE TEXT;

ALTER TABLE openalex_article ALTER COLUMN keywords TYPE TEXT;

CREATE INDEX idx_openalex_article_issn ON openalex_article (issn);

CREATE INDEX idx_openalex_article_language ON openalex_article (language);

ALTER TABLE openalex_article ADD CONSTRAINT uq_openalex_article_article_id UNIQUE (article_id);

ALTER TABLE openalex_article ADD UNIQUE (article_id);

ALTER TABLE openalex_article DROP CONSTRAINT openalex_article_article_id_fkey;

ALTER TABLE openalex_article ADD FOREIGN KEY(article_id) REFERENCES bibliographic_production (id) ON DELETE CASCADE;

ALTER TABLE openalex_researcher ADD COLUMN created_at TIMESTAMP WITH TIME ZONE NOT NULL;

ALTER TABLE openalex_researcher ADD COLUMN updated_at TIMESTAMP WITH TIME ZONE NOT NULL;

ALTER TABLE openalex_researcher ALTER COLUMN orcid TYPE VARCHAR(19);

CREATE INDEX idx_openalex_researcher_openalex ON openalex_researcher (openalex);

CREATE INDEX idx_openalex_researcher_orcid ON openalex_researcher (orcid);

ALTER TABLE openalex_researcher ADD CONSTRAINT uq_openalex_researcher_orcid UNIQUE (orcid);

ALTER TABLE openalex_researcher ADD CONSTRAINT uq_openalex_researcher_researcher_id UNIQUE (researcher_id);

ALTER TABLE openalex_researcher DROP CONSTRAINT openalex_researcher_researcher_id_fkey;

ALTER TABLE openalex_researcher ADD FOREIGN KEY(researcher_id) REFERENCES researcher (id) ON DELETE CASCADE;

UPDATE alembic_version SET version_num='5b9d6478ca92' WHERE alembic_version.version_num = 'ddff76fe9416';

-- Running upgrade 5b9d6478ca92 -> 384335110060

ALTER TABLE openalex_article DROP COLUMN created_at;

ALTER TABLE openalex_article DROP COLUMN updated_at;

ALTER TABLE openalex_researcher DROP COLUMN created_at;

ALTER TABLE openalex_researcher DROP COLUMN updated_at;

UPDATE alembic_version SET version_num='384335110060' WHERE alembic_version.version_num = '5b9d6478ca92';

-- Running upgrade 384335110060 -> 97f602cb7b01

CREATE EXTENSION IF NOT EXISTS pg_trgm;

UPDATE alembic_version SET version_num='97f602cb7b01' WHERE alembic_version.version_num = '384335110060';

COMMIT;

