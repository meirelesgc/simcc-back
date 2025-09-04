BEGIN;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;


CREATE TYPE RELATIONSHIP AS ENUM ('COLABORADOR', 'PERMANENTE');
CREATE TYPE NOTIFICATION_TYPE AS ENUM (
    'NEW_PRODUCTION',
    'USER_FOLLOWED',
    'PRODUCTION_LIKED',
    'LATTES_REMINDER',
    'ORCID_REMINDER',
    'NEW_LOGIN',
    'MESSAGE',
    'SYSTEM'
);

CREATE TABLE public.institution (
  institution_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  name VARCHAR(255) NOT NULL,
  acronym VARCHAR(16) UNIQUE,
  lattes_id VARCHAR(16),
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW(),
  deleted_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS users (
    user_id UUID NOT NULL,
    orcid_id CHAR(20),
    username VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL,

    provider VARCHAR(255),
    verify BOOLEAN NOT NULL DEFAULT FALSE,
    institution_id UUID,

    photo_url TEXT,
    lattes_id VARCHAR(16),
    linkedin VARCHAR(255),

    profile_image_url TEXT,
    background_image_url TEXT,

    PRIMARY KEY (user_id),
    FOREIGN KEY (institution_id) REFERENCES public.institution (institution_id) ON DELETE SET NULL ON UPDATE CASCADE,

    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    deleted_at TIMESTAMP
);

CREATE TABLE keys (
    key_id UUID DEFAULT uuid_generate_v4(),
    user_id UUID NOT NULL,
    key TEXT NOT NULL UNIQUE,
    last_used TIMESTAMP,
    PRIMARY KEY (key_id),
    FOREIGN KEY (user_id) REFERENCES public.users (user_id) ON DELETE CASCADE ON UPDATE CASCADE,

    created_at TIMESTAMP DEFAULT now(),
    deleted_at TIMESTAMP
);
CREATE TABLE public.roles (
    role_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP
);
CREATE TABLE public.permissions (
    permission_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255),
    display_name VARCHAR(255) NOT NULL UNIQUE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP
);
CREATE TABLE public.user_roles (
    user_id UUID NOT NULL,
    role_id UUID NOT NULL,
    PRIMARY KEY (user_id, role_id),
    FOREIGN KEY (user_id) REFERENCES public.users (user_id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (role_id) REFERENCES public.roles (role_id) ON DELETE CASCADE ON UPDATE CASCADE,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE public.role_permissions (
    role_id UUID NOT NULL,
    permission_id UUID NOT NULL,
    PRIMARY KEY (role_id, permission_id),
    FOREIGN KEY (role_id) REFERENCES public.roles (role_id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (permission_id) REFERENCES public.permissions (permission_id) ON DELETE CASCADE ON UPDATE CASCADE,
    created_at TIMESTAMP DEFAULT NOW()
);
CREATE TABLE public.researcher (
  researcher_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  name VARCHAR(150) NOT NULL,
  lattes_id VARCHAR(20),
  institution_id UUID NOT NULL,
  extra_field VARCHAR(255),

  status BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW(),
  deleted_at TIMESTAMP,
  
  UNIQUE (lattes_id, institution_id),
  FOREIGN KEY (institution_id) REFERENCES public.institution(institution_id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE public.graduate_program (
  graduate_program_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  code VARCHAR(100),
  name VARCHAR(100) NOT NULL,
  name_en VARCHAR(100),
  basic_area VARCHAR(100),
  cooperation_project VARCHAR(100),
  area VARCHAR(100) NOT NULL,
  modality VARCHAR(100) NOT NULL,
  program_type VARCHAR(100),
  rating VARCHAR(5),
  institution_id UUID NOT NULL,
  state VARCHAR(4) DEFAULT 'BA',
  city VARCHAR(100) DEFAULT 'Salvador',
  region VARCHAR(100) DEFAULT 'Nordeste',
  url_image VARCHAR(200),
  acronym VARCHAR(100),
  description TEXT,
  is_visible BOOLEAN DEFAULT FALSE,
  site TEXT,
  coordinator VARCHAR(100),
  email VARCHAR(100),
  start DATE,
  phone VARCHAR(20),
  periodicity VARCHAR(50),
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW(),
  deleted_at TIMESTAMP,
  FOREIGN KEY (institution_id) REFERENCES public.institution(institution_id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE public.graduate_program_researcher (
  graduate_program_id UUID NOT NULL,
  researcher_id UUID NOT NULL,
  year INT[],
  type_ RELATIONSHIP,
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW(),
  deleted_at TIMESTAMP,
  PRIMARY KEY (graduate_program_id, researcher_id),
  FOREIGN KEY (researcher_id) REFERENCES public.researcher(researcher_id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (graduate_program_id) REFERENCES public.graduate_program(graduate_program_id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE public.graduate_program_student (
  graduate_program_id UUID NOT NULL,
  researcher_id UUID NOT NULL,
  year INT[],
  created_at TIMESTAMP DEFAULT NOW(),
  updated_at TIMESTAMP DEFAULT NOW(),
  deleted_at TIMESTAMP,
  PRIMARY KEY (graduate_program_id, researcher_id, year),
  FOREIGN KEY (researcher_id) REFERENCES public.researcher(researcher_id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (graduate_program_id) REFERENCES public.graduate_program(graduate_program_id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE IF NOT EXISTS public.guidance_tracking
(
    id uuid NOT NULL DEFAULT uuid_generate_v4(),
    student_researcher_id uuid NOT NULL,
    supervisor_researcher_id uuid NOT NULL,
    co_supervisor_researcher_id uuid NOT NULL,
    graduate_program_id uuid NOT NULL,
    start_date date NOT NULL,
    planned_date_project date NOT NULL,
    done_date_project date,
    planned_date_qualification date NOT NULL,
    done_date_qualification date,
    planned_date_conclusion date NOT NULL,
    done_date_conclusion date,
    created_at timestamp without time zone NOT NULL DEFAULT now(),
    updated_at timestamp without time zone,
    deleted_at timestamp without time zone,
    CONSTRAINT guidance_tracking_pkey PRIMARY KEY (id),
    CONSTRAINT "FKco_supervisor_researcher" FOREIGN KEY (co_supervisor_researcher_id)
        REFERENCES public.researcher (researcher_id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    CONSTRAINT "FKgraduate_program" FOREIGN KEY (graduate_program_id)
        REFERENCES public.graduate_program (graduate_program_id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    CONSTRAINT "FKsudent_researcher" FOREIGN KEY (student_researcher_id)
        REFERENCES public.researcher (researcher_id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE,
    CONSTRAINT "FKsupervisor_researcher" FOREIGN KEY (supervisor_researcher_id)
        REFERENCES public.researcher (researcher_id) MATCH SIMPLE
        ON UPDATE CASCADE
        ON DELETE CASCADE
);
CREATE SCHEMA IF NOT EXISTS ufmg;
CREATE TABLE IF NOT EXISTS ufmg.researcher (
    researcher_id UUID PRIMARY KEY REFERENCES public.researcher(researcher_id) ON DELETE CASCADE ON UPDATE CASCADE,
    
    -- Campos comuns
    full_name VARCHAR(255),
    gender VARCHAR(255),
    status_code VARCHAR(255),
    work_regime VARCHAR(255),
    job_class CHAR,
    job_title VARCHAR(255),
    job_rank VARCHAR(255),
    job_reference_code VARCHAR(255),
    academic_degree VARCHAR(255),
    organization_entry_date DATE,
    last_promotion_date DATE,
    
    -- Novos campos
    employment_status_description VARCHAR(255),
    department_name VARCHAR(255),
    career_category VARCHAR(255),
    academic_unit VARCHAR(255),
    unit_code VARCHAR(255),
    function_code VARCHAR(255),
    position_code VARCHAR(255),
    leadership_start_date DATE,
    leadership_end_date DATE,
    current_function_name VARCHAR(255),
    function_location VARCHAR(255),
    
    -- Campos que estavam só na tabela antiga
    registration_number VARCHAR(200),            
    ufmg_registration_number VARCHAR(200),       
    semester_reference VARCHAR(6),

    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP     
);
CREATE TABLE IF NOT EXISTS ufmg.technician (
    technician_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    -- Campos comuns
    full_name VARCHAR(255),
    gender VARCHAR(255),
    status_code VARCHAR(255),
    work_regime VARCHAR(255),
    job_class CHAR,
    job_title VARCHAR(255),
    job_rank VARCHAR(255),
    job_reference_code VARCHAR(255),
    academic_degree VARCHAR(255),
    organization_entry_date DATE,
    last_promotion_date DATE,

    -- Novos campos
    employment_status_description VARCHAR(255),
    department_name VARCHAR(255),
    career_category VARCHAR(255),
    academic_unit VARCHAR(255),
    unit_code VARCHAR(255),
    function_code VARCHAR(255),
    position_code VARCHAR(255),
    leadership_start_date DATE,
    leadership_end_date DATE,
    current_function_name VARCHAR(255),
    function_location VARCHAR(255),

    -- Campos antigos
    registration_number VARCHAR(255),
    ufmg_registration_number VARCHAR(255),
    semester_reference VARCHAR(6),

    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP,
    deleted_at TIMESTAMP
);
CREATE SCHEMA IF NOT EXISTS feature;
CREATE TABLE IF NOT EXISTS feature.collection (
    collection_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    visible BOOLEAN DEFAULT TRUE,

    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    deleted_at TIMESTAMP,

    FOREIGN KEY (user_id) REFERENCES public.users(user_id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE IF NOT EXISTS feature.collection_entries(
    collection_id UUID REFERENCES feature.collection(collection_id),
    entry_id UUID NOT NULL,
    type VARCHAR(255) NOT NULL,
    FOREIGN KEY (collection_id) REFERENCES feature.collection(collection_id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE IF NOT EXISTS feature.stars(
    user_id UUID NOT NULL,
    entry_id UUID NOT NULL,
    type VARCHAR(255) NOT NULL,
    FOREIGN KEY (user_id) REFERENCES public.users(user_id) ON DELETE CASCADE ON UPDATE CASCADE
);
CREATE TABLE feature.notifications (
  notification_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id UUID NOT NULL,
  sender_id UUID,
  type NOTIFICATION_TYPE NOT NULL, 
  data JSONB,
  read BOOLEAN DEFAULT FALSE,
  created_at TIMESTAMP DEFAULT NOW(),
  read_at TIMESTAMP,

  FOREIGN KEY (user_id) REFERENCES public.users(user_id) ON DELETE CASCADE ON UPDATE CASCADE,
  FOREIGN KEY (sender_id) REFERENCES public.users(user_id) ON DELETE SET NULL ON UPDATE CASCADE
);

CREATE TABLE feature.chats (
    chat_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    chat_name VARCHAR,
    is_group BOOLEAN NOT NULL DEFAULT FALSE,

    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    deleted_at TIMESTAMP
);

CREATE TABLE feature.chat_participants (
    chat_id UUID NOT NULL,
    user_id UUID NOT NULL,
    joined_at TIMESTAMP DEFAULT NOW(),
    is_admin BOOLEAN NOT NULL DEFAULT FALSE,

    PRIMARY KEY (chat_id, user_id),
    FOREIGN KEY (chat_id) REFERENCES feature.chats (chat_id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (user_id) REFERENCES public.users (user_id) ON DELETE CASCADE ON UPDATE CASCADE,

    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    deleted_at TIMESTAMP
);

CREATE TABLE feature.chat_messages (
    message_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    chat_id UUID NOT NULL,
    sender_id UUID NOT NULL,
    content TEXT NOT NULL,
    
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    deleted_at TIMESTAMP,

    FOREIGN KEY (chat_id) REFERENCES feature.chats (chat_id) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (sender_id) REFERENCES public.users (user_id) ON DELETE CASCADE ON UPDATE CASCADE
);

COMMIT;

ROLLBACK;


--- Seeds
BEGIN;

INSERT INTO public.permissions (name, display_name) 
VALUES 
  ('ADMIN', 'administrativo'),
  ('INSTITUTION', 'instituicoes'),
  ('INSTITUTION', 'painel_instituicao'),
  ('DEPARTMENT', 'departamentos'),
  ('PROGRAM', 'programas'),
  (NULL, 'grupos_pesquisa'),
  ('RESEARCHER', 'pesquisadores'),
  (NULL, 'barema'),
  (NULL, 'notificacoes'),
  (NULL, 'indicadores_instituicao'),
  (NULL, 'indicadores_pos_graduacao'),
  (NULL, 'indicadores_departamento'),
  (NULL, 'indicadores_grupos_pesquisa'),
  (NULL, 'configuracoes'),
  ('ADMIN', 'apache_hop'),
  ('ADMIN', 'cargos_funcoes'),
  ('WEIGHTS', 'pesos_avaliacao'),
  ('ADMIN', 'parâmetros'),
  ('ADMIN', 'secao_pessoal');

COMMIT;

ROLLBACK;