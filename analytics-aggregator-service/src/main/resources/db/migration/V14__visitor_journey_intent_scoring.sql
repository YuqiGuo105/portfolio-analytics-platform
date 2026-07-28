-- Deterministic visitor journey and intent projections.
--
-- Runtime scoring deliberately has no model dependency. Product semantics
-- live in versioned database rules so every score is explainable and can be
-- rebuilt from public.behavior_events after rule or policy changes.

CREATE TABLE IF NOT EXISTS public.visitor_intent_policies (
    policy_id         TEXT        PRIMARY KEY,
    site_id           TEXT        NOT NULL,
    version           INT         NOT NULL,
    name              TEXT        NOT NULL,
    status            TEXT        NOT NULL DEFAULT 'DRAFT',
    medium_threshold  INT         NOT NULL,
    high_threshold    INT         NOT NULL,
    max_score         INT         NOT NULL DEFAULT 100,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    activated_at      TIMESTAMPTZ,
    CONSTRAINT visitor_intent_policy_status_check
        CHECK (status IN ('DRAFT', 'ACTIVE', 'RETIRED')),
    CONSTRAINT visitor_intent_policy_threshold_check
        CHECK (medium_threshold >= 0
           AND high_threshold > medium_threshold
           AND max_score >= high_threshold),
    UNIQUE (site_id, version)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_visitor_intent_policy_active
    ON public.visitor_intent_policies (site_id)
    WHERE status = 'ACTIVE';

CREATE TABLE IF NOT EXISTS public.visitor_intent_signal_rules (
    policy_id                TEXT        NOT NULL
        REFERENCES public.visitor_intent_policies (policy_id) ON DELETE CASCADE,
    rule_key                 TEXT        NOT NULL,
    description              TEXT        NOT NULL,
    event_name               TEXT,
    path_source              TEXT        NOT NULL DEFAULT 'ANY',
    path_match_type          TEXT        NOT NULL DEFAULT 'ANY',
    path_pattern             TEXT,
    content_type             TEXT,
    intent_dimension         TEXT        NOT NULL,
    base_weight              INT         NOT NULL,
    max_occurrences          INT         NOT NULL DEFAULT 1,
    min_engaged_seconds      INT,
    min_progress_percent     INT,
    active                   BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (policy_id, rule_key),
    CONSTRAINT visitor_intent_rule_source_check
        CHECK (path_source IN ('ANY', 'PAGE', 'TARGET')),
    CONSTRAINT visitor_intent_rule_match_check
        CHECK (path_match_type IN ('ANY', 'EXACT', 'PREFIX')),
    CONSTRAINT visitor_intent_rule_weight_check
        CHECK (base_weight >= 0 AND max_occurrences > 0),
    CONSTRAINT visitor_intent_rule_path_check
        CHECK (path_match_type = 'ANY' OR path_pattern IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_visitor_intent_rules_active
    ON public.visitor_intent_signal_rules (policy_id, active, event_name);

CREATE TABLE IF NOT EXISTS public.visitor_journey_funnel_rules (
    rule_id           TEXT        PRIMARY KEY,
    site_id           TEXT        NOT NULL,
    funnel_version    INT         NOT NULL,
    step_key          TEXT        NOT NULL,
    step_label        TEXT        NOT NULL,
    step_description  TEXT        NOT NULL,
    step_order        INT         NOT NULL,
    event_name        TEXT,
    path_source       TEXT        NOT NULL DEFAULT 'ANY',
    path_match_type   TEXT        NOT NULL DEFAULT 'ANY',
    path_pattern      TEXT,
    active            BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT visitor_funnel_source_check
        CHECK (path_source IN ('ANY', 'PAGE', 'TARGET')),
    CONSTRAINT visitor_funnel_match_check
        CHECK (path_match_type IN ('ANY', 'EXACT', 'PREFIX')),
    CONSTRAINT visitor_funnel_path_check
        CHECK (path_match_type = 'ANY' OR path_pattern IS NOT NULL)
);

CREATE INDEX IF NOT EXISTS idx_visitor_funnel_rules_active
    ON public.visitor_journey_funnel_rules (site_id, funnel_version, active, step_order);

CREATE TABLE IF NOT EXISTS public.visitor_attribution_rules (
    rule_id        TEXT        PRIMARY KEY,
    site_id        TEXT        NOT NULL,
    host_pattern   TEXT        NOT NULL,
    source_label   TEXT        NOT NULL,
    source_type    TEXT        NOT NULL,
    priority       INT         NOT NULL DEFAULT 100,
    active         BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_visitor_attribution_rules_active
    ON public.visitor_attribution_rules (site_id, active, priority);

CREATE TABLE IF NOT EXISTS public.content_intent_metadata (
    site_id               TEXT        NOT NULL,
    content_type          TEXT        NOT NULL,
    content_id            TEXT        NOT NULL,
    canonical_path        TEXT,
    display_title         TEXT,
    cover_url             TEXT,
    primary_intent        TEXT        NOT NULL,
    technical_domains     TEXT[]      NOT NULL DEFAULT ARRAY[]::TEXT[],
    complexity_level      TEXT,
    career_relevance      INT         NOT NULL DEFAULT 0,
    recommendation_group  TEXT,
    intent_weight         INT         NOT NULL DEFAULT 0,
    metadata_source       TEXT        NOT NULL DEFAULT 'ADMIN',
    metadata_version      INT         NOT NULL DEFAULT 1,
    active                BOOLEAN     NOT NULL DEFAULT TRUE,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (site_id, content_type, content_id),
    CONSTRAINT content_intent_complexity_check
        CHECK (complexity_level IS NULL OR complexity_level IN ('FOUNDATIONAL', 'INTERMEDIATE', 'ADVANCED')),
    CONSTRAINT content_intent_relevance_check
        CHECK (career_relevance BETWEEN 0 AND 100),
    CONSTRAINT content_intent_weight_check
        CHECK (intent_weight BETWEEN 0 AND 100),
    CONSTRAINT content_intent_source_check
        CHECK (metadata_source IN ('ADMIN', 'RULE_IMPORT', 'PUBLISH_EVENT', 'SOURCE_PROJECTION'))
);

CREATE INDEX IF NOT EXISTS idx_content_intent_metadata_path
    ON public.content_intent_metadata (site_id, canonical_path)
    WHERE active = TRUE;

CREATE TABLE IF NOT EXISTS public.content_intent_type_defaults (
    site_id               TEXT        NOT NULL,
    content_type          TEXT        NOT NULL,
    primary_intent        TEXT        NOT NULL,
    complexity_level      TEXT,
    career_relevance      INT         NOT NULL DEFAULT 0,
    recommendation_group  TEXT,
    intent_weight         INT         NOT NULL DEFAULT 0,
    active                BOOLEAN     NOT NULL DEFAULT TRUE,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (site_id, content_type),
    CONSTRAINT content_intent_default_complexity_check
        CHECK (complexity_level IS NULL OR complexity_level IN ('FOUNDATIONAL', 'INTERMEDIATE', 'ADVANCED')),
    CONSTRAINT content_intent_default_relevance_check
        CHECK (career_relevance BETWEEN 0 AND 100),
    CONSTRAINT content_intent_default_weight_check
        CHECK (intent_weight BETWEEN 0 AND 100)
);

CREATE TABLE IF NOT EXISTS public.visitor_journey_steps (
    event_id          TEXT        PRIMARY KEY,
    site_id           TEXT        NOT NULL,
    session_id        TEXT        NOT NULL,
    event_name        TEXT        NOT NULL,
    event_time        TIMESTAMPTZ NOT NULL,
    page_path         TEXT,
    target_path       TEXT,
    referrer_domain   TEXT,
    content_id        TEXT,
    content_type      TEXT,
    engaged_seconds   INT,
    progress_percent  INT,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_visitor_journey_session_time
    ON public.visitor_journey_steps (site_id, session_id, event_time, event_id);
CREATE INDEX IF NOT EXISTS idx_visitor_journey_window
    ON public.visitor_journey_steps (site_id, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_visitor_journey_content
    ON public.visitor_journey_steps (site_id, content_type, content_id)
    WHERE content_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS public.visitor_intent_snapshots (
    site_id               TEXT        NOT NULL,
    session_id            TEXT        NOT NULL,
    policy_id             TEXT        NOT NULL
        REFERENCES public.visitor_intent_policies (policy_id),
    policy_version        INT         NOT NULL,
    score                 INT         NOT NULL,
    intent_level          TEXT        NOT NULL,
    dominant_intent       TEXT,
    dimension_scores      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    contributing_signals  JSONB       NOT NULL DEFAULT '[]'::jsonb,
    first_event           TIMESTAMPTZ NOT NULL,
    last_event            TIMESTAMPTZ NOT NULL,
    calculated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (site_id, session_id, policy_id),
    CONSTRAINT visitor_intent_level_check
        CHECK (intent_level IN ('LOW', 'MEDIUM', 'HIGH')),
    CONSTRAINT visitor_intent_score_check
        CHECK (score >= 0)
);

CREATE INDEX IF NOT EXISTS idx_visitor_intent_window
    ON public.visitor_intent_snapshots
       (site_id, last_event DESC, intent_level, score DESC);

INSERT INTO public.visitor_intent_policies
    (policy_id, site_id, version, name, status,
     medium_threshold, high_threshold, max_score, activated_at)
VALUES
    ('yuqi-site-intent-v1', 'yuqi.site', 1, 'Portfolio visitor intent v1',
     'ACTIVE', 25, 55, 100, now())
ON CONFLICT (policy_id) DO NOTHING;

INSERT INTO public.visitor_intent_signal_rules
    (policy_id, rule_key, description, event_name, path_source,
     path_match_type, path_pattern, intent_dimension, base_weight,
     max_occurrences, min_engaged_seconds, min_progress_percent)
VALUES
    ('yuqi-site-intent-v1', 'general-page-view',
     'A visitor loaded a public page', 'page_view', 'PAGE',
     'ANY', NULL, 'exploration', 2, 4, NULL, NULL),
    ('yuqi-site-intent-v1', 'project-page-view',
     'A visitor opened a project detail page', 'page_view', 'PAGE',
     'PREFIX', '/work-single/', 'project_interest', 12, 3, NULL, NULL),
    ('yuqi-site-intent-v1', 'project-action',
     'A visitor explicitly opened a project', 'project_open', 'ANY',
     'ANY', NULL, 'project_interest', 14, 3, NULL, NULL),
    ('yuqi-site-intent-v1', 'content-read-half',
     'A visitor read at least half of an article or project', 'read_progress', 'ANY',
     'ANY', NULL, 'content_depth', 8, 2, NULL, 50),
    ('yuqi-site-intent-v1', 'engaged-thirty-seconds',
     'A visitor stayed engaged for at least thirty seconds', 'engaged_time', 'ANY',
     'ANY', NULL, 'content_depth', 7, 3, 30, NULL),
    ('yuqi-site-intent-v1', 'resume-view-cv',
     'A visitor opened the CV route', 'page_view', 'PAGE',
     'EXACT', '/cv', 'career_interest', 20, 1, NULL, NULL),
    ('yuqi-site-intent-v1', 'resume-view-resume',
     'A visitor opened the resume route', 'page_view', 'PAGE',
     'EXACT', '/resume', 'career_interest', 20, 1, NULL, NULL),
    ('yuqi-site-intent-v1', 'contact-view',
     'A visitor reached the contact route', 'page_view', 'PAGE',
     'EXACT', '/contact', 'contact_intent', 24, 1, NULL, NULL),
    ('yuqi-site-intent-v1', 'subscription-started',
     'A visitor started a subscription workflow', 'subscribe_started', 'ANY',
     'ANY', NULL, 'conversion_intent', 28, 1, NULL, NULL),
    ('yuqi-site-intent-v1', 'subscription-verified',
     'A visitor verified a subscription', 'subscribe_verified', 'ANY',
     'ANY', NULL, 'conversion_intent', 42, 1, NULL, NULL),
    ('yuqi-site-intent-v1', 'recommendation-click',
     'A visitor followed a recommendation', 'recommendation_click', 'ANY',
     'ANY', NULL, 'discovery_intent', 10, 3, NULL, NULL)
ON CONFLICT (policy_id, rule_key) DO NOTHING;

INSERT INTO public.visitor_journey_funnel_rules
    (rule_id, site_id, funnel_version, step_key, step_label,
     step_description, step_order, event_name, path_source,
     path_match_type, path_pattern)
VALUES
    ('funnel-v1-home', 'yuqi.site', 1, 'home', 'Homepage',
     'Loaded the main portfolio page', 1, 'page_view', 'PAGE', 'EXACT', '/'),
    ('funnel-v1-project-page', 'yuqi.site', 1, 'project', 'Project interest',
     'Opened or landed on a project page', 2, 'page_view', 'PAGE', 'PREFIX', '/work-single/'),
    ('funnel-v1-project-action', 'yuqi.site', 1, 'project', 'Project interest',
     'Opened or landed on a project page', 2, 'project_open', 'ANY', 'ANY', NULL),
    ('funnel-v1-resume-cv', 'yuqi.site', 1, 'resume', 'Resume intent',
     'Viewed resume or CV content', 3, 'page_view', 'PAGE', 'EXACT', '/cv'),
    ('funnel-v1-resume-route', 'yuqi.site', 1, 'resume', 'Resume intent',
     'Viewed resume or CV content', 3, 'page_view', 'PAGE', 'EXACT', '/resume'),
    ('funnel-v1-contact-page', 'yuqi.site', 1, 'contact', 'Contact or subscribe',
     'Reached a contact or subscription workflow', 4, 'page_view', 'PAGE', 'EXACT', '/contact'),
    ('funnel-v1-subscribe-start', 'yuqi.site', 1, 'contact', 'Contact or subscribe',
     'Reached a contact or subscription workflow', 4, 'subscribe_started', 'ANY', 'ANY', NULL),
    ('funnel-v1-subscribe-verified', 'yuqi.site', 1, 'contact', 'Contact or subscribe',
     'Reached a contact or subscription workflow', 4, 'subscribe_verified', 'ANY', 'ANY', NULL)
ON CONFLICT (rule_id) DO NOTHING;

INSERT INTO public.visitor_attribution_rules
    (rule_id, site_id, host_pattern, source_label, source_type, priority)
VALUES
    ('attribution-google', 'yuqi.site', 'google.', 'Google', 'search', 10),
    ('attribution-github', 'yuqi.site', 'github.', 'GitHub', 'developer', 20),
    ('attribution-linkedin', 'yuqi.site', 'linkedin.', 'LinkedIn', 'social', 30)
ON CONFLICT (rule_id) DO NOTHING;

-- These are data-owned defaults, not runtime branches. Admins can revise the
-- table without redeploying the consumer or replaying visitor events.
INSERT INTO public.content_intent_type_defaults
    (site_id, content_type, primary_intent, complexity_level,
     career_relevance, recommendation_group, intent_weight)
VALUES
    ('yuqi.site', 'PROJECT', 'project_interest', 'ADVANCED', 90, 'projects', 14),
    ('yuqi.site', 'BLOG', 'technical_learning', 'INTERMEDIATE', 65, 'articles', 8),
    ('yuqi.site', 'LIFE_BLOG', 'personal_context', 'FOUNDATIONAL', 20, 'life', 2),
    ('yuqi.site', 'EXPERIENCE', 'career_interest', 'ADVANCED', 100, 'experience', 18)
ON CONFLICT (site_id, content_type) DO NOTHING;

-- Content and analytics share the same PostgreSQL system of record. Project
-- content metadata in the source transaction instead of consuming a second
-- Kafka cluster. This adds no topics, no polling worker and no model cost.
CREATE OR REPLACE FUNCTION public.project_project_intent_metadata()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO public.content_intent_metadata
        (site_id, content_type, content_id, canonical_path, display_title,
         cover_url, primary_intent, technical_domains, complexity_level,
         career_relevance, recommendation_group, intent_weight,
         metadata_source, metadata_version, active, updated_at)
    SELECT
        d.site_id,
        d.content_type,
        NEW.id::text,
        '/work-single/' || NEW.id::text,
        NEW.title,
        NEW.image_url,
        d.primary_intent,
        array_remove(ARRAY[NEW.category, NEW.technology], NULL),
        d.complexity_level,
        d.career_relevance,
        d.recommendation_group,
        d.intent_weight,
        'SOURCE_PROJECTION',
        GREATEST(1, floor(extract(epoch FROM coalesce(NEW.updated_at, NEW.published_at, now())) / 60)::int),
        NEW.publication_status = 'PUBLISHED',
        now()
    FROM public.content_intent_type_defaults d
    WHERE d.site_id = 'yuqi.site'
      AND d.content_type = 'PROJECT'
      AND d.active = TRUE
    ON CONFLICT (site_id, content_type, content_id) DO UPDATE
       SET canonical_path = excluded.canonical_path,
           display_title = excluded.display_title,
           cover_url = excluded.cover_url,
           primary_intent = excluded.primary_intent,
           technical_domains = excluded.technical_domains,
           complexity_level = excluded.complexity_level,
           career_relevance = excluded.career_relevance,
           recommendation_group = excluded.recommendation_group,
           intent_weight = excluded.intent_weight,
           metadata_source = excluded.metadata_source,
           metadata_version = excluded.metadata_version,
           active = excluded.active,
           updated_at = now()
     WHERE content_intent_metadata.metadata_source <> 'ADMIN';
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION public.project_blog_intent_metadata()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO public.content_intent_metadata
        (site_id, content_type, content_id, canonical_path, display_title,
         cover_url, primary_intent, technical_domains, complexity_level,
         career_relevance, recommendation_group, intent_weight,
         metadata_source, metadata_version, active, updated_at)
    SELECT
        d.site_id,
        d.content_type,
        NEW.id::text,
        '/blog-single/' || NEW.id::text,
        NEW.title,
        NEW.image_url,
        d.primary_intent,
        array_remove(ARRAY[NEW.category, NEW.tags], NULL),
        d.complexity_level,
        d.career_relevance,
        d.recommendation_group,
        d.intent_weight,
        'SOURCE_PROJECTION',
        1,
        TRUE,
        now()
    FROM public.content_intent_type_defaults d
    WHERE d.site_id = 'yuqi.site'
      AND d.content_type = 'BLOG'
      AND d.active = TRUE
    ON CONFLICT (site_id, content_type, content_id) DO UPDATE
       SET canonical_path = excluded.canonical_path,
           display_title = excluded.display_title,
           cover_url = excluded.cover_url,
           primary_intent = excluded.primary_intent,
           technical_domains = excluded.technical_domains,
           complexity_level = excluded.complexity_level,
           career_relevance = excluded.career_relevance,
           recommendation_group = excluded.recommendation_group,
           intent_weight = excluded.intent_weight,
           metadata_source = excluded.metadata_source,
           active = TRUE,
           updated_at = now()
     WHERE content_intent_metadata.metadata_source <> 'ADMIN';
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION public.project_life_blog_intent_metadata()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO public.content_intent_metadata
        (site_id, content_type, content_id, canonical_path, display_title,
         cover_url, primary_intent, technical_domains, complexity_level,
         career_relevance, recommendation_group, intent_weight,
         metadata_source, metadata_version, active, updated_at)
    SELECT
        d.site_id,
        d.content_type,
        NEW.id::text,
        '/life-blog/' || NEW.id::text,
        NEW.title,
        NEW.image_url,
        d.primary_intent,
        array_remove(ARRAY[NEW.category, NEW.tags], NULL),
        d.complexity_level,
        d.career_relevance,
        d.recommendation_group,
        d.intent_weight,
        'SOURCE_PROJECTION',
        GREATEST(1, floor(extract(epoch FROM coalesce(NEW.updated_at, NEW.created_at, now())) / 60)::int),
        NEW.published_at IS NOT NULL,
        now()
    FROM public.content_intent_type_defaults d
    WHERE d.site_id = 'yuqi.site'
      AND d.content_type = 'LIFE_BLOG'
      AND d.active = TRUE
    ON CONFLICT (site_id, content_type, content_id) DO UPDATE
       SET canonical_path = excluded.canonical_path,
           display_title = excluded.display_title,
           cover_url = excluded.cover_url,
           primary_intent = excluded.primary_intent,
           technical_domains = excluded.technical_domains,
           complexity_level = excluded.complexity_level,
           career_relevance = excluded.career_relevance,
           recommendation_group = excluded.recommendation_group,
           intent_weight = excluded.intent_weight,
           metadata_source = excluded.metadata_source,
           metadata_version = excluded.metadata_version,
           active = excluded.active,
           updated_at = now()
     WHERE content_intent_metadata.metadata_source <> 'ADMIN';
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION public.project_experience_intent_metadata()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    INSERT INTO public.content_intent_metadata
        (site_id, content_type, content_id, display_title, primary_intent,
         technical_domains, complexity_level, career_relevance,
         recommendation_group, intent_weight, metadata_source,
         metadata_version, active, updated_at)
    SELECT
        d.site_id,
        d.content_type,
        NEW.id::text,
        NEW.name,
        d.primary_intent,
        array_remove(ARRAY[NEW.subname], NULL),
        d.complexity_level,
        d.career_relevance,
        d.recommendation_group,
        d.intent_weight,
        'SOURCE_PROJECTION',
        1,
        TRUE,
        now()
    FROM public.content_intent_type_defaults d
    WHERE d.site_id = 'yuqi.site'
      AND d.content_type = 'EXPERIENCE'
      AND d.active = TRUE
    ON CONFLICT (site_id, content_type, content_id) DO UPDATE
       SET display_title = excluded.display_title,
           primary_intent = excluded.primary_intent,
           technical_domains = excluded.technical_domains,
           complexity_level = excluded.complexity_level,
           career_relevance = excluded.career_relevance,
           recommendation_group = excluded.recommendation_group,
           intent_weight = excluded.intent_weight,
           metadata_source = excluded.metadata_source,
           active = TRUE,
           updated_at = now()
     WHERE content_intent_metadata.metadata_source <> 'ADMIN';
    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION public.deactivate_deleted_content_intent_metadata()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE public.content_intent_metadata
       SET active = FALSE, updated_at = now()
     WHERE site_id = 'yuqi.site'
       AND content_type = TG_ARGV[0]
       AND content_id = OLD.id::text
       AND metadata_source <> 'ADMIN';
    RETURN OLD;
END;
$$;

DROP TRIGGER IF EXISTS trg_project_intent_metadata ON public."Projects";
CREATE TRIGGER trg_project_intent_metadata
AFTER INSERT OR UPDATE ON public."Projects"
FOR EACH ROW EXECUTE FUNCTION public.project_project_intent_metadata();
DROP TRIGGER IF EXISTS trg_project_intent_metadata_delete ON public."Projects";
CREATE TRIGGER trg_project_intent_metadata_delete
AFTER DELETE ON public."Projects"
FOR EACH ROW EXECUTE FUNCTION public.deactivate_deleted_content_intent_metadata('PROJECT');

DROP TRIGGER IF EXISTS trg_blog_intent_metadata ON public."Blogs";
CREATE TRIGGER trg_blog_intent_metadata
AFTER INSERT OR UPDATE ON public."Blogs"
FOR EACH ROW EXECUTE FUNCTION public.project_blog_intent_metadata();
DROP TRIGGER IF EXISTS trg_blog_intent_metadata_delete ON public."Blogs";
CREATE TRIGGER trg_blog_intent_metadata_delete
AFTER DELETE ON public."Blogs"
FOR EACH ROW EXECUTE FUNCTION public.deactivate_deleted_content_intent_metadata('BLOG');

DROP TRIGGER IF EXISTS trg_life_blog_intent_metadata ON public.life_blogs;
CREATE TRIGGER trg_life_blog_intent_metadata
AFTER INSERT OR UPDATE ON public.life_blogs
FOR EACH ROW EXECUTE FUNCTION public.project_life_blog_intent_metadata();
DROP TRIGGER IF EXISTS trg_life_blog_intent_metadata_delete ON public.life_blogs;
CREATE TRIGGER trg_life_blog_intent_metadata_delete
AFTER DELETE ON public.life_blogs
FOR EACH ROW EXECUTE FUNCTION public.deactivate_deleted_content_intent_metadata('LIFE_BLOG');

DROP TRIGGER IF EXISTS trg_experience_intent_metadata ON public.experience;
CREATE TRIGGER trg_experience_intent_metadata
AFTER INSERT OR UPDATE ON public.experience
FOR EACH ROW EXECUTE FUNCTION public.project_experience_intent_metadata();
DROP TRIGGER IF EXISTS trg_experience_intent_metadata_delete ON public.experience;
CREATE TRIGGER trg_experience_intent_metadata_delete
AFTER DELETE ON public.experience
FOR EACH ROW EXECUTE FUNCTION public.deactivate_deleted_content_intent_metadata('EXPERIENCE');

-- Backfill current source rows through the same trigger functions so runtime
-- scoring is immediately useful after this migration.
UPDATE public."Projects" SET updated_at = updated_at;
UPDATE public."Blogs" SET title = title;
UPDATE public.life_blogs SET updated_at = updated_at;
UPDATE public.experience SET name = name;
