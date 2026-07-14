#!/usr/bin/env python3
"""Flatten the current OpenAlex JSONL snapshot into PostgreSQL COPY-ready CSV.gz.

The current public snapshot layout is:

    openalex-snapshot/data/jsonl/{entity}/updated_date=YYYY-MM-DD/part_NNNN.gz

The script streams gzip JSON Lines files and writes append-free CSV.gz outputs,
one table per generated CSV.  Use --limit for a validation run before processing
large entities such as works or authors.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import logging
import os
import sys
import time
from collections import Counter
from contextlib import ExitStack
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DEFAULT_SNAPSHOT_DIR = "openalex-snapshot"
DEFAULT_CSV_DIR = "csv-files"
DEFAULT_PROGRESS_INTERVAL = 100_000

CURRENT_ENTITIES = [
    "authors", "awards", "concepts", "continents", "countries", "domains",
    "fields", "funders", "institution-types", "institutions", "keywords",
    "languages", "licenses", "publishers", "sdgs", "source-types",
    "sources", "subfields", "topics", "work-types", "works",
]


@dataclass(frozen=True)
class Column:
    name: str
    pg_type: str = "text"


def c(name: str, pg_type: str = "text") -> Column:
    return Column(name, pg_type)


SUMMARY_COLUMNS = [
    c("summary_2yr_mean_citedness", "double precision"),
    c("summary_h_index", "integer"),
    c("summary_i10_index", "integer"),
]

TOPIC_HIERARCHY_COLUMNS = [
    c("subfield_id"), c("subfield_display_name"),
    c("field_id"), c("field_display_name"),
    c("domain_id"), c("domain_display_name"),
]

LOCATION_COLUMNS = [
    c("work_id"), c("location_position", "integer"), c("location_id"),
    c("source_id"), c("source_display_name"), c("source_issn_l"),
    c("source_issn", "jsonb"), c("source_is_oa", "boolean"),
    c("source_is_in_doaj", "boolean"), c("source_is_core", "boolean"),
    c("source_host_organization"), c("source_host_organization_name"),
    c("source_host_organization_lineage", "jsonb"),
    c("source_host_organization_lineage_names", "jsonb"), c("source_type"),
    c("is_oa", "boolean"), c("is_published", "boolean"),
    c("is_accepted", "boolean"), c("landing_page_url"), c("pdf_url"),
    c("raw_source_name"), c("raw_type"), c("provenance"), c("license"),
    c("license_id"), c("version"),
]

TABLES: dict[str, list[Column]] = {
    "authors": [
        c("id"), c("display_name"), c("full_name"), c("orcid"),
        c("display_name_alternatives", "jsonb"), c("raw_author_names", "jsonb"),
        c("works_count", "integer"), c("cited_by_count", "integer"),
        *SUMMARY_COLUMNS, c("works_api_url"),
        c("created_date", "timestamp without time zone"),
        c("updated_date", "timestamp without time zone"),
    ],
    "authors_ids": [c("author_id"), c("openalex"), c("orcid"), c("scopus")],
    "authors_counts_by_year": [c("author_id"), c("year", "integer"), c("works_count", "integer"), c("cited_by_count", "integer"), c("oa_works_count", "integer")],
    "authors_affiliations": [c("author_id"), c("affiliation_position", "integer"), c("institution_id"), c("institution_ror"), c("institution_display_name"), c("institution_country_code"), c("institution_type"), c("institution_lineage", "jsonb"), c("years", "jsonb")],
    "authors_last_known_institutions": [c("author_id"), c("institution_position", "integer"), c("institution_id"), c("institution_ror"), c("institution_display_name"), c("institution_country_code"), c("institution_type"), c("institution_lineage", "jsonb")],
    "authors_topics": [c("author_id"), c("topic_position", "integer"), c("topic_id"), c("display_name"), c("count", "integer"), *TOPIC_HIERARCHY_COLUMNS],
    "authors_topic_share": [c("author_id"), c("topic_position", "integer"), c("topic_id"), c("display_name"), c("value", "double precision"), *TOPIC_HIERARCHY_COLUMNS],
    "authors_x_concepts": [c("author_id"), c("concept_position", "integer"), c("concept_id"), c("wikidata"), c("display_name"), c("level", "integer"), c("score", "double precision"), c("count", "integer")],
    "authors_sources": [c("author_id"), c("source_position", "integer"), c("source_id"), c("display_name"), c("issn_l"), c("issn", "jsonb"), c("is_oa", "boolean"), c("is_in_doaj", "boolean"), c("is_core", "boolean"), c("host_organization"), c("host_organization_name"), c("host_organization_lineage", "jsonb"), c("host_organization_lineage_names", "jsonb"), c("type")],

    "awards": [c("id"), c("display_name"), c("description"), c("doi"), c("funder_id"), c("funder_display_name"), c("funder_ror"), c("funder_doi"), c("funder_award_id"), c("funder_scheme"), c("funding_type"), c("amount", "double precision"), c("currency"), c("start_date", "date"), c("start_year", "integer"), c("end_date", "date"), c("end_year", "integer"), c("landing_page_url"), c("provenance"), c("funded_outputs_count", "integer"), c("primary_topic_id"), c("primary_topic_display_name"), c("works_api_url"), c("created_date", "timestamp without time zone"), c("updated_date", "timestamp without time zone")],
    "awards_funded_outputs": [c("award_id"), c("output_position", "integer"), c("work_id")],
    "awards_investigators": [c("award_id"), c("investigator_position", "integer"), c("investigator_role"), c("given_name"), c("family_name"), c("orcid"), c("role_start"), c("affiliation", "jsonb")],
    "awards_institutions": [c("award_id"), c("institution_position", "integer"), c("institution_id"), c("institution_display_name"), c("institution_ror"), c("country_code"), c("raw_institution", "jsonb")],
    "awards_topics": [c("award_id"), c("topic_position", "integer"), c("topic_id"), c("display_name"), c("score", "double precision"), *TOPIC_HIERARCHY_COLUMNS],

    "concepts": [c("id"), c("wikidata"), c("display_name"), c("level", "integer"), c("description"), c("works_count", "integer"), c("cited_by_count", "integer"), *SUMMARY_COLUMNS, c("image_url"), c("image_thumbnail_url"), c("international", "jsonb"), c("works_api_url"), c("created_date", "timestamp without time zone"), c("updated_date", "timestamp without time zone")],
    "concepts_ids": [c("concept_id"), c("openalex"), c("wikidata"), c("wikipedia"), c("umls_aui", "jsonb"), c("umls_cui", "jsonb"), c("mag")],
    "concepts_ancestors": [c("concept_id"), c("ancestor_position", "integer"), c("ancestor_id"), c("ancestor_display_name")],
    "concepts_related_concepts": [c("concept_id"), c("related_position", "integer"), c("related_concept_id"), c("related_display_name"), c("score", "double precision")],
    "concepts_counts_by_year": [c("concept_id"), c("year", "integer"), c("works_count", "integer"), c("cited_by_count", "integer"), c("oa_works_count", "integer")],

    "continents": [c("id"), c("display_name"), c("description"), c("display_name_alternatives", "jsonb"), c("wikidata_id"), c("wikidata_url"), c("wikipedia_url"), c("ids", "jsonb"), c("created_date", "timestamp without time zone"), c("updated_date", "timestamp without time zone")],
    "continents_countries": [c("continent_id"), c("country_position", "integer"), c("country_id"), c("country_display_name")],
    "countries": [c("id"), c("display_name"), c("full_name"), c("description"), c("country_code"), c("alpha_3"), c("numeric", "integer"), c("continent_id"), c("continent_display_name"), c("is_global_south", "boolean"), c("works_count", "integer"), c("cited_by_count", "integer"), c("works_api_url"), c("authors_api_url"), c("institutions_api_url"), c("display_name_alternatives", "jsonb"), c("ids", "jsonb"), c("wikidata_url"), c("wikipedia_url"), c("created_date", "timestamp without time zone"), c("updated_date", "timestamp without time zone")],

    "domains": [c("id"), c("display_name"), c("description"), c("works_count", "integer"), c("cited_by_count", "integer"), c("display_name_alternatives", "jsonb"), c("works_api_url"), c("created_date", "timestamp without time zone"), c("updated_date", "timestamp without time zone")],
    "domains_ids": [c("domain_id"), c("openalex"), c("wikidata"), c("wikipedia")],
    "domains_fields": [c("domain_id"), c("field_position", "integer"), c("field_id"), c("field_display_name")],
    "domains_siblings": [c("domain_id"), c("sibling_position", "integer"), c("sibling_id"), c("sibling_display_name")],
    "fields": [c("id"), c("display_name"), c("description"), c("domain_id"), c("domain_display_name"), c("works_count", "integer"), c("cited_by_count", "integer"), c("display_name_alternatives", "jsonb"), c("works_api_url"), c("created_date", "timestamp without time zone"), c("updated_date", "timestamp without time zone")],
    "fields_ids": [c("field_id"), c("openalex"), c("wikidata"), c("wikipedia")],
    "fields_subfields": [c("field_id"), c("subfield_position", "integer"), c("subfield_id"), c("subfield_display_name")],
    "fields_siblings": [c("field_id"), c("sibling_position", "integer"), c("sibling_id"), c("sibling_display_name")],

    "funders": [c("id"), c("display_name"), c("description"), c("country_code"), c("homepage_url"), c("image_url"), c("image_thumbnail_url"), c("alternate_titles", "jsonb"), c("works_count", "integer"), c("cited_by_count", "integer"), c("awards_count", "integer"), *SUMMARY_COLUMNS, c("created_date", "timestamp without time zone"), c("updated_date", "timestamp without time zone")],
    "funders_ids": [c("funder_id"), c("openalex"), c("ror"), c("wikidata"), c("doi"), c("crossref")],
    "funders_counts_by_year": [c("funder_id"), c("year", "integer"), c("works_count", "integer"), c("cited_by_count", "integer"), c("oa_works_count", "integer")],
    "funders_roles": [c("funder_id"), c("role_position", "integer"), c("role"), c("role_id"), c("works_count", "integer")],

    "institution_types": [c("id"), c("display_name"), c("works_count", "integer"), c("cited_by_count", "integer"), c("works_api_url"), c("created_date", "timestamp without time zone"), c("updated_date", "timestamp without time zone")],
    "institutions": [c("id"), c("ror"), c("display_name"), c("country_code"), c("type"), c("type_id"), c("status"), c("homepage_url"), c("image_url"), c("image_thumbnail_url"), c("display_name_acronyms", "jsonb"), c("display_name_alternatives", "jsonb"), c("lineage", "jsonb"), c("is_super_system", "boolean"), c("works_count", "integer"), c("cited_by_count", "integer"), *SUMMARY_COLUMNS, c("works_api_url"), c("created_date", "timestamp without time zone"), c("updated_date", "timestamp without time zone")],
    "institutions_ids": [c("institution_id"), c("openalex"), c("ror"), c("grid"), c("wikipedia"), c("wikidata"), c("mag")],
    "institutions_geo": [c("institution_id"), c("city"), c("geonames_city_id"), c("region"), c("country_code"), c("country"), c("latitude", "double precision"), c("longitude", "double precision")],
    "institutions_associated_institutions": [c("institution_id"), c("associated_position", "integer"), c("associated_institution_id"), c("associated_display_name"), c("relationship")],
    "institutions_counts_by_year": [c("institution_id"), c("year", "integer"), c("works_count", "integer"), c("cited_by_count", "integer"), c("oa_works_count", "integer")],
    "institutions_roles": [c("institution_id"), c("role_position", "integer"), c("role"), c("role_id"), c("works_count", "integer")],
    "institutions_repositories": [c("institution_id"), c("repository_position", "integer"), c("repository_id"), c("repository_display_name"), c("host_organization"), c("host_organization_lineage", "jsonb"), c("raw_repository", "jsonb")],
    "institutions_topics": [c("institution_id"), c("topic_position", "integer"), c("topic_id"), c("display_name"), c("score", "double precision"), c("count", "integer"), *TOPIC_HIERARCHY_COLUMNS],
    "institutions_topic_share": [c("institution_id"), c("topic_position", "integer"), c("topic_id"), c("display_name"), c("value", "double precision"), *TOPIC_HIERARCHY_COLUMNS],

    "keywords": [c("id"), c("display_name"), c("works_count", "integer"), c("cited_by_count", "integer"), c("works_api_url"), c("created_date", "timestamp without time zone"), c("updated_date", "timestamp without time zone")],
    "languages": [c("id"), c("display_name"), c("works_count", "integer"), c("cited_by_count", "integer"), c("works_api_url"), c("created_date", "timestamp without time zone"), c("updated_date", "timestamp without time zone")],
    "licenses": [c("id"), c("display_name"), c("description"), c("url"), c("works_count", "integer"), c("cited_by_count", "integer"), c("works_api_url"), c("created_date", "timestamp without time zone"), c("updated_date", "timestamp without time zone")],
    "publishers": [c("id"), c("display_name"), c("alternate_titles", "jsonb"), c("country_codes", "jsonb"), c("hierarchy_level", "integer"), c("parent_publisher"), c("lineage", "jsonb"), c("ror_id"), c("wikidata_id"), c("homepage_url"), c("image_url"), c("image_thumbnail_url"), c("works_count", "integer"), c("cited_by_count", "integer"), *SUMMARY_COLUMNS, c("sources_api_url"), c("created_date", "timestamp without time zone"), c("updated_date", "timestamp without time zone")],
    "publishers_ids": [c("publisher_id"), c("openalex"), c("ror"), c("wikidata")],
    "publishers_counts_by_year": [c("publisher_id"), c("year", "integer"), c("works_count", "integer"), c("cited_by_count", "integer"), c("oa_works_count", "integer")],
    "publishers_roles": [c("publisher_id"), c("role_position", "integer"), c("role"), c("role_id"), c("works_count", "integer")],
    "sdgs": [c("id"), c("display_name"), c("description"), c("image_url"), c("image_thumbnail_url"), c("works_count", "integer"), c("cited_by_count", "integer"), c("ids", "jsonb"), c("works_api_url"), c("created_date", "timestamp without time zone"), c("updated_date", "timestamp without time zone")],
    "source_types": [c("id"), c("display_name"), c("works_count", "integer"), c("cited_by_count", "integer"), c("works_api_url"), c("created_date", "timestamp without time zone"), c("updated_date", "timestamp without time zone")],

    "sources": [c("id"), c("issn_l"), c("issn", "jsonb"), c("display_name"), c("alternate_titles", "jsonb"), c("country_code"), c("type"), c("publisher"), c("host_organization"), c("host_organization_name"), c("host_organization_lineage", "jsonb"), c("works_count", "integer"), c("cited_by_count", "integer"), c("oa_works_count", "integer"), c("is_oa", "boolean"), c("is_in_doaj", "boolean"), c("is_core", "boolean"), c("is_ojs", "boolean"), c("is_in_scielo", "boolean"), c("is_high_oa_rate", "boolean"), c("is_high_oa_rate_since_year", "integer"), c("is_in_doaj_since_year", "integer"), c("first_publication_year", "integer"), c("last_publication_year", "integer"), c("oa_flip_year", "integer"), c("apc_usd", "integer"), *SUMMARY_COLUMNS, c("homepage_url"), c("works_api_url"), c("created_date", "timestamp without time zone"), c("updated_date", "timestamp without time zone")],
    "sources_ids": [c("source_id"), c("openalex"), c("issn_l"), c("issn", "jsonb"), c("mag"), c("wikidata"), c("fatcat")],
    "sources_counts_by_year": [c("source_id"), c("year", "integer"), c("works_count", "integer"), c("cited_by_count", "integer"), c("oa_works_count", "integer")],
    "sources_apc_prices": [c("source_id"), c("price_position", "integer"), c("price", "integer"), c("currency")],
    "sources_topics": [c("source_id"), c("topic_position", "integer"), c("topic_id"), c("display_name"), c("score", "double precision"), c("count", "integer"), *TOPIC_HIERARCHY_COLUMNS],
    "sources_topic_share": [c("source_id"), c("topic_position", "integer"), c("topic_id"), c("display_name"), c("value", "double precision"), *TOPIC_HIERARCHY_COLUMNS],

    "subfields": [c("id"), c("display_name"), c("description"), c("field_id"), c("field_display_name"), c("domain_id"), c("domain_display_name"), c("works_count", "integer"), c("cited_by_count", "integer"), c("display_name_alternatives", "jsonb"), c("works_api_url"), c("created_date", "timestamp without time zone"), c("updated_date", "timestamp without time zone")],
    "subfields_ids": [c("subfield_id"), c("openalex"), c("wikidata"), c("wikipedia")],
    "subfields_topics": [c("subfield_id"), c("topic_position", "integer"), c("topic_id"), c("topic_display_name")],
    "subfields_siblings": [c("subfield_id"), c("sibling_position", "integer"), c("sibling_id"), c("sibling_display_name")],
    "topics": [c("id"), c("display_name"), c("description"), c("keywords", "jsonb"), c("subfield_id"), c("subfield_display_name"), c("field_id"), c("field_display_name"), c("domain_id"), c("domain_display_name"), c("works_count", "integer"), c("cited_by_count", "integer"), c("wikipedia_id"), c("works_api_url"), c("created_date", "timestamp without time zone"), c("updated_date", "timestamp without time zone")],
    "topics_ids": [c("topic_id"), c("openalex"), c("wikipedia")],
    "topics_keywords": [c("topic_id"), c("keyword_position", "integer"), c("keyword")],
    "topics_siblings": [c("topic_id"), c("sibling_position", "integer"), c("sibling_id"), c("sibling_display_name")],
    "work_types": [c("id"), c("display_name"), c("description"), c("works_count", "integer"), c("cited_by_count", "integer"), c("works_api_url"), c("created_date", "timestamp without time zone"), c("updated_date", "timestamp without time zone")],

    "works": [c("id"), c("doi"), c("title"), c("display_name"), c("publication_year", "integer"), c("publication_date", "date"), c("language"), c("type"), c("authors_count", "integer"), c("cited_by_count", "integer"), c("referenced_works_count", "integer"), c("locations_count", "integer"), c("countries_distinct_count", "integer"), c("institutions_distinct_count", "integer"), c("fwci", "double precision"), c("is_retracted", "boolean"), c("is_paratext", "boolean"), c("is_xpac", "boolean"), c("has_fulltext", "boolean"), c("has_content_pdf", "boolean"), c("has_content_grobid_xml", "boolean"), c("primary_topic_id"), c("primary_topic_display_name"), c("primary_topic_score", "double precision"), c("primary_topic_subfield_id"), c("primary_topic_subfield_display_name"), c("primary_topic_field_id"), c("primary_topic_field_display_name"), c("primary_topic_domain_id"), c("primary_topic_domain_display_name"), c("abstract_inverted_index", "jsonb"), c("created_date", "timestamp without time zone"), c("updated_date", "timestamp without time zone")],
    "works_ids": [c("work_id"), c("openalex"), c("doi"), c("mag"), c("pmid"), c("pmcid")],
    "works_indexed_in": [c("work_id"), c("position", "integer"), c("indexed_in")],
    "works_corresponding_author_ids": [c("work_id"), c("position", "integer"), c("author_id")],
    "works_corresponding_institution_ids": [c("work_id"), c("position", "integer"), c("institution_id")],
    "works_primary_locations": LOCATION_COLUMNS,
    "works_locations": LOCATION_COLUMNS,
    "works_best_oa_locations": LOCATION_COLUMNS,
    "works_authorships": [c("work_id"), c("authorship_position", "integer"), c("author_position"), c("author_id"), c("author_display_name"), c("author_orcid"), c("raw_author_name"), c("raw_orcid"), c("is_corresponding", "boolean"), c("countries", "jsonb")],
    "works_authorship_institutions": [c("work_id"), c("authorship_position", "integer"), c("institution_position", "integer"), c("author_id"), c("institution_id"), c("institution_ror"), c("institution_display_name"), c("institution_country_code"), c("institution_type"), c("institution_lineage", "jsonb")],
    "works_authorship_affiliations": [c("work_id"), c("authorship_position", "integer"), c("affiliation_position", "integer"), c("raw_affiliation_string"), c("institution_ids", "jsonb"), c("raw_affiliation", "jsonb")],
    "works_authorship_raw_affiliation_strings": [c("work_id"), c("authorship_position", "integer"), c("raw_affiliation_position", "integer"), c("raw_affiliation_string")],
    "works_authorship_countries": [c("work_id"), c("authorship_position", "integer"), c("country_position", "integer"), c("country_code")],
    "works_biblio": [c("work_id"), c("volume"), c("issue"), c("first_page"), c("last_page")],
    "works_topics": [c("work_id"), c("topic_position", "integer"), c("topic_id"), c("display_name"), c("score", "double precision"), *TOPIC_HIERARCHY_COLUMNS],
    "works_keywords": [c("work_id"), c("keyword_position", "integer"), c("keyword_id"), c("display_name"), c("score", "double precision")],
    "works_concepts": [c("work_id"), c("concept_position", "integer"), c("concept_id"), c("wikidata"), c("display_name"), c("level", "integer"), c("score", "double precision")],
    "works_sustainable_development_goals": [c("work_id"), c("sdg_position", "integer"), c("sdg_id"), c("display_name"), c("score", "double precision")],
    "works_awards": [c("work_id"), c("award_position", "integer"), c("award_id"), c("display_name"), c("funder_award_id"), c("funder_id"), c("funder_display_name")],
    "works_funders": [c("work_id"), c("funder_position", "integer"), c("funder_id"), c("display_name"), c("ror")],
    "works_institutions": [c("work_id"), c("institution_position", "integer"), c("institution_id"), c("institution_ror"), c("institution_display_name"), c("institution_country_code"), c("institution_type"), c("institution_lineage", "jsonb")],
    "works_open_access": [c("work_id"), c("is_oa", "boolean"), c("oa_status"), c("oa_url"), c("any_repository_has_fulltext", "boolean")],
    "works_counts_by_year": [c("work_id"), c("year", "integer"), c("cited_by_count", "integer")],
    "works_citation_normalized_percentile": [c("work_id"), c("value", "double precision"), c("is_in_top_1_percent", "boolean"), c("is_in_top_10_percent", "boolean")],
    "works_cited_by_percentile_year": [c("work_id"), c("min", "integer"), c("max", "integer")],
    "works_apc_list": [c("work_id"), c("value", "double precision"), c("currency"), c("value_usd", "double precision"), c("provenance")],
    "works_apc_paid": [c("work_id"), c("value", "double precision"), c("currency"), c("value_usd", "double precision"), c("provenance")],
    "works_mesh": [c("work_id"), c("mesh_position", "integer"), c("descriptor_ui"), c("descriptor_name"), c("qualifier_ui"), c("qualifier_name"), c("is_major_topic", "boolean")],
    "works_referenced_works": [c("work_id"), c("referenced_position", "integer"), c("referenced_work_id")],
    "works_related_works": [c("work_id"), c("related_position", "integer"), c("related_work_id")],
}

ENTITY_TABLES = {
    "authors": ["authors", "authors_ids", "authors_counts_by_year", "authors_affiliations", "authors_last_known_institutions", "authors_topics", "authors_topic_share", "authors_x_concepts", "authors_sources"],
    "awards": ["awards", "awards_funded_outputs", "awards_investigators", "awards_institutions", "awards_topics"],
    "concepts": ["concepts", "concepts_ids", "concepts_ancestors", "concepts_related_concepts", "concepts_counts_by_year"],
    "continents": ["continents", "continents_countries"],
    "countries": ["countries"],
    "domains": ["domains", "domains_ids", "domains_fields", "domains_siblings"],
    "fields": ["fields", "fields_ids", "fields_subfields", "fields_siblings"],
    "funders": ["funders", "funders_ids", "funders_counts_by_year", "funders_roles"],
    "institution-types": ["institution_types"],
    "institutions": ["institutions", "institutions_ids", "institutions_geo", "institutions_associated_institutions", "institutions_counts_by_year", "institutions_roles", "institutions_repositories", "institutions_topics", "institutions_topic_share"],
    "keywords": ["keywords"],
    "languages": ["languages"],
    "licenses": ["licenses"],
    "publishers": ["publishers", "publishers_ids", "publishers_counts_by_year", "publishers_roles"],
    "sdgs": ["sdgs"],
    "source-types": ["source_types"],
    "sources": ["sources", "sources_ids", "sources_counts_by_year", "sources_apc_prices", "sources_topics", "sources_topic_share"],
    "subfields": ["subfields", "subfields_ids", "subfields_topics", "subfields_siblings"],
    "topics": ["topics", "topics_ids", "topics_keywords", "topics_siblings"],
    "work-types": ["work_types"],
    "works": ["works", "works_ids", "works_indexed_in", "works_corresponding_author_ids", "works_corresponding_institution_ids", "works_primary_locations", "works_locations", "works_best_oa_locations", "works_authorships", "works_authorship_institutions", "works_authorship_affiliations", "works_authorship_raw_affiliation_strings", "works_authorship_countries", "works_biblio", "works_topics", "works_keywords", "works_concepts", "works_sustainable_development_goals", "works_awards", "works_funders", "works_institutions", "works_open_access", "works_counts_by_year", "works_citation_normalized_percentile", "works_cited_by_percentile_year", "works_apc_list", "works_apc_paid", "works_mesh", "works_referenced_works", "works_related_works"],
}


def as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def nested(value: Any, *keys: str) -> Any:
    current = value
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def clean_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    return value


def summary_fields(entity: dict[str, Any]) -> dict[str, Any]:
    stats = as_dict(entity.get("summary_stats"))
    return {
        "summary_2yr_mean_citedness": stats.get("2yr_mean_citedness"),
        "summary_h_index": stats.get("h_index"),
        "summary_i10_index": stats.get("i10_index"),
    }


def hierarchy_fields(item: dict[str, Any]) -> dict[str, Any]:
    return {
        "subfield_id": nested(item, "subfield", "id"),
        "subfield_display_name": nested(item, "subfield", "display_name"),
        "field_id": nested(item, "field", "id"),
        "field_display_name": nested(item, "field", "display_name"),
        "domain_id": nested(item, "domain", "id"),
        "domain_display_name": nested(item, "domain", "display_name"),
    }


def institution_fields(prefix: str, institution: dict[str, Any]) -> dict[str, Any]:
    return {
        f"{prefix}_id": institution.get("id"),
        f"{prefix}_ror": institution.get("ror"),
        f"{prefix}_display_name": institution.get("display_name"),
        f"{prefix}_country_code": institution.get("country_code"),
        f"{prefix}_type": institution.get("type"),
        f"{prefix}_lineage": institution.get("lineage"),
    }


def topic_row(parent_column: str, parent_id: str, position: int, topic: dict[str, Any]) -> dict[str, Any]:
    return {
        parent_column: parent_id,
        "topic_position": position,
        "topic_id": topic.get("id"),
        "display_name": topic.get("display_name"),
        "score": topic.get("score"),
        "count": topic.get("count"),
        "value": topic.get("value"),
        **hierarchy_fields(topic),
    }


class CsvSet:
    def __init__(self, output_dir: Path, table_names: list[str]):
        self.output_dir = output_dir
        self.table_names = table_names
        self.stack = ExitStack()
        self.writers: dict[str, csv.DictWriter] = {}
        self.row_counts: Counter[str] = Counter()

    def __enter__(self) -> "CsvSet":
        self.output_dir.mkdir(parents=True, exist_ok=True)
        for table_name in self.table_names:
            path = self.output_dir / f"{table_name}.csv.gz"
            handle = self.stack.enter_context(gzip.open(path, "wt", encoding="utf-8", newline=""))
            columns = [column.name for column in TABLES[table_name]]
            writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
            writer.writeheader()
            self.writers[table_name] = writer
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.stack.__exit__(*exc_info)

    def write(self, table_name: str, row: dict[str, Any]) -> None:
        columns = [column.name for column in TABLES[table_name]]
        self.writers[table_name].writerow({column: clean_value(row.get(column)) for column in columns})
        self.row_counts[table_name] += 1


class Flattener:
    def __init__(self, snapshot_dir: Path, output_dir: Path, limit: int | None, files_per_entity: int, progress_interval: int):
        self.snapshot_dir = snapshot_dir
        self.output_dir = output_dir
        self.limit = limit
        self.files_per_entity = files_per_entity
        self.progress_interval = progress_interval
        self.records_read: Counter[str] = Counter()
        self.records_skipped: Counter[str] = Counter()
        self.json_errors: Counter[str] = Counter()

    def run(self, entities: list[str]) -> None:
        for entity in entities:
            self.flatten_entity(entity)

    def entity_files(self, entity: str) -> list[Path]:
        current_root = self.snapshot_dir / "data" / "jsonl" / entity
        manifest = current_root / "manifest.json"
        if manifest.exists():
            payload = json.loads(manifest.read_text(encoding="utf-8"))
            files = []
            suffix = f"/jsonl/{entity}/"
            for entry in payload.get("files", []):
                url = entry.get("url", "")
                if suffix not in url:
                    continue
                path = current_root / url.split(suffix, 1)[1]
                if path.exists():
                    files.append(path)
            if files:
                return files
        files = sorted(current_root.glob("updated_date=*/part_*.gz"))
        if files:
            return files
        return sorted((self.snapshot_dir / "data" / entity).glob("*/*.gz"))

    def flatten_entity(self, entity: str) -> None:
        if entity not in ENTITY_TABLES:
            raise ValueError(f"unsupported entity: {entity}")
        files = self.entity_files(entity)
        if self.files_per_entity:
            files = files[: self.files_per_entity]
        logging.info("entity=%s files=%s", entity, len(files))
        started = time.time()
        flatten = getattr(self, f"flatten_{entity.replace('-', '_')}")
        with CsvSet(self.output_dir, ENTITY_TABLES[entity]) as csvs:
            stop = False
            for file_number, path in enumerate(files, 1):
                if stop:
                    break
                logging.info("entity=%s file=%s/%s path=%s", entity, file_number, len(files), path)
                with gzip.open(path, "rt", encoding="utf-8") as lines:
                    for line_number, line in enumerate(lines, 1):
                        if not line.strip():
                            continue
                        try:
                            record = json.loads(line)
                        except json.JSONDecodeError as exc:
                            self.json_errors[entity] += 1
                            logging.warning("bad JSON entity=%s file=%s line=%s error=%s", entity, path, line_number, exc)
                            continue
                        self.records_read[entity] += 1
                        if not record.get("id"):
                            self.records_skipped[entity] += 1
                            continue
                        flatten(record, csvs)
                        if self.progress_interval and self.records_read[entity] % self.progress_interval == 0:
                            logging.info("entity=%s records_read=%s elapsed=%.1fs", entity, self.records_read[entity], time.time() - started)
                        if self.limit and self.records_read[entity] >= self.limit:
                            stop = True
                            break
            logging.info("entity=%s records_read=%s skipped_missing_id=%s json_errors=%s rows=%s elapsed=%.1fs", entity, self.records_read[entity], self.records_skipped[entity], self.json_errors[entity], dict(sorted(csvs.row_counts.items())), time.time() - started)

    def flatten_authors(self, author: dict[str, Any], csvs: CsvSet) -> None:
        author_id = author["id"]
        csvs.write("authors", {"id": author_id, "display_name": author.get("display_name"), "full_name": author.get("full_name"), "orcid": author.get("orcid"), "display_name_alternatives": author.get("display_name_alternatives"), "raw_author_names": author.get("raw_author_names"), "works_count": author.get("works_count"), "cited_by_count": author.get("cited_by_count"), **summary_fields(author), "works_api_url": author.get("works_api_url"), "created_date": author.get("created_date"), "updated_date": author.get("updated_date")})
        ids = as_dict(author.get("ids"))
        csvs.write("authors_ids", {"author_id": author_id, "openalex": ids.get("openalex"), "orcid": ids.get("orcid"), "scopus": ids.get("scopus")})
        write_counts(csvs, "authors_counts_by_year", "author_id", author_id, author)
        for position, affiliation in enumerate(as_list(author.get("affiliations"))):
            institution = as_dict(as_dict(affiliation).get("institution"))
            csvs.write("authors_affiliations", {"author_id": author_id, "affiliation_position": position, **institution_fields("institution", institution), "years": as_dict(affiliation).get("years")})
        for position, institution in enumerate(as_list(author.get("last_known_institutions"))):
            csvs.write("authors_last_known_institutions", {"author_id": author_id, "institution_position": position, **institution_fields("institution", as_dict(institution))})
        for position, topic in enumerate(as_list(author.get("topics"))):
            csvs.write("authors_topics", topic_row("author_id", author_id, position, as_dict(topic)))
        for position, topic in enumerate(as_list(author.get("topic_share"))):
            csvs.write("authors_topic_share", topic_row("author_id", author_id, position, as_dict(topic)))
        for position, concept in enumerate(as_list(author.get("x_concepts"))):
            concept = as_dict(concept)
            csvs.write("authors_x_concepts", {"author_id": author_id, "concept_position": position, "concept_id": concept.get("id"), "wikidata": concept.get("wikidata"), "display_name": concept.get("display_name"), "level": concept.get("level"), "score": concept.get("score"), "count": concept.get("count")})
        for position, source in enumerate(as_list(author.get("sources"))):
            source = as_dict(source)
            csvs.write("authors_sources", {"author_id": author_id, "source_position": position, "source_id": source.get("id"), "display_name": source.get("display_name"), "issn_l": source.get("issn_l"), "issn": source.get("issn"), "is_oa": source.get("is_oa"), "is_in_doaj": source.get("is_in_doaj"), "is_core": source.get("is_core"), "host_organization": source.get("host_organization"), "host_organization_name": source.get("host_organization_name"), "host_organization_lineage": source.get("host_organization_lineage"), "host_organization_lineage_names": source.get("host_organization_lineage_names"), "type": source.get("type")})

    def flatten_awards(self, award: dict[str, Any], csvs: CsvSet) -> None:
        award_id = award["id"]
        funder = as_dict(award.get("funder"))
        primary_topic = as_dict(award.get("primary_topic") or award.get("primary_topic_full"))
        csvs.write("awards", {"id": award_id, "display_name": award.get("display_name"), "description": award.get("description"), "doi": award.get("doi"), "funder_id": funder.get("id"), "funder_display_name": funder.get("display_name"), "funder_ror": funder.get("ror_id") or funder.get("ror"), "funder_doi": funder.get("doi"), "funder_award_id": award.get("funder_award_id"), "funder_scheme": award.get("funder_scheme"), "funding_type": award.get("funding_type"), "amount": award.get("amount"), "currency": award.get("currency"), "start_date": award.get("start_date"), "start_year": award.get("start_year"), "end_date": award.get("end_date"), "end_year": award.get("end_year"), "landing_page_url": award.get("landing_page_url"), "provenance": award.get("provenance"), "funded_outputs_count": award.get("funded_outputs_count"), "primary_topic_id": primary_topic.get("id"), "primary_topic_display_name": primary_topic.get("display_name"), "works_api_url": award.get("works_api_url"), "created_date": award.get("created_date"), "updated_date": award.get("updated_date")})
        for position, work_id in enumerate(as_list(award.get("funded_outputs"))):
            csvs.write("awards_funded_outputs", {"award_id": award_id, "output_position": position, "work_id": work_id})
        for role_name in ("lead_investigator", "co_lead_investigator"):
            investigator = as_dict(award.get(role_name))
            if investigator:
                write_investigator(csvs, award_id, 0, role_name, investigator)
        for position, investigator in enumerate(as_list(award.get("investigators"))):
            write_investigator(csvs, award_id, position, "investigator", as_dict(investigator))
        for position, institution in enumerate(as_list(award.get("institution_awarded_full") or award.get("institution_awarded"))):
            institution = as_dict(institution)
            csvs.write("awards_institutions", {"award_id": award_id, "institution_position": position, "institution_id": institution.get("id"), "institution_display_name": institution.get("display_name") or institution.get("name"), "institution_ror": institution.get("ror"), "country_code": institution.get("country_code"), "raw_institution": institution})
        topics = award.get("topics_full") if award.get("topics_full") is not None else award.get("topics")
        for position, topic in enumerate(as_list(topics)):
            csvs.write("awards_topics", topic_row("award_id", award_id, position, as_dict(topic)))

    def flatten_concepts(self, concept: dict[str, Any], csvs: CsvSet) -> None:
        concept_id = concept["id"]
        csvs.write("concepts", {"id": concept_id, "wikidata": concept.get("wikidata"), "display_name": concept.get("display_name"), "level": concept.get("level"), "description": concept.get("description"), "works_count": concept.get("works_count"), "cited_by_count": concept.get("cited_by_count"), **summary_fields(concept), "image_url": concept.get("image_url"), "image_thumbnail_url": concept.get("image_thumbnail_url"), "international": concept.get("international"), "works_api_url": concept.get("works_api_url"), "created_date": concept.get("created_date"), "updated_date": concept.get("updated_date")})
        ids = as_dict(concept.get("ids"))
        csvs.write("concepts_ids", {"concept_id": concept_id, "openalex": ids.get("openalex"), "wikidata": ids.get("wikidata"), "wikipedia": ids.get("wikipedia"), "umls_aui": ids.get("umls_aui"), "umls_cui": ids.get("umls_cui"), "mag": ids.get("mag")})
        for position, ancestor in enumerate(as_list(concept.get("ancestors"))):
            ancestor = as_dict(ancestor)
            csvs.write("concepts_ancestors", {"concept_id": concept_id, "ancestor_position": position, "ancestor_id": ancestor.get("id"), "ancestor_display_name": ancestor.get("display_name")})
        for position, related in enumerate(as_list(concept.get("related_concepts"))):
            related = as_dict(related)
            csvs.write("concepts_related_concepts", {"concept_id": concept_id, "related_position": position, "related_concept_id": related.get("id"), "related_display_name": related.get("display_name"), "score": related.get("score")})
        write_counts(csvs, "concepts_counts_by_year", "concept_id", concept_id, concept)

    def flatten_continents(self, continent: dict[str, Any], csvs: CsvSet) -> None:
        continent_id = continent["id"]
        csvs.write("continents", {"id": continent_id, "display_name": continent.get("display_name"), "description": continent.get("description"), "display_name_alternatives": continent.get("display_name_alternatives"), "wikidata_id": continent.get("wikidata_id"), "wikidata_url": continent.get("wikidata_url"), "wikipedia_url": continent.get("wikipedia_url"), "ids": continent.get("ids"), "created_date": continent.get("created_date"), "updated_date": continent.get("updated_date")})
        for position, country in enumerate(as_list(continent.get("countries"))):
            country = as_dict(country)
            csvs.write("continents_countries", {"continent_id": continent_id, "country_position": position, "country_id": country.get("id"), "country_display_name": country.get("display_name")})

    def flatten_countries(self, country: dict[str, Any], csvs: CsvSet) -> None:
        continent = as_dict(country.get("continent"))
        csvs.write("countries", {"id": country["id"], "display_name": country.get("display_name"), "full_name": country.get("full_name"), "description": country.get("description"), "country_code": country.get("country_code"), "alpha_3": country.get("alpha_3"), "numeric": country.get("numeric"), "continent_id": continent.get("id") or country.get("continent_id"), "continent_display_name": continent.get("display_name"), "is_global_south": country.get("is_global_south"), "works_count": country.get("works_count"), "cited_by_count": country.get("cited_by_count"), "works_api_url": country.get("works_api_url"), "authors_api_url": country.get("authors_api_url"), "institutions_api_url": country.get("institutions_api_url"), "display_name_alternatives": country.get("display_name_alternatives"), "ids": country.get("ids"), "wikidata_url": country.get("wikidata_url"), "wikipedia_url": country.get("wikipedia_url"), "created_date": country.get("created_date"), "updated_date": country.get("updated_date")})

    def flatten_domains(self, domain: dict[str, Any], csvs: CsvSet) -> None:
        write_hierarchy_main(csvs, "domains", domain)
        write_ids(csvs, "domains_ids", "domain_id", domain["id"], domain.get("ids"))
        write_simple_links(csvs, "domains_fields", "domain_id", domain["id"], "field", domain.get("fields"))
        write_simple_links(csvs, "domains_siblings", "domain_id", domain["id"], "sibling", domain.get("siblings"))

    def flatten_fields(self, field: dict[str, Any], csvs: CsvSet) -> None:
        domain = as_dict(field.get("domain"))
        csvs.write("fields", {"id": field["id"], "display_name": field.get("display_name"), "description": field.get("description"), "domain_id": domain.get("id"), "domain_display_name": domain.get("display_name"), "works_count": field.get("works_count"), "cited_by_count": field.get("cited_by_count"), "display_name_alternatives": field.get("display_name_alternatives"), "works_api_url": field.get("works_api_url"), "created_date": field.get("created_date"), "updated_date": field.get("updated_date")})
        write_ids(csvs, "fields_ids", "field_id", field["id"], field.get("ids"))
        write_simple_links(csvs, "fields_subfields", "field_id", field["id"], "subfield", field.get("subfields"))
        write_simple_links(csvs, "fields_siblings", "field_id", field["id"], "sibling", field.get("siblings"))

    def flatten_funders(self, funder: dict[str, Any], csvs: CsvSet) -> None:
        funder_id = funder["id"]
        csvs.write("funders", {"id": funder_id, "display_name": funder.get("display_name"), "description": funder.get("description"), "country_code": funder.get("country_code"), "homepage_url": funder.get("homepage_url"), "image_url": funder.get("image_url"), "image_thumbnail_url": funder.get("image_thumbnail_url"), "alternate_titles": funder.get("alternate_titles"), "works_count": funder.get("works_count"), "cited_by_count": funder.get("cited_by_count"), "awards_count": funder.get("awards_count"), **summary_fields(funder), "created_date": funder.get("created_date"), "updated_date": funder.get("updated_date")})
        write_ids(csvs, "funders_ids", "funder_id", funder_id, funder.get("ids"))
        write_counts(csvs, "funders_counts_by_year", "funder_id", funder_id, funder)
        write_roles(csvs, "funders_roles", "funder_id", funder_id, funder.get("roles"))

    def flatten_institution_types(self, record: dict[str, Any], csvs: CsvSet) -> None:
        write_lookup(csvs, "institution_types", record)

    def flatten_institutions(self, institution: dict[str, Any], csvs: CsvSet) -> None:
        institution_id = institution["id"]
        csvs.write("institutions", {"id": institution_id, "ror": institution.get("ror"), "display_name": institution.get("display_name"), "country_code": institution.get("country_code"), "type": institution.get("type"), "type_id": institution.get("type_id"), "status": institution.get("status"), "homepage_url": institution.get("homepage_url"), "image_url": institution.get("image_url"), "image_thumbnail_url": institution.get("image_thumbnail_url"), "display_name_acronyms": institution.get("display_name_acronyms"), "display_name_alternatives": institution.get("display_name_alternatives"), "lineage": institution.get("lineage"), "is_super_system": institution.get("is_super_system"), "works_count": institution.get("works_count"), "cited_by_count": institution.get("cited_by_count"), **summary_fields(institution), "works_api_url": institution.get("works_api_url"), "created_date": institution.get("created_date"), "updated_date": institution.get("updated_date")})
        write_ids(csvs, "institutions_ids", "institution_id", institution_id, institution.get("ids"))
        geo = as_dict(institution.get("geo"))
        if geo:
            csvs.write("institutions_geo", {"institution_id": institution_id, "city": geo.get("city"), "geonames_city_id": geo.get("geonames_city_id"), "region": geo.get("region"), "country_code": geo.get("country_code"), "country": geo.get("country"), "latitude": geo.get("latitude"), "longitude": geo.get("longitude")})
        associated = institution.get("associated_institutions") or institution.get("associated_insitutions")
        for position, item in enumerate(as_list(associated)):
            item = as_dict(item)
            csvs.write("institutions_associated_institutions", {"institution_id": institution_id, "associated_position": position, "associated_institution_id": item.get("id"), "associated_display_name": item.get("display_name"), "relationship": item.get("relationship")})
        write_counts(csvs, "institutions_counts_by_year", "institution_id", institution_id, institution)
        write_roles(csvs, "institutions_roles", "institution_id", institution_id, institution.get("roles"))
        for position, repository in enumerate(as_list(institution.get("repositories"))):
            repository = as_dict(repository)
            csvs.write("institutions_repositories", {"institution_id": institution_id, "repository_position": position, "repository_id": repository.get("id"), "repository_display_name": repository.get("display_name"), "host_organization": repository.get("host_organization"), "host_organization_lineage": repository.get("host_organization_lineage"), "raw_repository": repository})
        for position, topic in enumerate(as_list(institution.get("topics"))):
            csvs.write("institutions_topics", topic_row("institution_id", institution_id, position, as_dict(topic)))
        for position, topic in enumerate(as_list(institution.get("topic_share"))):
            csvs.write("institutions_topic_share", topic_row("institution_id", institution_id, position, as_dict(topic)))

    def flatten_keywords(self, record: dict[str, Any], csvs: CsvSet) -> None:
        write_lookup(csvs, "keywords", record)

    def flatten_languages(self, record: dict[str, Any], csvs: CsvSet) -> None:
        write_lookup(csvs, "languages", record)

    def flatten_licenses(self, record: dict[str, Any], csvs: CsvSet) -> None:
        csvs.write("licenses", {"id": record["id"], "display_name": record.get("display_name"), "description": record.get("description"), "url": record.get("url"), "works_count": record.get("works_count"), "cited_by_count": record.get("cited_by_count"), "works_api_url": record.get("works_api_url"), "created_date": record.get("created_date"), "updated_date": record.get("updated_date")})

    def flatten_publishers(self, publisher: dict[str, Any], csvs: CsvSet) -> None:
        publisher_id = publisher["id"]
        csvs.write("publishers", {"id": publisher_id, "display_name": publisher.get("display_name"), "alternate_titles": publisher.get("alternate_titles"), "country_codes": publisher.get("country_codes"), "hierarchy_level": publisher.get("hierarchy_level"), "parent_publisher": publisher.get("parent_publisher"), "lineage": publisher.get("lineage"), "ror_id": publisher.get("ror_id"), "wikidata_id": publisher.get("wikidata_id"), "homepage_url": publisher.get("homepage_url"), "image_url": publisher.get("image_url"), "image_thumbnail_url": publisher.get("image_thumbnail_url"), "works_count": publisher.get("works_count"), "cited_by_count": publisher.get("cited_by_count"), **summary_fields(publisher), "sources_api_url": publisher.get("sources_api_url"), "created_date": publisher.get("created_date"), "updated_date": publisher.get("updated_date")})
        write_ids(csvs, "publishers_ids", "publisher_id", publisher_id, publisher.get("ids"))
        write_counts(csvs, "publishers_counts_by_year", "publisher_id", publisher_id, publisher)
        write_roles(csvs, "publishers_roles", "publisher_id", publisher_id, publisher.get("roles"))

    def flatten_sdgs(self, sdg: dict[str, Any], csvs: CsvSet) -> None:
        csvs.write("sdgs", {"id": sdg["id"], "display_name": sdg.get("display_name"), "description": sdg.get("description"), "image_url": sdg.get("image_url"), "image_thumbnail_url": sdg.get("image_thumbnail_url"), "works_count": sdg.get("works_count"), "cited_by_count": sdg.get("cited_by_count"), "ids": sdg.get("ids"), "works_api_url": sdg.get("works_api_url"), "created_date": sdg.get("created_date"), "updated_date": sdg.get("updated_date")})

    def flatten_source_types(self, record: dict[str, Any], csvs: CsvSet) -> None:
        write_lookup(csvs, "source_types", record)

    def flatten_sources(self, source: dict[str, Any], csvs: CsvSet) -> None:
        source_id = source["id"]
        csvs.write("sources", {"id": source_id, "issn_l": source.get("issn_l"), "issn": source.get("issn"), "display_name": source.get("display_name"), "alternate_titles": source.get("alternate_titles"), "country_code": source.get("country_code"), "type": source.get("type"), "publisher": source.get("publisher"), "host_organization": source.get("host_organization"), "host_organization_name": source.get("host_organization_name"), "host_organization_lineage": source.get("host_organization_lineage"), "works_count": source.get("works_count"), "cited_by_count": source.get("cited_by_count"), "oa_works_count": source.get("oa_works_count"), "is_oa": source.get("is_oa"), "is_in_doaj": source.get("is_in_doaj"), "is_core": source.get("is_core"), "is_ojs": source.get("is_ojs"), "is_in_scielo": source.get("is_in_scielo"), "is_high_oa_rate": source.get("is_high_oa_rate"), "is_high_oa_rate_since_year": source.get("is_high_oa_rate_since_year"), "is_in_doaj_since_year": source.get("is_in_doaj_since_year"), "first_publication_year": source.get("first_publication_year"), "last_publication_year": source.get("last_publication_year"), "oa_flip_year": source.get("oa_flip_year"), "apc_usd": source.get("apc_usd"), **summary_fields(source), "homepage_url": source.get("homepage_url"), "works_api_url": source.get("works_api_url"), "created_date": source.get("created_date"), "updated_date": source.get("updated_date")})
        write_ids(csvs, "sources_ids", "source_id", source_id, source.get("ids"))
        write_counts(csvs, "sources_counts_by_year", "source_id", source_id, source)
        for position, price in enumerate(as_list(source.get("apc_prices"))):
            price = as_dict(price)
            csvs.write("sources_apc_prices", {"source_id": source_id, "price_position": position, "price": price.get("price"), "currency": price.get("currency")})
        for position, topic in enumerate(as_list(source.get("topics"))):
            csvs.write("sources_topics", topic_row("source_id", source_id, position, as_dict(topic)))
        for position, topic in enumerate(as_list(source.get("topic_share"))):
            csvs.write("sources_topic_share", topic_row("source_id", source_id, position, as_dict(topic)))

    def flatten_subfields(self, subfield: dict[str, Any], csvs: CsvSet) -> None:
        field = as_dict(subfield.get("field")); domain = as_dict(subfield.get("domain"))
        csvs.write("subfields", {"id": subfield["id"], "display_name": subfield.get("display_name"), "description": subfield.get("description"), "field_id": field.get("id"), "field_display_name": field.get("display_name"), "domain_id": domain.get("id"), "domain_display_name": domain.get("display_name"), "works_count": subfield.get("works_count"), "cited_by_count": subfield.get("cited_by_count"), "display_name_alternatives": subfield.get("display_name_alternatives"), "works_api_url": subfield.get("works_api_url"), "created_date": subfield.get("created_date"), "updated_date": subfield.get("updated_date")})
        write_ids(csvs, "subfields_ids", "subfield_id", subfield["id"], subfield.get("ids"))
        write_simple_links(csvs, "subfields_topics", "subfield_id", subfield["id"], "topic", subfield.get("topics"))
        write_simple_links(csvs, "subfields_siblings", "subfield_id", subfield["id"], "sibling", subfield.get("siblings"))

    def flatten_topics(self, topic: dict[str, Any], csvs: CsvSet) -> None:
        topic_id = topic["id"]; ids = as_dict(topic.get("ids"))
        csvs.write("topics", {"id": topic_id, "display_name": topic.get("display_name"), "description": topic.get("description"), "keywords": topic.get("keywords"), **hierarchy_fields(topic), "works_count": topic.get("works_count"), "cited_by_count": topic.get("cited_by_count"), "wikipedia_id": ids.get("wikipedia"), "works_api_url": topic.get("works_api_url"), "created_date": topic.get("created_date"), "updated_date": topic.get("updated_date")})
        csvs.write("topics_ids", {"topic_id": topic_id, "openalex": ids.get("openalex"), "wikipedia": ids.get("wikipedia")})
        for position, keyword in enumerate(as_list(topic.get("keywords"))):
            csvs.write("topics_keywords", {"topic_id": topic_id, "keyword_position": position, "keyword": keyword})
        write_simple_links(csvs, "topics_siblings", "topic_id", topic_id, "sibling", topic.get("siblings"))

    def flatten_work_types(self, record: dict[str, Any], csvs: CsvSet) -> None:
        csvs.write("work_types", {"id": record["id"], "display_name": record.get("display_name"), "description": record.get("description"), "works_count": record.get("works_count"), "cited_by_count": record.get("cited_by_count"), "works_api_url": record.get("works_api_url"), "created_date": record.get("created_date"), "updated_date": record.get("updated_date")})

    def flatten_works(self, work: dict[str, Any], csvs: CsvSet) -> None:
        work_id = work["id"]; primary_topic = as_dict(work.get("primary_topic")); primary_h = hierarchy_fields(primary_topic)
        csvs.write("works", {"id": work_id, "doi": work.get("doi"), "title": work.get("title"), "display_name": work.get("display_name"), "publication_year": work.get("publication_year"), "publication_date": work.get("publication_date"), "language": work.get("language"), "type": work.get("type"), "authors_count": work.get("authors_count"), "cited_by_count": work.get("cited_by_count"), "referenced_works_count": work.get("referenced_works_count"), "locations_count": work.get("locations_count"), "countries_distinct_count": work.get("countries_distinct_count"), "institutions_distinct_count": work.get("institutions_distinct_count"), "fwci": work.get("fwci"), "is_retracted": work.get("is_retracted"), "is_paratext": work.get("is_paratext"), "is_xpac": work.get("is_xpac"), "has_fulltext": work.get("has_fulltext"), "has_content_pdf": nested(work, "has_content", "pdf"), "has_content_grobid_xml": nested(work, "has_content", "grobid_xml"), "primary_topic_id": primary_topic.get("id"), "primary_topic_display_name": primary_topic.get("display_name"), "primary_topic_score": primary_topic.get("score"), "primary_topic_subfield_id": primary_h.get("subfield_id"), "primary_topic_subfield_display_name": primary_h.get("subfield_display_name"), "primary_topic_field_id": primary_h.get("field_id"), "primary_topic_field_display_name": primary_h.get("field_display_name"), "primary_topic_domain_id": primary_h.get("domain_id"), "primary_topic_domain_display_name": primary_h.get("domain_display_name"), "abstract_inverted_index": work.get("abstract_inverted_index"), "created_date": work.get("created_date"), "updated_date": work.get("updated_date")})
        ids = as_dict(work.get("ids"))
        csvs.write("works_ids", {"work_id": work_id, "openalex": ids.get("openalex"), "doi": ids.get("doi"), "mag": ids.get("mag"), "pmid": ids.get("pmid"), "pmcid": ids.get("pmcid")})
        write_scalar_array(csvs, "works_indexed_in", "work_id", work_id, "indexed_in", work.get("indexed_in"))
        write_scalar_array(csvs, "works_corresponding_author_ids", "work_id", work_id, "author_id", work.get("corresponding_author_ids"))
        write_scalar_array(csvs, "works_corresponding_institution_ids", "work_id", work_id, "institution_id", work.get("corresponding_institution_ids"))
        write_location(csvs, "works_primary_locations", work_id, 0, as_dict(work.get("primary_location")))
        for position, location in enumerate(as_list(work.get("locations"))):
            write_location(csvs, "works_locations", work_id, position, as_dict(location))
        write_location(csvs, "works_best_oa_locations", work_id, 0, as_dict(work.get("best_oa_location")))
        for position, authorship in enumerate(as_list(work.get("authorships"))):
            write_authorship(csvs, work_id, position, as_dict(authorship))
        biblio = as_dict(work.get("biblio"))
        if biblio:
            csvs.write("works_biblio", {"work_id": work_id, "volume": biblio.get("volume"), "issue": biblio.get("issue"), "first_page": biblio.get("first_page"), "last_page": biblio.get("last_page")})
        for position, topic in enumerate(as_list(work.get("topics"))):
            csvs.write("works_topics", topic_row("work_id", work_id, position, as_dict(topic)))
        for position, keyword in enumerate(as_list(work.get("keywords"))):
            keyword = as_dict(keyword)
            csvs.write("works_keywords", {"work_id": work_id, "keyword_position": position, "keyword_id": keyword.get("id"), "display_name": keyword.get("display_name"), "score": keyword.get("score")})
        for position, concept in enumerate(as_list(work.get("concepts"))):
            concept = as_dict(concept)
            csvs.write("works_concepts", {"work_id": work_id, "concept_position": position, "concept_id": concept.get("id"), "wikidata": concept.get("wikidata"), "display_name": concept.get("display_name"), "level": concept.get("level"), "score": concept.get("score")})
        for position, sdg in enumerate(as_list(work.get("sustainable_development_goals"))):
            sdg = as_dict(sdg)
            csvs.write("works_sustainable_development_goals", {"work_id": work_id, "sdg_position": position, "sdg_id": sdg.get("id"), "display_name": sdg.get("display_name"), "score": sdg.get("score")})
        for position, award in enumerate(as_list(work.get("awards"))):
            award = as_dict(award)
            csvs.write("works_awards", {"work_id": work_id, "award_position": position, "award_id": award.get("id"), "display_name": award.get("display_name"), "funder_award_id": award.get("funder_award_id"), "funder_id": award.get("funder_id"), "funder_display_name": award.get("funder_display_name")})
        for position, funder in enumerate(as_list(work.get("funders"))):
            funder = as_dict(funder)
            csvs.write("works_funders", {"work_id": work_id, "funder_position": position, "funder_id": funder.get("id"), "display_name": funder.get("display_name"), "ror": funder.get("ror")})
        for position, institution in enumerate(as_list(work.get("institutions"))):
            csvs.write("works_institutions", {"work_id": work_id, "institution_position": position, **institution_fields("institution", as_dict(institution))})
        open_access = as_dict(work.get("open_access"))
        if open_access:
            csvs.write("works_open_access", {"work_id": work_id, "is_oa": open_access.get("is_oa"), "oa_status": open_access.get("oa_status"), "oa_url": open_access.get("oa_url"), "any_repository_has_fulltext": open_access.get("any_repository_has_fulltext")})
        write_counts(csvs, "works_counts_by_year", "work_id", work_id, work)
        citation = as_dict(work.get("citation_normalized_percentile"))
        if citation:
            csvs.write("works_citation_normalized_percentile", {"work_id": work_id, "value": citation.get("value"), "is_in_top_1_percent": citation.get("is_in_top_1_percent"), "is_in_top_10_percent": citation.get("is_in_top_10_percent")})
        percentile = as_dict(work.get("cited_by_percentile_year"))
        if percentile:
            csvs.write("works_cited_by_percentile_year", {"work_id": work_id, "min": percentile.get("min"), "max": percentile.get("max")})
        write_apc(csvs, "works_apc_list", work_id, work.get("apc_list"))
        write_apc(csvs, "works_apc_paid", work_id, work.get("apc_paid"))
        for position, mesh in enumerate(as_list(work.get("mesh"))):
            mesh = as_dict(mesh)
            csvs.write("works_mesh", {"work_id": work_id, "mesh_position": position, "descriptor_ui": mesh.get("descriptor_ui"), "descriptor_name": mesh.get("descriptor_name"), "qualifier_ui": mesh.get("qualifier_ui"), "qualifier_name": mesh.get("qualifier_name"), "is_major_topic": mesh.get("is_major_topic")})
        write_scalar_array(csvs, "works_referenced_works", "work_id", work_id, "referenced_work_id", work.get("referenced_works"), "referenced_position")
        write_scalar_array(csvs, "works_related_works", "work_id", work_id, "related_work_id", work.get("related_works"), "related_position")


def write_lookup(csvs: CsvSet, table_name: str, record: dict[str, Any]) -> None:
    csvs.write(table_name, {"id": record["id"], "display_name": record.get("display_name"), "works_count": record.get("works_count"), "cited_by_count": record.get("cited_by_count"), "works_api_url": record.get("works_api_url"), "created_date": record.get("created_date"), "updated_date": record.get("updated_date")})


def write_hierarchy_main(csvs: CsvSet, table_name: str, record: dict[str, Any]) -> None:
    csvs.write(table_name, {"id": record["id"], "display_name": record.get("display_name"), "description": record.get("description"), "works_count": record.get("works_count"), "cited_by_count": record.get("cited_by_count"), "display_name_alternatives": record.get("display_name_alternatives"), "works_api_url": record.get("works_api_url"), "created_date": record.get("created_date"), "updated_date": record.get("updated_date")})


def write_ids(csvs: CsvSet, table_name: str, parent_column: str, parent_id: str, ids_value: Any) -> None:
    ids = as_dict(ids_value)
    row = {parent_column: parent_id, "openalex": ids.get("openalex"), "orcid": ids.get("orcid"), "scopus": ids.get("scopus"), "ror": ids.get("ror"), "grid": ids.get("grid"), "wikipedia": ids.get("wikipedia"), "wikidata": ids.get("wikidata"), "doi": ids.get("doi"), "crossref": ids.get("crossref"), "issn_l": ids.get("issn_l"), "issn": ids.get("issn"), "mag": ids.get("mag"), "fatcat": ids.get("fatcat")}
    csvs.write(table_name, row)


def write_counts(csvs: CsvSet, table_name: str, parent_column: str, parent_id: str, entity: dict[str, Any]) -> None:
    for count in as_list(entity.get("counts_by_year")):
        count = as_dict(count)
        csvs.write(table_name, {parent_column: parent_id, "year": count.get("year"), "works_count": count.get("works_count"), "cited_by_count": count.get("cited_by_count"), "oa_works_count": count.get("oa_works_count")})


def write_roles(csvs: CsvSet, table_name: str, parent_column: str, parent_id: str, roles: Any) -> None:
    for position, role in enumerate(as_list(roles)):
        role = as_dict(role)
        csvs.write(table_name, {parent_column: parent_id, "role_position": position, "role": role.get("role"), "role_id": role.get("id"), "works_count": role.get("works_count")})


def write_simple_links(csvs: CsvSet, table_name: str, parent_column: str, parent_id: str, child_prefix: str, links: Any) -> None:
    for position, link in enumerate(as_list(links)):
        link = as_dict(link)
        csvs.write(table_name, {parent_column: parent_id, f"{child_prefix}_position": position, f"{child_prefix}_id": link.get("id"), f"{child_prefix}_display_name": link.get("display_name")})


def write_scalar_array(csvs: CsvSet, table_name: str, parent_column: str, parent_id: str, value_column: str, values: Any, position_name: str = "position") -> None:
    for position, value in enumerate(as_list(values)):
        csvs.write(table_name, {parent_column: parent_id, position_name: position, value_column: value})


def write_investigator(csvs: CsvSet, award_id: str, position: int, role: str, investigator: dict[str, Any]) -> None:
    csvs.write("awards_investigators", {"award_id": award_id, "investigator_position": position, "investigator_role": role, "given_name": investigator.get("given_name"), "family_name": investigator.get("family_name"), "orcid": investigator.get("orcid"), "role_start": investigator.get("role_start"), "affiliation": investigator.get("affiliation")})


def write_location(csvs: CsvSet, table_name: str, work_id: str, position: int, location: dict[str, Any]) -> None:
    if not location:
        return
    source = as_dict(location.get("source"))
    if not any(location.values()) and not source:
        return
    csvs.write(table_name, {"work_id": work_id, "location_position": position, "location_id": location.get("id"), "source_id": source.get("id"), "source_display_name": source.get("display_name"), "source_issn_l": source.get("issn_l"), "source_issn": source.get("issn"), "source_is_oa": source.get("is_oa"), "source_is_in_doaj": source.get("is_in_doaj"), "source_is_core": source.get("is_core"), "source_host_organization": source.get("host_organization"), "source_host_organization_name": source.get("host_organization_name"), "source_host_organization_lineage": source.get("host_organization_lineage"), "source_host_organization_lineage_names": source.get("host_organization_lineage_names"), "source_type": source.get("type"), "is_oa": location.get("is_oa"), "is_published": location.get("is_published"), "is_accepted": location.get("is_accepted"), "landing_page_url": location.get("landing_page_url"), "pdf_url": location.get("pdf_url"), "raw_source_name": location.get("raw_source_name"), "raw_type": location.get("raw_type"), "provenance": location.get("provenance"), "license": location.get("license"), "license_id": location.get("license_id"), "version": location.get("version")})


def write_authorship(csvs: CsvSet, work_id: str, position: int, authorship: dict[str, Any]) -> None:
    author = as_dict(authorship.get("author")); author_id = author.get("id")
    csvs.write("works_authorships", {"work_id": work_id, "authorship_position": position, "author_position": authorship.get("author_position"), "author_id": author_id, "author_display_name": author.get("display_name"), "author_orcid": author.get("orcid"), "raw_author_name": authorship.get("raw_author_name"), "raw_orcid": authorship.get("raw_orcid"), "is_corresponding": authorship.get("is_corresponding"), "countries": authorship.get("countries")})
    for institution_position, institution in enumerate(as_list(authorship.get("institutions"))):
        csvs.write("works_authorship_institutions", {"work_id": work_id, "authorship_position": position, "institution_position": institution_position, "author_id": author_id, **institution_fields("institution", as_dict(institution))})
    for affiliation_position, affiliation in enumerate(as_list(authorship.get("affiliations"))):
        affiliation = as_dict(affiliation)
        csvs.write("works_authorship_affiliations", {"work_id": work_id, "authorship_position": position, "affiliation_position": affiliation_position, "raw_affiliation_string": affiliation.get("raw_affiliation_string"), "institution_ids": affiliation.get("institution_ids"), "raw_affiliation": affiliation})
    for raw_position, raw_affiliation in enumerate(as_list(authorship.get("raw_affiliation_strings"))):
        csvs.write("works_authorship_raw_affiliation_strings", {"work_id": work_id, "authorship_position": position, "raw_affiliation_position": raw_position, "raw_affiliation_string": raw_affiliation})
    for country_position, country_code in enumerate(as_list(authorship.get("countries"))):
        csvs.write("works_authorship_countries", {"work_id": work_id, "authorship_position": position, "country_position": country_position, "country_code": country_code})


def write_apc(csvs: CsvSet, table_name: str, work_id: str, apc_value: Any) -> None:
    apc = as_dict(apc_value)
    if apc:
        csvs.write(table_name, {"work_id": work_id, "value": apc.get("value"), "currency": apc.get("currency"), "value_usd": apc.get("value_usd"), "provenance": apc.get("provenance")})


def schema_sql() -> str:
    lines = ["-- Generated by flatten-openalex-jsonl.py --print-schema.", "CREATE SCHEMA IF NOT EXISTS openalex;", ""]
    for table_name, columns in TABLES.items():
        lines.append(f"DROP TABLE IF EXISTS openalex.{table_name} CASCADE;")
        lines.append(f"CREATE TABLE openalex.{table_name} (")
        lines.append(",\n".join(f"    {column.name} {column.pg_type}" for column in columns))
        lines.append(");\n")
    return "\n".join(lines)


def copy_sql() -> str:
    lines = [
        "-- Generated by flatten-openalex-jsonl.py --print-copy-sql.",
        "-- Usage: psql -v csv_dir=/path/to/csv-files -f copy-openalex-csv.sql",
        "",
        "\\if :{?csv_dir}",
        "\\else",
        "\\echo 'Usage: psql -v csv_dir=/path/to/csv-files -f copy-openalex-csv.sql'",
        "\\quit 1",
        "\\endif",
        "\\setenv OPENALEX_CSV_DIR :csv_dir",
        "",
    ]
    for table_name, columns in TABLES.items():
        column_list = ", ".join(column.name for column in columns)
        lines.append(f"\\copy openalex.{table_name} ({column_list}) FROM PROGRAM 'gunzip -c \"${{OPENALEX_CSV_DIR}}/{table_name}.csv.gz\"' WITH (FORMAT csv, HEADER true)")
    lines.append("")
    return "\n".join(lines)


def index_sql() -> str:
    lines = ["-- Generated by flatten-openalex-jsonl.py --print-index-sql.", "-- Run after COPY for faster bulk loading.", ""]
    indexed = {"id", "work_id", "author_id", "institution_id", "source_id", "publisher_id", "funder_id", "concept_id", "topic_id", "award_id", "domain_id", "field_id", "subfield_id", "country_id", "referenced_work_id", "related_work_id"}
    for table_name, columns in TABLES.items():
        for column in columns:
            if column.name in indexed:
                lines.append(f"CREATE INDEX IF NOT EXISTS {table_name}_{column.name}_idx ON openalex.{table_name} ({column.name});")
    lines.append("")
    return "\n".join(lines)


def write_sql_files(sql_dir: Path) -> None:
    (sql_dir / "openalex-pg-schema.sql").write_text(schema_sql(), encoding="utf-8")
    (sql_dir / "copy-openalex-csv.sql").write_text(copy_sql(), encoding="utf-8")
    (sql_dir / "create-openalex-indexes.sql").write_text(index_sql(), encoding="utf-8")


def parse_entities(raw: str) -> list[str]:
    if raw == "all":
        return CURRENT_ENTITIES
    entities = [entity.strip() for entity in raw.split(",") if entity.strip()]
    bad = [entity for entity in entities if entity not in CURRENT_ENTITIES]
    if bad:
        raise SystemExit(f"Unknown entities: {', '.join(bad)}")
    return entities


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--snapshot-dir", default=DEFAULT_SNAPSHOT_DIR)
    parser.add_argument("--output-dir", default=DEFAULT_CSV_DIR)
    parser.add_argument("--entities", default="all", help="Comma-separated entity names or all.")
    parser.add_argument("--limit", type=int, help="Maximum records to read per entity.")
    parser.add_argument("--files-per-entity", type=int, default=int(os.environ.get("OPENALEX_DEMO_FILES_PER_ENTITY", "0")))
    parser.add_argument("--progress-interval", type=int, default=DEFAULT_PROGRESS_INTERVAL)
    parser.add_argument("--log-level", default="INFO")
    parser.add_argument("--print-schema", action="store_true")
    parser.add_argument("--print-copy-sql", action="store_true")
    parser.add_argument("--print-index-sql", action="store_true")
    parser.add_argument("--write-sql", action="store_true")
    parser.add_argument("--sql-dir", default=str(Path(__file__).resolve().parent))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.print_schema:
        print(schema_sql())
        return 0
    if args.print_copy_sql:
        print(copy_sql())
        return 0
    if args.print_index_sql:
        print(index_sql())
        return 0
    if args.write_sql:
        write_sql_files(Path(args.sql_dir))
        return 0
    logging.basicConfig(level=getattr(logging, args.log_level.upper()), format="%(asctime)s %(levelname)s %(message)s")
    flattener = Flattener(Path(args.snapshot_dir), Path(args.output_dir), args.limit, args.files_per_entity, args.progress_interval)
    flattener.run(parse_entities(args.entities))
    return 0


if __name__ == "__main__":
    sys.exit(main())
