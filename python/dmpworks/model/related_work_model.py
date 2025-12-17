from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel

from dmpworks.model.common import to_camel
from dmpworks.model.work_model import WorkModel


class DoiMatch(BaseModel):
    model_config = {
        "alias_generator": to_camel,
        "arbitrary_types_allowed": True,
        "populate_by_name": True,
    }

    found: bool
    score: float
    sources: List[DoiMatchSource]


class DoiMatchSource(BaseModel):
    model_config = {
        "alias_generator": to_camel,
        "arbitrary_types_allowed": True,
        "populate_by_name": True,
    }

    parent_award_id: Optional[str]
    award_id: str
    award_url: str


class ContentMatch(BaseModel):
    model_config = {
        "alias_generator": to_camel,
        "arbitrary_types_allowed": True,
        "populate_by_name": True,
    }

    score: float
    title_highlight: Optional[str]
    abstract_highlights: List[str]


class ItemMatch(BaseModel):
    model_config = {
        "alias_generator": to_camel,
        "arbitrary_types_allowed": True,
        "populate_by_name": True,
    }

    index: int
    score: float
    fields: Optional[List[str]] = None


class RelatedWork(BaseModel):
    model_config = {
        "alias_generator": to_camel,
        "arbitrary_types_allowed": True,
        "populate_by_name": True,
    }

    dmp_doi: str
    work: WorkModel
    score: float
    score_max: float
    doi_match: DoiMatch
    content_match: ContentMatch
    author_matches: List[ItemMatch] = []
    institution_matches: List[ItemMatch] = []
    funder_matches: List[ItemMatch] = []
    award_matches: List[ItemMatch] = []


class RelatedWorkJudgement(BaseModel):
    model_config = {
        "alias_generator": to_camel,
        "arbitrary_types_allowed": True,
        "populate_by_name": True,
    }

    # Metadata
    dmp_doi: str
    work_doi: str
    # Features
    mlt_content: float
    funded_doi_matched: float
    dmp_award_count: float
    award_match_count: float
    dmp_author_count: float
    author_orcid_match_count: float
    author_surname_match_count: float
    dmp_institution_count: float
    institution_ror_match_count: float
    institution_name_match_count: float
    dmp_funder_count: float
    funder_ror_match_count: float
    funder_name_match_count: float
    # Whether the match is valid or not
    label: Optional[int] = None
