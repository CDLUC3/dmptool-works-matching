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
