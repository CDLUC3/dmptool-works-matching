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


class RelatedWorkTrainingRow(BaseModel):
    model_config = {
        "alias_generator": to_camel,
        "arbitrary_types_allowed": True,
        "populate_by_name": True,
    }

    # Metadata
    dmp_doi: str
    work_doi: str
    work_title: Optional[str]
    # Features
    mlt_content: float
    funded_doi_matched: int
    dmp_award_count: int
    award_match_count: int
    dmp_author_count: int
    author_orcid_match_count: int
    author_surname_match_count: int
    dmp_institution_count: int
    institution_ror_match_count: int
    institution_name_match_count: int
    dmp_funder_count: int
    funder_ror_match_count: int
    funder_name_match_count: int
    # Whether the match is valid or not
    judgement: Optional[int] = None

    def to_ranklib(self) -> str:
        judgement = [str(self.judgement), f"qid:{self.dmp_doi}"]
        features = [
            f"{i}:{feature}"
            for i, feature in enumerate(
                [
                    self.mlt_content,
                    self.funded_doi_matched,
                    self.dmp_award_count,
                    self.award_match_count,
                    self.dmp_author_count,
                    self.author_orcid_match_count,
                    self.author_surname_match_count,
                    self.dmp_institution_count,
                    self.institution_ror_match_count,
                    self.institution_name_match_count,
                    self.dmp_funder_count,
                    self.funder_ror_match_count,
                    self.funder_name_match_count,
                ]
            )
        ]
        comments = [f"# {self.work_doi}"]
        if self.work_title is not None:
            comments.append(f" {self.work_title}")
        combined = judgement + features + comments
        return " ".join(combined)
