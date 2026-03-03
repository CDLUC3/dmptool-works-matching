from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel

from dmpworks.model.common import to_camel
from dmpworks.model.work_model import WorkModel


class DoiMatch(BaseModel):
    """Represents a match based on DOI.

    Attributes:
        found: Whether a match was found.
        score: The match score.
        sources: A list of sources for the match.
    """
    model_config = {
        "alias_generator": to_camel,
        "arbitrary_types_allowed": True,
        "populate_by_name": True,
    }

    found: bool
    score: float
    sources: List[DoiMatchSource]


class DoiMatchSource(BaseModel):
    """Represents a source for a DOI match.

    Attributes:
        parent_award_id: The parent award ID.
        award_id: The award ID.
        award_url: The URL of the award.
    """
    model_config = {
        "alias_generator": to_camel,
        "arbitrary_types_allowed": True,
        "populate_by_name": True,
    }

    parent_award_id: Optional[str]
    award_id: str
    award_url: str


class ContentMatch(BaseModel):
    """Represents a match based on content (title, abstract).

    Attributes:
        score: The match score.
        title_highlight: Highlighted title text.
        abstract_highlights: List of highlighted abstract text snippets.
    """
    model_config = {
        "alias_generator": to_camel,
        "arbitrary_types_allowed": True,
        "populate_by_name": True,
    }

    score: float
    title_highlight: Optional[str]
    abstract_highlights: List[str]


class ItemMatch(BaseModel):
    """Represents a match for a specific item (e.g., author, institution).

    Attributes:
        index: The index of the matched item.
        score: The match score.
        fields: List of fields that matched.
    """
    model_config = {
        "alias_generator": to_camel,
        "arbitrary_types_allowed": True,
        "populate_by_name": True,
    }

    index: int
    score: float
    fields: Optional[List[str]] = None


class RelatedWork(BaseModel):
    """Represents a related work with match details.

    Attributes:
        dmp_doi: The DOI of the DMP.
        work: The related work model.
        score: The total match score.
        score_max: The maximum possible score.
        doi_match: Details of the DOI match.
        content_match: Details of the content match.
        author_matches: List of author matches.
        institution_matches: List of institution matches.
        funder_matches: List of funder matches.
        award_matches: List of award matches.
        intra_work_doi_matches: List of intra-work DOI matches.
        possible_shared_project_doi_matches: List of possible shared project DOI matches.
        dataset_citation_doi_matches: List of dataset citation DOI matches.
    """
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
    intra_work_doi_matches: List[ItemMatch] = []
    possible_shared_project_doi_matches: List[ItemMatch] = []
    dataset_citation_doi_matches: List[ItemMatch] = []


class RelatedWorkTrainingRow(BaseModel):
    """Represents a row of training data for related work ranking.

    Attributes:
        dmp_doi: The DOI of the DMP.
        work_doi: The DOI of the work.
        work_title: The title of the work.
        mlt_content: More Like This content score.
        funded_doi_matched: Whether a funded DOI matched.
        dmp_award_count: Count of awards in the DMP.
        award_match_count: Count of matching awards.
        dmp_author_count: Count of authors in the DMP.
        author_orcid_match_count: Count of matching author ORCIDs.
        author_surname_match_count: Count of matching author surnames.
        dmp_institution_count: Count of institutions in the DMP.
        institution_ror_match_count: Count of matching institution RORs.
        institution_name_match_count: Count of matching institution names.
        dmp_funder_count: Count of funders in the DMP.
        funder_ror_match_count: Count of matching funder RORs.
        funder_name_match_count: Count of matching funder names.
        intra_work_doi_count: Count of intra-work DOIs.
        possible_shared_project_doi_count: Count of possible shared project DOIs.
        dataset_citation_doi_count: Count of dataset citation DOIs.
        judgement: The relevance judgement (label).
    """
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
    intra_work_doi_count: int
    possible_shared_project_doi_count: int
    dataset_citation_doi_count: int

    # Whether the match is valid or not
    judgement: Optional[int] = None

    def to_ranklib(self) -> str:
        """Convert the training row to RankLib format.

        Returns:
            str: A string formatted for RankLib.
        """
        judgement = [str(self.judgement), f"qid:{self.dmp_doi}"]
        features = [
            f"{i + 1}:{feature}"
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
                    self.intra_work_doi_count,
                    self.possible_shared_project_doi_count,
                    self.dataset_citation_doi_count,
                ]
            )
        ]
        comments = [f"# {self.work_doi}"]
        if self.work_title is not None:
            comments.append(f" {remove_newlines(self.work_title)}")
        combined = judgement + features + comments
        return " ".join(combined)


def remove_newlines(s: str, replacement: str = "") -> Optional[str]:
    """Remove newlines from a string.

    Args:
        s: The input string.
        replacement: The string to replace newlines with.

    Returns:
        str: The string with newlines removed.
    """
    if s is None:
        return None
    return s.replace("\r\n", "\n").replace("\r", "\n").replace("\n", replacement)
