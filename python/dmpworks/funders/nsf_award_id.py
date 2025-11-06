from __future__ import annotations

import logging
import re
from typing import Optional

from dmpworks.funders.award_id import AwardID
from dmpworks.funders.nsf_funder_api import nsf_fetch_org_id

log = logging.getLogger(__name__)


class NSFAwardID(AwardID):
    parent_ror_ids: set = {"021nxhr62"}

    def __init__(self, text: str, org_id: Optional[str] = None, award_id: Optional[str] = None):
        """Construct an NSF Award ID.

        :param org: NSF org ID, e.g. IOS, CBET etc.
        :param award_id: 7 digit award ID, .e.g 1509218.
        :param text: the original text it was parsed from.
        """

        super().__init__(text, ["text", "org_id", "award_id"])
        self.org_id = org_id
        self.award_id = award_id

    def generate_variants(self):
        variants = []

        # 1507101
        if self.award_id is not None:
            variants.append(str(self.award_id))

        if self.org_id is not None and self.award_id is not None:
            # DMR 1507101
            variants.append(f"{self.org_id} {self.award_id}")

            # DMR-1507101
            variants.append(f"{self.org_id}-{self.award_id}")

        return variants

    def fetch_additional_metadata(self):
        """Fetch the NSF Org ID for the award ID.

        :return: None.
        """

        if self.org_id is None and self.award_id is not None:
            self.org_id = nsf_fetch_org_id(self.award_id)

    def identifier_string(self) -> str:
        """The canonical identifier as a string"""

        if self.org_id and self.award_id:
            return f"{self.org_id}-{self.award_id}"

        return str(self.award_id)

    def award_url(self) -> Optional[str]:
        if self.award_id is not None:
            return f"https://www.nsf.gov/awardsearch/showAward?AWD_ID={self.award_id}&HistoricalAwards=false"
        return None

    @staticmethod
    def parse(text: Optional[str]) -> Optional[NSFAwardID]:
        return parse_nsf_award_id(text)


def parse_nsf_award_id(text: Optional[str]) -> Optional[NSFAwardID]:
    original_text = text

    # Return None if None or empty string
    if text is None or text.strip() == "":
        return None

    # Try to parse NSF award URL AWD_ID query parameter
    # https://www.nsf.gov/awardsearch/showAward?AWD_ID=2234213&HistoricalAwards=false
    match = re.search(r"AWD_ID=(?P<award_id>\d+)", text)
    if match:
        return NSFAwardID(original_text, award_id=match.group("award_id"))

    # Fixup common typos and errors
    text = text.replace("NSF-", "")
    text = text.replace("NSF", "")
    text = text.strip()

    # Try to parse org_id and award_id together
    match = re.search(r"(?P<org_id>[A-Z]{3,4})(?P<award_id>\d{7})", text)
    if match:
        return NSFAwardID(original_text, org_id=match.group("org_id"), award_id=match.group("award_id"))

    # Try to parse 7 digit award_id by itself
    match = re.search(r"\d{7}", text)
    if match:
        return NSFAwardID(original_text, award_id=match.group())

    return None
