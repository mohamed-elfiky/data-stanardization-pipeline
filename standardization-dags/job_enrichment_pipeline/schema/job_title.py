from pydantic import BaseModel, Field
from typing import List, Optional


class JobTitleEnrichment(BaseModel):
    title: str
    seniority_level: str
    department: str
    function: str
