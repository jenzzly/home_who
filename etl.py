import os
import json
import logging
import time
from datetime import datetime, timezone
from typing import Optional

import requests
import psycopg2
from psycopg2.extras import execute_values
from pydantic import BaseModel, field_validator, ValidationError


class lifebirthrecord(BaseModel):
    indicator_code: str
    spatial_dim: str          # Country ISO3 code
    parent_loc: str           # Continent
    time_dim: int             # Year
    dim1: Optional[str]       # SEX dimension (MLE / FMLE / BTSX)
    numeric_value: Optional[float]
    low_value: Optional[float]
    high_value: Optional[float]
    date_modified: Optional[datetime]


def main():
    print("App started")

if __name__ == "__main__":
    main()