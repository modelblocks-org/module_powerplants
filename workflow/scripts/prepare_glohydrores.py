"""Prepare a clean hydropower dataset that fits our schema."""

import sys
from typing import TYPE_CHECKING, Any, Literal, TypedDict

import _schemas

if TYPE_CHECKING:
    snakemake: Any
sys.stderr = open(snakemake.log[0], "w")
