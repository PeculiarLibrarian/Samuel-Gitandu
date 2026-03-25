"""
PADI Sovereign Bureau - Data Ingestion Module.

Provides live blockchain data ingestion from multiple networks
(OP Mainnet, Ethereum Mainnet) with confidence scoring and
verification according to Nairobi-01 node 1003 rules.
"""

__version__ = "1.0.0"
__author__ = "PADI Sovereign Bureau"
__node__ = "Nairobi-01"

from .config import PADIConfig, APIProvider
from .fetcher import EVMAPIDataFetcher, DataFetchError
from .normalizer import DataNormalizer, NormalizationError

__all__ = [
    "PADIConfig",
    "APIProvider",
    "EVMAPIDataFetcher",
    "DataFetchError",
    "DataNormalizer",
    "NormalizationError",
]
