"""
Data normalizer for PADI Bureau.

Normalizes blockchain data into a consistent RDF format according to
PADI ontology schema.
"""

import json
from datetime import datetime
from typing import Dict, Any, Optional
from enum import Enum
import logging

try:
    from rdflib import Graph, Literal, URIRef, Namespace
    from rdflib.namespace import RDF, RDFS, XSD
except ImportError:
    # Fallback if rdflib not installed yet
    Graph = None
    Literal = None
    URIRef = None
    Namespace = None

logger = logging.getLogger(__name__)


class NetworkType(Enum):
    """Supported network types."""
    OP_MAINNET = "op-mainnet"
    OP_SEPOLIA = "op-sepolia"
    ETH_MAINNET = "eth-mainnet"
    ETH_SEPOLIA = "eth-sepolia"


class NormalizationError(Exception):
    """
    Raised when data normalization fails.
    
    Attributes:
        message: Error description
        data: Original data that failed to normalize
        details: Additional error details
    """
    def __init__(self, message: str, data: Any = None, details: Dict = None):
        self.message = message
        self.data = data
        self.details = details or {}
        super().__init__(self.message)


class DataNormalizer:
    """
    Normalizes blockchain data into consistent RDF format.
    
    Converts raw blockchain data into standardized RDF triples
    following the PADI ontology schema at schema/ontology.ttl.
    """
    
    # PADI namespace for ontology
    PADI = Namespace("http://padi.sovereign.bureau/ontology#")
    
    # Blockchain-specific namespaces
    BLOCKCHAIN = Namespace("http://blockchain.info/schema#")
    
    def __init__(self, base_uri: str = "http://padi.sovereign.bureau/data/"):
        """
        Initialize data normalizer.
        
        Args:
            base_uri: Base URI for generated RDF resources
        """
        self.base_uri = base_uri
        
        if Graph is None:
            logger.warning("⚠️  rdflib not installed - RDF normalization disabled")
            self.rdf_enabled = False
        else:
            self.rdf_enabled = True
    
    def normalize_block(self, block_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize block data to standard format.
        
        Args:
            block_data: Raw block data from API
            
        Returns:
            Normalized block data dictionary
            
        Raises:
            NormalizationError: If data is invalid
        """
        try:
            # Validate required fields
            required_fields = ["number", "hash", "timestamp", "provider"]
            for field in required_fields:
                if field not in block_data:
                    raise NormalizationError(
                        f"Missing required field: {field}",
                        data=block_data
                    )
            
            # Create normalized block structure
            normalized = {
                "type": "Block",
                "block_number": block
