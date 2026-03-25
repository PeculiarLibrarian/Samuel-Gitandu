"""
EVM API data fetcher for PADI Bureau.

Implements confidence scoring, multi-provider fallback, and
verification according to Nairobi-01 node 1003 rules.
"""

import asyncio
import aiohttp
from typing import Dict, Any, Optional, List
from datetime import datetime
import logging
from .config import PADIConfig, APIProvider

logger = logging.getLogger(__name__)


class DataFetchError(Exception):
    """
    Raised when API data fetch fails.
    
    Attributes:
        message: Error description
        provider: Name of provider that failed
        details: Additional error details
    """
    def __init__(self, message: str, provider: str = "", details: Dict = None):
        self.message = message
        self.provider = provider
        self.details = details or {}
        super().__init__(self.message)


class EVMAPIDataFetcher:
    """
    Fetches EVM blockchain data from multiple API providers with
    confidence scoring and verification.
    
    Implements Nairobi-01 node 1003 rule enforcement:
    - Confidence threshold of 1.0 (100% certainty)
    - Requires 3 verification sources
    
    Features:
    - Multi-provider fallback chain
    - Confidence scoring
    - Cross-provider verification
    - Rate limiting awareness
    """
    
    def __init__(self, providers: List[APIProvider] = None):
        """
        Initialize EVM data fetcher.
        
        Args:
            providers: List of API providers (uses config default if None)
        """
        # Validate 1003 rules on initialization
        if not PADIConfig.validate_1003_rules():
            logger.warning("⚠️  Nairobi-01 Rule 1003: Configuration violates confidence requirements")
        
        # Use provided providers or get from config
        if providers is None:
            providers = PADIConfig.get_all_enabled_providers()
        
        if not providers:
            raise DataFetchError(
                "No valid API providers configured",
                details={"suggestion": "Check .env file configuration"}
            )
        
        self.providers = providers
        self.primary_provider = PADIConfig.get_primary_provider()
        self.fallback_providers = PADIConfig.get_fallback_providers()
        
        # Request tracking for rate limiting
        self.request_counts: Dict[str, int] = {
            p.name: 0 for p in self.providers
        }
        
        logger.info(
            f"Initialized EVMAPIDataFetcher with {len(providers)} providers: "
            f"{[p.name for p in providers]}"
        )
    
    async def _rpc_call(
        self,
        provider: APIProvider,
        method: str,
        params: List = None
    ) -> Dict[str, Any]:
        """
        Execute a single JSON-RPC call.
        
        Args:
            provider: API provider to query
            method: RPC method name
            params: RPC parameters
            
        Returns:
            RPC response result
            
        Raises:
            DataFetchError: If call fails or returns error
        """
        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": method,
            "params": params or []
        }
        
        # Check rate limit
        if provider.rate_limit_per_minute > 0:
            if self.request_counts[provider.name] >= provider.rate_limit_per_minute:
                raise DataFetchError(
                    f"Rate limit exceeded for {provider.name}",
                    provider=provider.name,
                    details={
                        "requests": self.request_counts[provider.name],
                        "limit": provider.rate_limit_per_minute
                    }
                )
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    provider.full_url,
                    json=payload,
                    timeout=aiohttp.ClientTimeout(total=30)
                ) as response:
                    data = await response.json()
                    
                    # Increment request counter
                    self.request_counts[provider.name] = self.request_counts.get(provider.name, 0) + 1
                    
                    # Check for RPC error
                    if 'error' in data:
                        raise DataFetchError(
                            f"RPC Error: {data['error']}",
                            provider=provider.name,
                            details={"error": data['error']}
                        )
                    
                    return data.get('result')
                    
        except aiohttp.TimeoutError:
            raise DataFetchError(
                "Request timeout",
                provider=provider.name
            )
        except aiohttp.ClientError as e:
            raise DataFetchError(
                f"Network error: {str(e)}",
                provider=provider.name
            )
    
    async def _get_block_height_from_provider(
        self,
        provider: APIProvider
    ) -> Dict[str, Any]:
        """
        Get current block height from a specific provider.
        
        Args:
            provider: API provider to query
            
        Returns:
            Dictionary containing block height and metadata
        """
        block_hex = await self._rpc_call(provider, "eth_blockNumber")
        block_dec = int(block_hex, 16)
        
        return {
            "block_height": block_dec,
            "provider": provider.name,
            "chain_id": provider.chain_id,
            "network": provider.network,
            "confidence": 1.0  # Direct API response
        }
    
    async def _verify_block_height(
        self,
        block_height: int,
        exclude_provider: str = None
    ) -> Dict[str, Any]:
        """
        Verify block height with additional providers for confidence scoring.
        
        Implements Nairobi-01 node 1003 rule: requires 3 verification sources.
        
        Args:
            block_height: Block height to verify
            exclude_provider: Provider name to exclude from verification
            
        Returns:
            Dictionary containing verification results and confidence score
        """
        verification_sources = []
        matching_heights = []
        
        # Get providers to verify against
        verification_providers = [
            p for p in self.providers 
            if p.name != exclude_provider
        ]
        
        if len(verification_providers) + 1 < PADIConfig.REQUIRED_VERIFICATION_SOURCES:
            logger.warning(
                f"⚠️  Insufficient verification sources: "
                f"{len(verification_providers) + 1} available, "
                f"{PADIConfig.REQUIRED_VERIFICATION_SOURCES} required by Rule 1003"
            )
        
        # Verify with additional providers
        for provider in verification_providers[:PADIConfig.REQUIRED_VERIFICATION_SOURCES - 1]:
            try:
                result = await self._get_block_height_from_provider(provider)
                verification_sources.append({
                    "provider": provider.name,
                    "block_height": result["block_height"],
                    "confidence": 1.0
                })
                
                # Check if height matches within drift threshold
                if abs(result["block_height"] - block_height) <= PADIConfig.MAX_DRIFT_SECONDS:
                    matching_heights.append(provider.name)
            except DataFetchError as e:
                logger.warning(f"Verification failed for {provider.name}: {e.message}")
                verification_sources.append({
                    "provider": provider.name,
                    "error": e.message,
                    "confidence": 0.0
                })
        
        # Calculate confidence score
        total_sources = len(verification_sources) + 1  # +1 for primary
        matching_sources = len(matching_heights) + 1  # +1 for primary
        
        confidence_score = matching_sources / total_sources if total_sources > 0 else 0.0
        
        return {
            "primary_height": block_height,
            "verification_sources": verification_sources,
            "matching_sources": matching_heights,
            "confident": confidence_score >= PADIConfig.CONFIDENCE_THRESHOLD,
            "confidence_score": confidence_score,
            "all_sources_verify": (
                matching_sources >= PADIConfig.REQUIRED_VERIFICATION_SOURCES
            )
        }
    
    async def get_current_block_height(self) -> Dict[str, Any]:
        """
        Get current block height with confidence verification.
        
        Implements multi-provider fallback and verification:
        1. Try primary provider first
        2. If fails, try fallback providers
        3. Verify result with additional providers for confidence
        4. Return confident result or raise error
        
        Returns:
            Dictionary containing block height and verification metadata
            
        Raises:
            DataFetchError: If unable to get confident result
        """
        result = None
        last_error = None
        
        # Try primary provider first
        if self.primary_provider and self.primary_provider.enabled:
            try:
                primary_result = await self._get_block_height_from_provider(
                    self.primary_provider
                )
                
                # Verify with other providers
                verification = await self._verify_block_height(
                    primary_result["block_height"],
                    exclude_provider=self.primary_provider.name
                )
                
                # Check if result meets confidence threshold
                if verification["confident"] and verification["all_sources_verify"]:
                    result = {
                        **primary_result,
                        "verification": verification,
                        "verified": True
                    }
                    logger.info(
                        f"✅ Verified block height: {primary_result['block_height']:,} "
                        f"from {self.primary_provider.name}"
                    )
                else:
                    logger.warning(
                        f"⚠️  Primary result below confidence threshold: "
                        f"{verification['confidence_score']:.2f} "
                        f"(required {PADIConfig.CONFIDENCE_THRESHOLD})"
                    )
                    last_error = DataFetchError(
                        "Primary provider result below confidence threshold",
                        provider=self.primary_provider.name,
                        details={
                            "confidence_score": verification["confidence_score"],
                            "required_confidence": PADIConfig.CONFIDENCE_THRESHOLD,
                            "verification": verification
                        }
                    )
                    
            except DataFetchError as e:
                logger.warning(f"Primary provider failed: {e.message}")
                last_error = e
        
        # Try fallback providers if primary didn't work
        if not result and self.fallback_providers:
            for provider in self.fallback_providers:
                if not provider.enabled:
                    continue
                
                try:
                    fallback_result = await self._get_block_height_from_provider(provider)
                    
                    # Verify with other providers
                    verification = await self._verify_block_height(
                        fallback_result["block_height"],
                        exclude_provider=provider.name
                    )
                    
                    # Check if result meets confidence threshold
                    if verification["confident"] and verification["all_sources_verify"]:
                        result = {
                            **fallback_result,
                            "verification": verification,
                            "verified": True
                        }
                        logger.info(
                            f"✅ Fallback verified block height: "
                            f"{fallback_result['block_height']:,} from {provider.name}"
                        )
                        break
                    else:
                        logger.warning(
                            f"Fallback provider {provider.name} "
                            f"below confidence threshold"
                        )
                        
                except DataFetchError as e:
                    logger.warning(f"Fallback provider {provider.name} failed: {e.message}")
                    continue
        
        # Raise error if no confident result found
        if not result:
            error_msg = f"Unable to get confident block height"
            if last_error:
                raise DataFetchError(
                    error_msg,
                    details={"last_error": last_error.message}
                )
            raise DataFetchError(error_msg)
        
        return result
    
    async def get_block_by_number(self, block_number: int, full_transactions: bool = False) -> Dict[str, Any]:
        """
        Get block data by block number.
        
        Args:
            block_number: Block number to fetch
            full_transactions: Whether to fetch full transaction objects
            
        Returns:
            Block data dictionary
            
        Raises:
            DataFetchError: If unable to fetch block data
        """
        # Use primary provider for block data
        provider = self.primary_provider
        
        if not provider or not provider.enabled:
            # Try fallback providers
            for p in self.fallback_providers:
                if p and p.enabled:
                    provider = p
                    break
        
        if not provider:
            raise DataFetchError("No available providers for block data fetch")
        
        block_hex = hex(block_number)
        
        try:
            block = await self._rpc_call(
                provider,
                "eth_getBlockByNumber",
                [block_hex, full_transactions]
            )
            
            if not block:
                raise DataFetchError("Block not found")
            
            # Parse block data
            parsed_block = {
                "number": int(block["number"], 16) if block["number"] else None,
                "hash": block["hash"],
                "parent_hash": block["parentHash"],
                "timestamp": int(block["timestamp"], 16) if block["timestamp"] else None,
                "transactions": block["transactions"],
                "transaction_count": len(block["transactions"]),
                "gas_used": int(block["gasUsed"], 16) if block["gasUsed"] else 0,
                "gas_limit": int(block["gasLimit"], 16) if block["gasLimit"] else 0,
                "provider": provider.name,
                "chain_id": provider.chain_id
            }
            
            logger.info(
                f"Retrieved block {parsed_block['number']:,} "
                f"with {parsed_block['transaction_count']} transactions"
            )
            
            return parsed_block
            
        except DataFetchError:
            raise
        except Exception as e:
            raise DataFetchError(
                f"Error parsing block data: {str(e)}",
                provider=provider.name
            )
    
    async def get_transaction_by_hash(self, tx_hash: str) -> Dict[str, Any]:
        """
        Get transaction data by transaction hash.
        
        Args:
            tx_hash: Transaction hash
            
        Returns:
            Transaction data dictionary
            
        Raises:
            DataFetchError: If unable to fetch transaction data
        """
        # Use primary provider for transaction data
        provider = self.primary_provider
        
        if not provider or not provider.enabled:
            for p in self.fallback_providers:
                if p and p.enabled:
                    provider = p
                    break
        
        if not provider:
            raise DataFetchError("No available providers for transaction data fetch")
        
        try:
            tx = await self._rpc_call(provider, "eth_getTransactionByHash", [tx_hash])
            
            if not tx:
                raise DataFetchError("Transaction not found")
            
            parsed_tx = {
                "hash": tx["hash"],
                "from": tx["from"],
                "to": tx["to"],
                "value": int(tx["value"], 16) if tx["value"] else 0,
                "gas": int(tx["gas"], 16) if tx["gas"] else 0,
                "gas_price": int(tx["gasPrice"], 16) if tx["gasPrice"] else 0,
                "block_number": int(tx["blockNumber"], 16) if tx["blockNumber"] else None,
                "transaction_index": int(tx["transactionIndex"], 16) if tx["transactionIndex"] else None,
                "provider": provider.name,
                "chain_id": provider.chain_id
            }
            
            logger.info(f"Retrieved transaction {tx_hash[:10]}... from block {parsed_tx['block_number']}")
            
            return parsed_tx
            
        except DataFetchError:
            raise
        except Exception as e:
            raise DataFetchError(
                f"Error parsing transaction data: {str(e)}",
                provider=provider.name
            )
