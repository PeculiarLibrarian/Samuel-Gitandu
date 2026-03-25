"""
Multi-network API configuration for PADI Bureau ingestion layer.
Supports OP Mainnet (primary) and Ethereum Mainnet (fallback).
Compliant with Nairobi-01 node 1003 rules enforcement.
"""

from typing import Dict, Optional, List
import os
from dotenv import load_dotenv
from dataclasses import dataclass
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()


@dataclass
class APIProvider:
    """
    Configuration for a single API provider.
    
    Attributes:
        name: Human-readable provider name
        endpoint: Full API endpoint URL
        api_key: API key (optional if included in endpoint)
        chain_id: EVM chain ID (None for non-EVM networks)
        network: Network type (mainnet, testnet)
        rate_limit_per_minute: Request rate limit
        enabled: Whether this provider is active
        network_type: EVM or non-EVM network type
    """
    name: str
    endpoint: str
    api_key: Optional[str] = None
    chain_id: Optional[int] = None
    network: str = "mainnet"
    rate_limit_per_minute: int = 100
    enabled: bool = True
    network_type: str = "evm"
    
    @property
    def full_url(self) -> str:
        """Construct full API URL with key if needed."""
        if self.api_key and "{key}" in self.endpoint:
            return self.endpoint.replace("{key}", self.api_key)
        return self.endpoint


class PADIConfig:
    """
    Centralized configuration for multi-network API providers.
    
    Primary: OP Mainnet (Optimism L2)
    Fallback: Ethereum Mainnet
    
    Enforces Nairobi-01 node 1003 rules for confidence scoring
    and verification requirements.
    """
    
    # ═════════════════════════════════════════════════════════════════
    # Node Configuration - Nairobi-01
    # ═════════════════════════════════════════════════════════════════
    
    NODE_LOCATION = os.getenv("PADI_NODE_LOCATION", "Nairobi-01")
    DEFAULT_NETWORK = os.getenv("PADI_DEFAULT_NETWORK", "mainnet")
    PRIMARY_NETWORK = os.getenv("PADI_PRIMARY_NETWORK", "op").lower()
    FALLBACK_NETWORKS = os.getenv("PADI_FALLBACK_NETWORKS", "ethereum").lower().split(",")
    MAX_DRIFT_SECONDS = int(os.getenv("PADI_MAX_DRIFT_SECONDS", "5"))
    BLOCK_REFRESH_INTERVAL = int(os.getenv("PADI_BLOCK_REFRESH_INTERVAL", "15"))
    
    # Logging configuration
    LOG_LEVEL = os.getenv("PADI_LOG_LEVEL", "INFO")
    DETAILED_LOGS = os.getenv("PADI_DETAILED_LOGS", "false").lower() == "true"
    
    # ═════════════════════════════════════════════════════════════════
    # 1003 Rule Enforcement (Architect Approval Required to Modify)
    # ═════════════════════════════════════════════════════════════════
    
    CONFIDENCE_THRESHOLD = float(os.getenv("PADI_CONFIDENCE_THRESHOLD", "1.0"))
    REQUIRED_VERIFICATION_SOURCES = int(os.getenv("PADI_REQUIRED_VERIFICATION_SOURCES", "3"))
    
    @classmethod
    def validate_1003_rules(cls) -> bool:
        """
        Validate that 1003 rule enforcement is correct.
        
        Returns:
            True if rules are correctly enforced
        """
        is_valid = (
            cls.CONFIDENCE_THRESHOLD == 1.0 and 
            cls.REQUIRED_VERIFICATION_SOURCES == 3
        )
        
        if not is_valid:
            logger.warning(
                f"⚠️  Nairobi-01 Rule 1003 Violation: "
                f"CONFIDENCE_THRESHOLD={cls.CONFIDENCE_THRESHOLD} "
                f"(expected 1.0), "
                f"REQUIRED_VERIFICATION_SOURCES={cls.REQUIRED_VERIFICATION_SOURCES} "
                f"(expected 3)"
            )
        
        return is_valid
    
    # ═════════════════════════════════════════════════════════════════
    # OP Mainnet (Optimism L2) - Primary Network
    # ═════════════════════════════════════════════════════════════════
    
    OP_MAINNET = APIProvider(
        name="Alchemy-OP-Mainnet",
        endpoint=os.getenv(
            "ALCHEMY_OP_MAINNET_ENDPOINT",
            "https://opt-mainnet.g.alchemy.com/v2/{key}"
        ),
        api_key=os.getenv("ALCHEMY_OP_MAINNET_API_KEY"),
        chain_id=10,  # Optimism Mainnet chain ID
        network="mainnet",
        rate_limit_per_minute=300,
        enabled=bool(os.getenv("ALCHEMY_OP_MAINNET_API_KEY")),
        network_type="evm"
    )
    
    OP_SEPOLIA = APIProvider(
        name="Alchemy-OP-Sepolia",
        endpoint=os.getenv(
            "ALCHEMY_OP_SEPOLIA_ENDPOINT",
            "https://opt-sepolia.g.alchemy.com/v2/{key}"
        ),
        api_key=os.getenv("ALCHEMY_OP_SEPOLIA_API_KEY"),
        chain_id=11155420,  # OP Sepolia chain ID
        network="sepolia",
        rate_limit_per_minute=300,
        enabled=bool(os.getenv("ALCHEMY_OP_SEPOLIA_API_KEY")),
        network_type="evm"
    )
    
    # ═════════════════════════════════════════════════════════════════
    # Ethereum Mainnet - Fallback Network
    # ═════════════════════════════════════════════════════════════════
    
    ETH_MAINNET = APIProvider(
        name="Alchemy-Ethereum-Mainnet",
        endpoint=os.getenv(
            "ALCHEMY_ETH_MAINNET_ENDPOINT",
            "https://eth-mainnet.g.alchemy.com/v2/{key}"
        ),
        api_key=os.getenv("ALCHEMY_ETH_MAINNET_API_KEY"),
        chain_id=1,  # Ethereum Mainnet chain ID
        network="mainnet",
        rate_limit_per_minute=300,
        enabled=bool(os.getenv("ALCHEMY_ETH_MAINNET_API_KEY")),
        network_type="evm"
    )
    
    ETH_SEPOLIA = APIProvider(
        name="Alchemy-Ethereum-Sepolia",
        endpoint=os.getenv(
            "ALCHEMY_ETH_SEPOLIA_ENDPOINT",
            "https://eth-sepolia.g.alchemy.com/v2/{key}"
        ),
        api_key=os.getenv("ALCHEMY_ETH_SEPOLIA_API_KEY"),
        chain_id=11155111,  # Ethereum Sepolia chain ID
        network="sepolia",
        rate_limit_per_minute=300,
        enabled=bool(os.getenv("ALCHEMY_ETH_SEPOLIA_API_KEY")),
        network_type="evm"
    )
    
    @classmethod
    def get_op_provider(cls, use_testnet: bool = False) -> Optional[APIProvider]:
        """
        Get the active OP (Optimism) provider.
        
        Args:
            use_testnet: Whether to use Sepolia testnet
            
        Returns:
            Configured provider or None
        """
        if use_testnet:
            return cls.OP_SEPOLIA if cls.OP_SEPOLIA.enabled else None
        return cls.OP_MAINNET
    
    @classmethod
    def get_eth_provider(cls, use_testnet: bool = False) -> Optional[APIProvider]:
        """
        Get the active Ethereum provider.
        
        Args:
            use_testnet: Whether to use Sepolia testnet
            
        Returns:
            Configured provider or None
        """
        if use_testnet:
            return cls.ETH_SEPOLIA if cls.ETH_SEPOLIA.enabled else None
        return cls.ETH_MAINNET
    
    @classmethod
    def get_provider_by_network(cls, network: str, use_testnet: bool = False) -> Optional[APIProvider]:
        """
        Get provider by network name.
        
        Args:
            network: "op", "ethereum", "eth", "base", "solana"
            use_testnet: Whether to use testnet/devnet
            
        Returns:
            Configured provider or None
        """
        network = network.lower()
        if network == "op":
            return cls.get_op_provider(use_testnet)
        elif network in ("ethereum", "eth"):
            return cls.get_eth_provider(use_testnet)
        return None
    
    @classmethod
    def get_primary_provider(cls) -> Optional[APIProvider]:
        """
        Get the primary provider based on PADI_PRIMARY_NETWORK setting.
        
        Returns:
            Configured primary provider or None
        """
        return cls.get_provider_by_network(
            cls.PRIMARY_NETWORK,
            use_testnet=(cls.DEFAULT_NETWORK == "testnet")
        )
    
    @classmethod
    def get_fallback_providers(cls) -> List[APIProvider]:
        """
        Get enabled fallback providers in priority order.
        
        Returns:
            List of enabled fallback providers
        """
        providers = []
        for network in cls.FALLBACK_NETWORKS:
            provider = cls.get_provider_by_network(network.strip())
            if provider and provider.enabled:
                providers.append(provider)
        return providers
    
    @classmethod
    def get_all_enabled_providers(cls) -> List[APIProvider]:
        """
        Get all enabled providers for fallback chain.
        
        Returns:
            List of all enabled providers
        """
        enabled = []
        
        # OP providers
        if cls.OP_MAINNET.enabled or cls.OP_SEPOLIA.enabled:
            op_provider = cls.get_op_provider(cls.DEFAULT_NETWORK == "testnet")
            if op_provider:
                enabled.append(op_provider)
        
        # Ethereum providers
        if cls.ETH_MAINNET.enabled or cls.ETH_SEPOLIA.enabled:
            eth_provider = cls.get_eth_provider(cls.DEFAULT_NETWORK == "testnet")
            if eth_provider:
                enabled.append(eth_provider)
        
        return enabled
    
    @classmethod
    def validate_config(cls) -> Dict[str, Dict[str, any]]:
        """
        Validate that required configurations are set.
        
        Returns:
            Dictionary containing validation results
        """
        results = {
            "op": {},
            "ethereum": {},
            "bureau": {},
            "1003_rule": {}
        }
        
        # Validate OP configuration
        results["op"]["mainnet"] = {
            "configured": bool(os.getenv("ALCHEMY_OP_MAINNET_API_KEY")),
            "endpoint": os.getenv("ALCHEMY_OP_MAINNET_ENDPOINT"),
            "provider": cls.OP_MAINNET.name,
            "enabled": cls.OP_MAINNET.enabled
        }
        results["op"]["sepolia"] = {
            "configured": bool(os.getenv("ALCHEMY_OP_SEPOLIA_API_KEY")),
            "endpoint": os.getenv("ALCHEMY_OP_SEPOLIA_ENDPOINT"),
            "provider": cls.OP_SEPOLIA.name,
            "enabled": cls.OP_SEPOLIA.enabled
        }
        
        # Validate Ethereum configuration
        results["ethereum"]["mainnet"] = {
            "configured": bool(os.getenv("ALCHEMY_ETH_MAINNET_API_KEY")),
            "endpoint": os.getenv("ALCHEMY_ETH_MAINNET_ENDPOINT"),
            "provider": cls.ETH_MAINNET.name,
            "enabled": cls.ETH_MAINNET.enabled
        }
        results["ethereum"]["sepolia"] = {
            "configured": bool(os.getenv("ALCHEMY_ETH_SEPOLIA_API_KEY")),
            "endpoint": os.getenv("ALCHEMY_ETH_SEPOLIA_ENDPOINT"),
            "provider": cls.ETH_SEPOLIA.name,
            "enabled": cls.ETH_SEPOLIA.enabled
        }
        
        # Validate bureau config
        results["bureau"] = {
            "node_location": cls.NODE_LOCATION,
            "default_network": cls.DEFAULT_NETWORK,
            "primary_network": cls.PRIMARY_NETWORK,
            "fallback_networks": cls.FALLBACK_NETWORKS,
            "log_level": cls.LOG_LEVEL
        }
        
        # Validate 1003 Rule configuration
        results["1003_rule"]["confidence_threshold"] = cls.CONFIDENCE_THRESHOLD
        results["1003_rule"]["required_sources"] = cls.REQUIRED_VERIFICATION_SOURCES
        results["1003_rule"]["valid"] = (
            cls.CONFIDENCE_THRESHOLD == 1.0 and 
            cls.REQUIRED_VERIFICATION_SOURCES == 3
        )
        
        return results
    
    @classmethod
    def print_config_summary(cls):
        """
        Print a formatted summary of current configuration.
        Useful for debugging and setup verification.
        """
        config = cls.validate_config()
        
        # Validate 1003 rules
        is_1003_valid = cls.validate_1003_rules()
        
        print("\n" + "=" * 70)
        print("🏛️  PADI Sovereign Bureau - Multi-Network Configuration")
        print("=" * 70)
        print(f"📍 Node: {cls.NODE_LOCATION}")
        print("=" * 70)
        
        # Primary network
        print(f"\n⭐ Primary Network: {cls.PRIMARY_NETWORK.upper()}")
        print("-" * 70)
        for mode, cfg in config[cls.PRIMARY_NETWORK].items():
            status = "✅ Enabled" if cfg["enabled"] else "⬜ Not Configured"
            print(f"  {mode}:")
            print(f"    • Status: {status}")
            if cfg["enabled"]:
                print(f"    • Provider: {cfg['provider']}")
                print(f"    • Endpoint: {cfg['endpoint'][:50]}...")
        
        # Fallback networks
        if cls.FALLBACK_NETWORKS:
            print(f"\n🔄 Fallback Networks: {', '.join(cls.FALLBACK_NETWORKS).upper()}")
            print("-" * 70)
            for net in cls.FALLBACK_NETWORKS:
                net = net.strip()
                if net in config:
                    for mode, cfg in config[net].items():
                        if cfg["enabled"]:
                            print(f"  • {net.upper()} {mode}:")
                            print(f"    - Provider: {cfg['provider']}")
        
        # Bureau configuration
        print("\n⚙️  Bureau Configuration:")
        print("-" * 70)
        for key, value in config["bureau"].items():
            print(f"  • {key}: {value}")
        
        # 1003 Rule
        print("\n🎯 1003 Rule Enforcement:")
        print("-" * 70)
        for key, value in config["1003_rule"].items():
            if key == "valid":
                status = "✅ Strict Compliance" if value else "⚠️ Modified"
                print(f"  • Rule Compliance: {status}")
            else:
                print(f"  • {key}: {value}")
        
        # Enabled providers
        enabled_providers = len(cls.get_all_enabled_providers())
        print(f"\n🔗 Total Enabled Providers: {enabled_providers}")
        print("=" * 70 + "\n")


# Initialize and print configuration on module load
if __name__ != "__main__":
    PADIConfig.print_config_summary()
