import sys
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from web3 import Web3
from rdflib import RDF, Namespace, Graph
from config import Config

# =====================================================
# 🏛️ PADI EXECUTOR v4.0 — NAIROBI NODE-01
# Phase Four: Multi-Network Execution Layer
# Fully aligned with ontology.ttl and shapes.ttl
# Supports: OP Mainnet, OP Sepolia, ETH Mainnet, ETH Sepolia
# =====================================================

EX = Namespace("http://padi.u/schema#")

# ---------------------------
# Network Configuration
# ---------------------------

NETWORK_CONFIG = {
    "op-mainnet": {
        "chain_id": 10,
        "rpc_url": getattr(Config, "OP_MAINNET_RPC_URL", None),
        "name": "OP Mainnet",
        "network_type": "layer2"
    },
    "op-sepolia": {
        "chain_id": 11155420,
        "rpc_url": getattr(Config, "OP_SEPOLIA_RPC_URL", None),
        "name": "OP Sepolia",
        "network_type": "layer2-testnet"
    },
    "eth-mainnet": {
        "chain_id": 1,
        "rpc_url": getattr(Config, "ETH_MAINNET_RPC_URL", None),
        "name": "Ethereum Mainnet",
        "network_type": "layer1"
    },
    "eth-sepolia": {
        "chain_id": 11155111,
        "rpc_url": getattr(Config, "ETH_SEPOLIA_RPC_URL", None),
        "name": "Ethereum Sepolia",
        "network_type": "layer1-testnet"
    },
    # Legacy Base L2 support (deprecated but maintained)
    "base-l2": {
        "chain_id": getattr(Config, "CHAIN_ID", 8453),
        "rpc_url": getattr(Config, "BASE_L2_RPC_URL", None),
        "name": "Base L2 (Legacy)",
        "network_type": "layer2-legacy"
    }
}

VALID_NETWORKS = list(NETWORK_CONFIG.keys())
VALID_CHAIN_IDS = {config["chain_id"]: network for network, config in NETWORK_CONFIG.items()}

# ---------------------------
# Logging Setup
# ---------------------------
logging.basicConfig(
    level=Config.LOG_LEVEL,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("PADI-EXECUTOR")


class Executor:
    """
    The Executor parses RDF graphs with ExecutableFacts,
    signs and broadcasts transactions to multiple networks
    (OP Mainnet, OP Sepolia, ETH Mainnet, ETH Sepolia),
    and maintains a JSON-audit log with optional read-only fallback.
    
    Multi-network capable with automatic network routing and validation.
    """

    def __init__(self):
        """Initialize Executor with multi-network support."""
        self.node_id = Config.NODE_ID

        # Load Wallet
        self.address = Config.PADI_WALLET_ADDRESS
        self.private_key = Config.PADI_PRIVATE_KEY

        # Initialize Web3 connections for all configured networks
        self.w3_connections: Dict[str, Web3] = {}
        self._initialize_networks()

        if not self.private_key:
            logger.warning(f"{self.node_id}: No Private Key provided. Read-only mode engaged.")

        # Track execution statistics per network
        self.execution_stats: Dict[str, Dict[str, int]] = {
            network: {
                "successful": 0,
                "failed": 0,
                "skipped": 0
            }
            for network in VALID_NETWORKS
        }

        self.audit_log = []

    def _initialize_networks(self):
        """
        Initialize Web3 connections for all configured networks.
        Logs warnings for networks without RPC URLs.
        """
        for network, config in NETWORK_CONFIG.items():
            rpc_url = config["rpc_url"]
            
            if rpc_url:
                try:
                    w3 = Web3(Web3.HTTPProvider(rpc_url))
                    if w3.is_connected():
                        self.w3_connections[network] = w3
                        logger.info(f"{self.node_id}: Connected to {config['name']} (Chain ID: {config['chain_id']})")
                    else:
                        logger.warning(f"{self.node_id}: Failed to connect to {config['name']} (RPC URL: {rpc_url})")
                except Exception as e:
                    logger.error(f"{self.node_id}: Error connecting to {config['name']}: {e}")
            else:
                logger.warning(f"{self.node_id}: No RPC URL configured for {config['name']}. Network unavailable.")

        if not self.w3_connections:
            logger.error(f"{self.node_id}: No networks configured. Executor cannot execute transactions.")
            sys.exit(1)

    # ---------------------------
    # RDF Parsing
    # ---------------------------
    def extract_facts(self, graph: Graph) -> List[Dict[str, any]]:
        """
        Extract ExecutableFact individuals from RDF graph.
        Returns list of dictionaries with required properties including network context.
        """
        facts = list(graph.subjects(RDF.type, EX.ExecutableFact))
        if not facts:
            logger.info("🛡️ Safety Lock: No ExecutableFact found in graph.")
            return []

        extracted = []
        for fact in facts:
            # Core properties
            target = graph.value(fact, EX.hasTargetAddress)
            action = graph.value(fact, EX.hasActionType)
            signal_id = graph.value(fact, EX.hasSignalID)
            confidence = graph.value(fact, EX.hasConfidence)

            # NEW: Network context properties
            network_type = graph.value(fact, EX.hasNetworkType)
            chain_id = graph.value(fact, EX.hasChainID)
            source_provider = graph.value(fact, EX.hasSourceProvider)
            primary_network = graph.value(fact, EX.hasPrimaryNetwork)
            cross_network_verification = graph.value(fact, EX.hasCrossNetworkVerification)

            if not target or not action or not signal_id:
                logger.warning(f"⚠️ Incomplete fact detected: {fact}. Skipping.")
                continue

            # NEW: Network validation
            extracted_network = str(network_type) if network_type else None
            if extracted_network and extracted_network not in VALID_NETWORKS:
                logger.warning(f"⚠️ Invalid network type detected: {extracted_network}. Skipping fact: {fact}.")
                continue

            # NEW: Chain ID validation
            extracted_chain_id = int(chain_id) if chain_id else None
            if extracted_chain_id:
                expected_chain_id = NETWORK_CONFIG.get(extracted_network, {}).get("chain_id")
                if expected_chain_id and extracted_chain_id != expected_chain_id:
                    logger.warning(
                        f"⚠️ Chain ID mismatch for {signal_id}: "
                        f"expected {expected_chain_id} for network {extracted_network}, got {extracted_chain_id}. Skipping."
                    )
                    continue

            extracted.append({
                "target": str(target),
                "action": str(action),
                "signal_id": str(signal_id),
                "confidence": float(confidence),
                # NEW: Network context
                "network_type": extracted_network,
                "chain_id": extracted_chain_id,
                "source_provider": str(source_provider) if source_provider else None,
                "primary_network": str(primary_network) if primary_network else None,
                "cross_network_verification": bool(cross_network_verification) if cross_network_verification is not None else True
            })
        return extracted

    # ---------------------------
    # Transaction Signing
    # ---------------------------
    def sign_and_send(
        self,
        target: str,
        action_type: str,
        signal_id: str,
        network_type: str,
        chain_id: int,
        gas_price: Optional[int] = None
    ) -> Optional[str]:
        """
        Sign and broadcast a transaction to the specified network.
        Zero-value triggers by default (e.g., ARB/SWAP/AUDIT).
        
        Args:
            target: Target contract address
            action_type: Action type (e.g., ARBITRAGE, SWAP, AUDIT)
            signal_id: Signal identifier
            network_type: Target network type
            chain_id: Network chain ID
            gas_price: Optional gas price override
        
        Returns:
            Transaction hash if successful, None otherwise
        """
        # Check for read-only mode
        if not self.private_key or self.private_key.startswith("Your"):
            logger.warning(f"❌ Signing skipped for {signal_id} — Read-only mode.")
            return None

        # Validate network configuration
        if network_type not in self.w3_connections:
            logger.error(f"❌ Network {network_type} not connected. Cannot execute {signal_id}.")
            self.execution_stats[network_type]["failed"] += 1
            return None

        w3 = self.w3_connections[network_type]
        network_name = NETWORK_CONFIG[network_type]["name"]
        expected_chain_id = NETWORK_CONFIG[network_type]["chain_id"]

        # Validate chain ID
        if chain_id != expected_chain_id:
            logger.error(
                f"❌ Chain ID mismatch for {signal_id}: "
                f"expected {expected_chain_id} for {network_name}, got {chain_id}."
            )
            self.execution_stats[network_type]["failed"] += 1
            return None

        try:
            nonce = w3.eth.get_transaction_count(self.address)
            gas_price_final = gas_price or w3.eth.gas_price

            tx = {
                'nonce': nonce,
                'to': Web3.to_checksum_address(target),
                'value': w3.to_wei(0, 'ether'),
                'gas': 250_000,
                'gasPrice': int(gas_price_final),
                'chainId': chain_id
            }

            logger.info(f"🚀 Signing {action_type} for Signal {signal_id} on {network_name}")
            signed_tx = w3.eth.account.sign_transaction(tx, self.private_key)
            tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction)
            hex_hash = w3.to_hex(tx_hash)
            
            logger.info(
                f"✅ Dispatched: {signal_id} | Network: {network_name} "
                f"| Chain ID: {chain_id} | TX Hash: {hex_hash}"
            )
            
            self.execution_stats[network_type]["successful"] += 1
            return hex_hash

        except Exception as e:
            logger.error(f"❌ Transaction FAILED for {signal_id} on {network_name}: {e}")
            self.execution_stats[network_type]["failed"] += 1
            return None

    # ---------------------------
    # Batch Execution
    # ---------------------------
    def execute_batch(
        self,
        promoted_graphs: List[Graph],
        gas_price: Optional[int] = None
    ) -> List[Optional[str]]:
        """
        Iterate through a batch of RDF graphs containing ExecutableFacts.
        Routes transactions to appropriate networks based on network context.
        
        Args:
            promoted_graphs: List of RDF graphs with ExecutableFacts
            gas_price: Optional gas price override for all transactions
        
        Returns:
            List of transaction hashes (None for failures/skipped)
        """
        receipts = []

        for graph in promoted_graphs:
            fact_list = self.extract_facts(graph)
            for fact in fact_list:
                # NEW: Extract network context
                network_type = fact.get("network_type")
                chain_id = fact.get("chain_id")

                # Skip if network context is missing
                if not network_type or not chain_id:
                    logger.warning(
                        f"⚠️ Network context missing for {fact['signal_id']}. Skipping execution."
                    )
                    self.execution_stats.get(network_type, {}).setdefault("skipped", 0)
                    receipts.append(None)
                    continue

                tx_hash = self.sign_and_send(
                    target=fact['target'],
                    action_type=fact['action'],
                    signal_id=fact['signal_id'],
                    network_type=network_type,  # ✅ FIXED: ASCII COMMA
                    chain_id=chain_id,
                    gas_price=gas_price
                )
                receipts.append(tx_hash)

            # Audit log snapshot
            ttl_snapshot = graph.serialize(format='turtle')
            self.audit_log.append(ttl_snapshot)

        return receipts

    # ---------------------------
    # Network Management
    # ---------------------------
    def get_network_status(self) -> Dict[str, Dict[str, str]]:
        """
        Get connection status for all configured networks.
        
        Returns:
            Dictionary mapping network names to status information
        """
        status = {}
        for network, config in NETWORK_CONFIG.items():
            w3 = self.w3_connections.get(network)
            status[network] = {
                "name": config["name"],
                "chain_id": config["chain_id"],
                "connected": w3 is not None,
                "rpc_url": config["rpc_url"]
            }
        return status

    def get_execution_stats(self) -> Dict[str, Dict[str, int]]:
        """
        Get execution statistics per network.
        
        Returns:
            Dictionary mapping network names to execution statistics
        """
        return self.execution_stats

    def reset_execution_stats(self):
        """Reset execution statistics for all networks."""
        for network in VALID_NETWORKS:
            self.execution_stats[network] = {
                "successful": 0,
                "failed": 0,
                "skipped": 0
            }


# ---------------------------
# Standalone Test Entry
# ---------------------------
if __name__ == "__main__":
    if Config.validate():
        logger.info(f"--- 🏛️ PADI EXECUTOR: {Config.NODE_ID} READY ---")
        executor = Executor()

        # Display network status
        logger.info("=== Network Status ===")
        status = executor.get_network_status()
        for network, info in status.items():
            conn_status = "✅ Connected" if info["connected"] else "❌ Disconnected"
            logger.info(f"{info['name']} (Chain ID {info['chain_id']}): {conn_status}")
        logger.info()

        # Example 1: OP Mainnet ExecutableFact
        logger.info("=== Example 1: OP Mainnet Transaction ===")
        g1 = Graph()
        node1 = EX["OP_Transaction_01"]
        g1.add((node1, RDF.type, EX.ExecutableFact))
        g1.add((node1, EX.hasTargetAddress, "0x4752ba5DBc23f44D620376279d4b37A730947593"))
        g1.add((node1, EX.hasActionType, "ARBITRAGE"))
        g1.add((node1, EX.hasSignalID, "OP-TX-001"))
        g1.add((node1, EX.hasConfidence, 1.0))
        # NEW: OP Mainnet network context
        g1.add((node1, EX.hasNetworkType, "op-mainnet"))
        g1.add((node1, EX.hasChainID, 10))
        g1.add((node1, EX.hasSourceProvider, "Alchemy-OP-Mainnet"))

        executor.execute_batch([g1])
        logger.info()

        # Example 2: Ethereum Mainnet ExecutableFact
        logger.info("=== Example 2: Ethereum Mainnet Transaction ===")
        g2 = Graph()
        node2 = EX["ETH_Transaction_01"]
        g2.add((node2, RDF.type, EX.ExecutableFact))
        g2.add((node2, EX.hasTargetAddress, "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045"))
        g2.add((node2, EX.hasActionType, "SWAP"))
        g2.add((node2, EX.hasSignalID, "ETH-TX-001"))
        g2.add((node2, EX.hasConfidence, 1.0))
        # NEW: Ethereum Mainnet network context
        g2.add((node2, EX.hasNetworkType, "eth-mainnet"))
        g2.add((node2, EX.hasChainID, 1))
        g2.add((node2, EX.hasSourceProvider, "Alchemy-ETH-Mainnet"))

        executor.execute_batch([g2])
        logger.info()

        # Display execution statistics
        logger.info("=== Execution Statistics ===")
        stats = executor.get_execution_stats()
        for network, stat in stats.items():
            logger.info(
                f"{network}: "
                f"✅ {stat['successful']} | ❌ {stat['failed']} | ⏭️ {stat['skipped']}"
            )
        logger.info()

        logger.info("✅ Standalone test complete. Audit log captured.")
