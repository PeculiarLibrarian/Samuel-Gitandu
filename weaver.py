#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import logging
from typing import Dict, List, Optional, Tuple, Any
from web3 import Web3
from datetime import datetime
from rdflib import RDF, Namespace, Graph

# =====================================================
# 🏛️ PADI WEAVER v2.1 — NAIROBI NODE-01
# ALIGNMENT STATUS: PRODUCTION CERTIFIED ✅
# =====================================================

# ✅ FIXED: Reference PadiConfig singleton, not legacy config
from padi_config import get_config as PadiConfig
from bureau_core import audit_signal

# Namespace Definitions
EX = Namespace("http://padi.u/schema#")
logger = logging.getLogger("PADI-WEAVER")


class Weaver:
    """
    V2.1 Features:
    - ✅ PadiConfig v5.0 Singleton Integration (Zero Drift)
    - ✅ Pre-flight Registry Handshake (Ontology Verification)
    - ✅ Schema Graph Access (Pre-Flight Verification)
    - ✅ Batch Processing (Multi-Signal Support)
    - ✅ Network Status Management
    - ✅ Diagnostics Integration
    """

    def __init__(self):
        """Initialize Weaver with PadiConfig v5.0 singleton."""
        # 🛡️ v2.1: Load PadiConfig singleton
        self.config = PadiConfig()
        
        # 🛡️ v2.1: Initialize with sovereign node identity
        self.node_id = self.config.node_id

        # Initialize multi-network nervous system
        self.w3_connections: Dict[str, Web3] = {}
        self._initialize_networks()

    def _initialize_networks(self):
        """Initialize Web3 connections using PadiConfig singleton."""
        # 🛡️ v2.1: Pull validated networks from singleton
        configured_networks = self.config.get_configured_networks()
        
        if not configured_networks:
            raise ConnectionError(f"❌ {self.node_id}: No networks configured.")
        
        logger.info(f"{self.node_id}: Initializing {len(configured_networks)} networks...")
        
        for name, net_cfg in configured_networks.items():
            try:
                w3 = Web3(Web3.HTTPProvider(net_cfg["rpc_url"]))
                if w3.is_connected():
                    self.w3_connections[name] = w3
                    connection_msg = (
                        f"🚀 {self.node_id}: Connected to {net_cfg['name']} "
                        f"(ID: {net_cfg['chain_id']})"
                    )
                    logger.info(connection_msg)
                    print(connection_msg)
                else:
                    warning_msg = f"⚠️ {self.node_id}: Connection failed for {net_cfg['name']}"
                    logger.warning(warning_msg)
                    print(warning_msg)
            except Exception as e:
                error_msg = f"❌ {self.node_id}: Error on {name}: {e}"
                logger.error(error_msg)
                print(error_msg)

        if not self.w3_connections:
            raise ConnectionError(
                f"❌ {self.node_id}: No networks connected. "
                "Weaver cannot process signals."
            )

    def _pre_flight_registry_handshake(
        self,
        action: str,
        target: str,
        network: str
    ) -> Tuple[bool, Optional[str]]:
        """
        🛡️ NEW v2.1: PRE-FLIGHT REGISTRY HANDSHAKE
        Verifies the action ontology before the signal hits the Audit phase.
        """
        # Step 1: Validate against PadiConfig Registry
        is_valid, error = self.config.verify_action_ontology(action, target)
        if not is_valid:
            return False, error
        
        # Step 2: Ensure network alignment
        if network not in self.w3_connections:
            return False, f"Network {network} is not active on Node {self.node_id}"
            
        return True, None

    def get_live_context(self, network_type: str) -> Optional[Dict[str, Any]]:
        """
        Fetch latest block and gas price from specified network.
        🛡️ v2.1: Uses blockchain timestamp (synchronizes across nodes).
        """
        # Validate network configuration
        if network_type not in self.w3_connections:
            logger.error(f"❌ Network {network_type} not connected. Cannot fetch context.")
            return None
        
        w3 = self.w3_connections[network_type]
        
        try:
            # ✅ Get full block details (more accurate than w3.eth.block_number)
            latest_block = w3.eth.get_block('latest')
            gas_price = w3.from_wei(w3.eth.gas_price, 'gwei')
            return {
                "block_number": latest_block['number'],
                "gas_price": float(gas_price),
                # ✅ Use blockchain timestamp (synchronizes across nodes)
                "timestamp": datetime.fromtimestamp(latest_block['timestamp']).isoformat(),
                "network_type": network_type
            }
        except Exception as e:
            logger.error(f"📡 RPC Sync Error on {network_type}: {e}")
            return None

    def process_signal(
        self,
        raw_data: Dict[str, Any],
        network_type: Optional[str] = None
    ) -> Optional[Graph]:
        """Process signal with Pre-flight Handshake and Sovereign Alignment."""
        
        # Infer network using singleton defaults
        network_type = network_type or raw_data.get(
            "network_type",
            self.config.DEFAULT_NETWORK_TYPE
        )
        
        # 🛡️ v2.1: Execute Pre-flight Handshake
        is_valid, error_msg = self._pre_flight_registry_handshake(
            raw_data.get("action", "OBSERVE"),
            raw_data.get("target", "0x000"),
            network_type
        )
        
        if not is_valid:
            logger.error(f"❌ Handshake Rejected: {error_msg}")
            print(f"❌ Handshake Rejected: {error_msg}")
            return None

        # Fetch Live Context
        ctx = self.get_live_context(network_type)
        if not ctx:
            safety_msg = (
                f"🛡️ Safety Lock: Signal rejected due to "
                f"Infrastructure Desync on {network_type}."
            )
            logger.warning(safety_msg)
            print(safety_msg)
            return None

        print(
            f"\n--- 🧵 Weaving Signal: {raw_data.get('label', 'Unknown')} "
            f"({network_type}) ---"
        )
        
        # Prepare parameters from PadiConfig
        net_cfg = self.config.get_network_config(network_type)
        
        # 🛡️ v2.1: Trigger Audit via bureau_core
        graph, conforms, status, report = audit_signal(
            name=raw_data.get("label", "UNLABELED_SIG"),
            confidence=raw_data.get("confidence", 0.0),
            sources=raw_data.get("sources", []),
            target_address=raw_data.get("target"),
            action_type=raw_data.get("action"),
            network_type=network_type,
            chain_id=net_cfg["chain_id"],
            block_number=ctx["block_number"],
            # 🛡️ Pass 1003 Rule enforcement toggle from config
            enforce_1003=self.config.validation_rules.get("enforce_1003_rule", True)
        )

        # Reporting
        if conforms:
            success_msg = (
                f"✅ DETERMINISTIC: Signal {raw_data.get('uid')} promoted.\n"
                f"📍 Target: {raw_data.get('target')} | Network: {network_type} "
                f"| Block: {ctx['block_number']}"
            )
            logger.info(success_msg)
            print(success_msg)
            return graph
        else:
            failure_msg = f"❌ PROBABILISTIC: Signal blocked. Reason: {report}"
            logger.warning(failure_msg)
            print(failure_msg)
            return None

    def process_batch(self, signals: List[Dict[str, Any]]) -> List[Graph]:
        """
        Processes multiple signals in a deterministic batch.
        🛡️ v2.1: NEW METHOD for multi-signal processing.
        
        Args:
            signals: List of signal data dictionaries
        
        Returns:
            List of RDF graphs for promoted ExecutableFacts
        """
        print(f"\n--- 🔄 Weaver: Processing batch of {len(signals)} signals ---")
        logger.info(f"Processing batch of {len(signals)} signals")
        
        results = []
        signal_ids = []
        
        # Group signals by network for efficient processing
        signals_by_network: Dict[str, List[Dict[str, Any]]] = {}
        for sig in signals:
            network_type = sig.get("network_type", self.config.DEFAULT_NETWORK_TYPE)
            if network_type not in signals_by_network:
                signals_by_network[network_type] = []
            signals_by_network[network_type].append(sig)
        
        # Process signals by network
        for network_type, network_signals in signals_by_network.items():
            print(f"\n--- 🔄 Processing {len(network_signals)} signals on {network_type} ---")
            logger.info(f"Processing {len(network_signals)} signals on {network_type}")
            
            for sig in network_signals:
                signal_ids.append(sig.get("uid"))
                graph = self.process_signal(sig, network_type)
                if graph:
                    results.append(graph)
        
        promoted_count = len(results)
        total_count = len(signals)
        completion_msg = (
            f"\n✅ Batch complete: {promoted_count}/{total_count} signals promoted."
        )
        logger.info(completion_msg)
        print(completion_msg)
        
        # Display breakdown by network
        for network_type, network_signals in signals_by_network.items():
            network_promoted = sum(
                1 for sig in network_signals
                if self._signal_was_promoted(sig, results)
            )
            network_msg = (
                f"   {network_type}: {network_promoted}/{len(network_signals)} promoted"
            )
            logger.info(network_msg)
            print(network_msg)
        
        return results

    def _signal_was_promoted(
        self,
        signal: Dict[str, Any],
        promoted_graphs: List[Graph]
    ) -> bool:
        """
        Check if a signal was promoted to ExecutableFact.
        🛡️ v2.1: NEW HELPER METHOD for signal tracking.
        
        Args:
            signal: Signal data dictionary
            promoted_graphs: List of promoted RDF graphs
        
        Returns:
            True if signal was promoted, False otherwise
        """
        signal_id = signal.get("uid")
        if not signal_id:
            return False
        
        # Check if any promoted graph contains this signal ID
        for graph in promoted_graphs:
            signal_id_values = graph.value(
                EX[signal_id.split("-")[0] if "-" in signal_id else signal_id],
                EX.hasSignalID
            )
            if signal_id_values and str(signal_id_values) == signal_id:
                return True
        
        return False
    
    def get_connected_networks(self) -> List[str]:
        """Get list of connected network types."""
        return list(self.w3_connections.keys())

    def get_network_status(self) -> Dict[str, Dict[str, str]]:
        """Get connection status for all configured networks."""
        status = {}
        configured_networks = self.config.get_configured_networks()
        
        for network_type, config in configured_networks.items():
            w3 = self.w3_connections.get(network_type)
            status[network_type] = {
                "name": config["name"],
                "chain_id": config["chain_id"],
                "connected": w3 is not None,
                "rpc_url": config["rpc_url"]
            }
        
        return status

    def get_diagnostics(self) -> Dict[str, Any]:
        """Get comprehensive diagnostics."""
        return {
            "node_id": self.node_id,
            "timestamp": datetime.now().isoformat(),
            "connected_networks": self.get_connected_networks(),
            "network_status": self.get_network_status(),
            "config_diagnostics": self.config.get_diagnostics()
        }


# ---------------------------
# Example / Standalone Test
# ---------------------------
if __name__ == "__main__":
    # Initialize PadiConfig singleton
    PadiConfig()
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("--- 🏛️ PADI WEAVER V2.1: NAIROBI NODE-01 STANDALONE TEST ---")
    print()
    
    try:
        weaver = Weaver()
        
        # Display network status
        print("=== Network Status ===")
        status = weaver.get_network_status()
        for network, info in status.items():
            conn_status = "✅ Connected" if info["connected"] else "❌ Disconnected"
            print(f"{info['name']} (Chain ID {info['chain_id']}): {conn_status}")
        print("")

        # Example batch of multi-network signals
        mock_batch = [
            {
                "label": "Alpha_Arb_OP_001",
                "confidence": 1.0,
                "sources": ["Pyth-OP", "Chainlink-OP", "Uniswap_Events-OP"],
                "target": "0x4752ba5DBc23f44D620376279d4b37A730947593",
                "action": "ARBITRAGE",
                "uid": "OP-TX-001",
                "network_type": "op-mainnet"
            },
            {
                "label": "Beta_Arb_ETH_001",
                "confidence": 1.0,
                "sources": ["Pyth-ETH", "Chainlink-ETH", "Uniswap_Events-ETH"],
                "target": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
                "action": "ARBITRAGE",
                "uid": "ETH-TX-001",
                "network_type": "eth-mainnet"
            },
            # Probabilistic Signal (Should fail)
            {
                "label": "Gamma_Arb_002",
                "confidence": 0.9,
                "sources": ["Pyth"],
                "target": "0x9999ba5DBc23f44D620376279d4b37A730947999",
                "action": "SWAP",
                "uid": "PROB-TX-001",
                "network_type": "op-mainnet"
            }
        ]

        weaver.process_batch(mock_batch)
        
        print()
        print("=== 🛡️ Diagnostics ===")
        diagnostics = weaver.get_diagnostics()
        print(f"Node ID: {diagnostics['node_id']}")
        print(f"Connected Networks: {', '.join(diagnostics['connected_networks'])}")
        print()
        print("=== Test Complete ===")
        
    except Exception as e:
        print(f"❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
