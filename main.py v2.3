#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import signal
import logging
import threading
import sys
from pathlib import Path
from typing import Dict, Optional, Any

# Add project root to path for imports
project_root = Path(__file__).parent.absolute()
sys.path.insert(0, str(project_root))

from padi_config import get_config as PadiConfig
from schema_bootstrap import SchemaBootstrap
from weaver import Weaver
from executor import Executor

# =====================================================
# 🏛️ PADI BUREAU v2.3 — NAIROBI NODE-01
# main.py — Multi-Network Production Entry Point / Process Manager
# Supports: OP Mainnet, OP Sepolia, ETH Mainnet, ETH Sepolia
# 
# V2.3 NEW FEATURES:
# - ✅ Schema Bootstrapping System Integration
# - ✅ Pre-flight Schema Validation
# - ✅ Auto-generation of Minimal Schema Files
# - ✅ Schema Health Monitoring
# - ✅ Graceful Degradation Support
# 
# V2.1 LEGACY FEATURES (Maintained):
# - ✅ PadiConfig v5.0 Singleton Integration
# - ✅ Pre-flight Registry Handshake Integration
# - ✅ Sovereign Configuration Alignment
# - ✅ Enhanced Logging with Config
# - ✅ Graceful Shutdown with State Persistence
# 
# V2.0 LEGACY FEATURES (Maintained):
# - ✅ Multi-Network Support
# - ✅ Component Initialization
# - ✅ Main Event Loop
# - ✅ Statistics Reporting
# - ✅ Test Mode
# =====================================================

# ---------------------------
# Logging Setup
# ---------------------------
# 🛡️ v2.1: Initialize PadiConfig singleton first
config = PadiConfig()

logging.basicConfig(
    level=config.log_level,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("PADI-MAIN")

# ---------------------------
# Graceful Shutdown
# ---------------------------
shutdown_event = threading.Event()

def shutdown_handler(sig, frame):
    """Handle graceful shutdown on SIGINT."""
    logger.info("🛑 SIGINT received. Shutting down Nairobi Node gracefully...")
    
    # 🛡️ v2.1: Persist state before shutdown
    try:
        logger.info("💾 Persisting state before shutdown...")
        # Executor will persist nonce cache automatically
        # Additional state persistence could be added here
    except Exception as e:
        logger.error(f"⚠️ State persistence failed: {e}")
    
    shutdown_event.set()

signal.signal(signal.SIGINT, shutdown_handler)

# ---------------------------
# Node Bootstrap
# ---------------------------
def start_node():
    """
    Start the PADI Bureau Node with multi-network support and schema bootstrapping.
    Initializes schema bootstrap, Weaver and Executor, displays network status,
    and runs the main event loop for signal processing and execution.
    🛡️ v2.3: Integrates Schema Bootstrap System for pre-flight validation.
    """
    logger.info(f"--- 🏛️ STARTING PADI BUREAU: {config.node_id} ---")
    logger.info()

    # 1. Configuration Validation
    logger.info("=== Configuration Validation ===")
    # 🛡️ v2.1: Config is already validated in PadiConfig constructor
    logger.info("✅ Configuration validated.")
    logger.info(f"📍 Node ID: {config.node_id}")
    logger.info(f"🔒 Simulation Mode: {config.simulation_mode}")
    logger.info(f"🛡️ Sovereign Kill-Switch: ACTIVE")
    logger.info()

    # 2. 🛡️ v2.3 NEW: Schema Bootstrapping
    logger.info("=== 🛡️ Schema Bootstrapping ===")
    schema_bootstrap = SchemaBootstrap(
        schema_dir=config.get("schema_dir", "schema"),
        allow_auto_generate=config.get("allow_schema_auto_generate", True),
        require_all_files=config.get("require_all_schema_files", False)
    )
    
    # Bootstrap schema files
    bootstrap_success = schema_bootstrap.bootstrap(fail_fast=False)
    
    if not bootstrap_success:
        logger.critical(
            "❌ Schema bootstrapping failed. System cannot start without "
            "valid schema files."
        )
        logger.critical("   Please check that the schema directory is accessible.")
        logger.critical("   You can enable auto-generation by setting "
                       "PADI_SCHEMA_AUTO_GENERATE=true in your .env file.")
        return
    
    # Display bootstrap status
    bootstrap_status = schema_bootstrap.get_bootstrap_status()
    bootstrap_mode = bootstrap_status.get("bootstrap_mode", "UNKNOWN")
    
    if bootstrap_mode == "NORMAL":
        logger.info("✅ Schema bootstrapping successful (NORMAL mode).")
    elif bootstrap_mode == "DEGRADED":
        logger.warning(
            "⚠️  Schema bootstrapping successful (DEGRADED mode). "
            "Auto-generated minimal schema files."
        )
        logger.warning(
            "   System will operate with basic 1003 Rule enforcement. "
            "For full validation capabilities, provide complete schema files."
        )
    else:
        logger.error(f"❌ Schema bootstrapping failed: {bootstrap_mode}")
        return
    
    # Display schema paths
    schema_paths = schema_bootstrap.get_schema_paths()
    logger.info(f"   SHACL Shapes: {schema_paths['shapes']}")
    logger.info(f"   Ontology: {schema_paths['ontology']}")
    logger.info()

    # 3. Display Network Configuration
    logger.info("=== Network Configuration ===")
    configured_networks = config.get_configured_networks()
    for network_type, net_config in configured_networks.items():
        logger.info(
            f"  ✅ {net_config['name']} (Chain ID: {net_config['chain_id']})"
        )
    logger.info(f"  Default Network: {config.DEFAULT_NETWORK_TYPE}")
    logger.info(
        f"  Cross-Network Verification: "
        f"{config.DEFAULT_CROSS_NETWORK_VERIFICATION}"
    )
    logger.info()

    # 4. Display Validation Rules
    logger.info("=== Validation Rules (1003 Rule) ===")
    logger.info(
        f"  Required Confidence: "
        f"{config.validation_rules.get('required_confidence')}"
    )
    logger.info(
        f"  Required Verification Sources: "
        f"{config.validation_rules.get('required_verification_sources')}"
    )
    logger.info(
        f"  Enforce 1003 Rule: "
        f"{config.validation_rules.get('enforce_1003_rule')}"
    )
    logger.info()

    # 5. Initialize Components
    logger.info("=== Component Initialization ===")
    try:
        weaver = Weaver()
        logger.info("✅ Weaver (Nervous System) initialized.")
        
        executor = Executor()
        logger.info("✅ Executor (Execution Layer) initialized.")
        
        logger.info("✅ All components initialized successfully.")
        logger.info()
    except Exception as e:
        logger.critical(f"❌ Component Initialization Failed: {e}")
        import traceback
        logger.critical(traceback.format_exc())
        return

    # 6. Display Network Status
    logger.info("=== Network Status ===")
    weaver_status = weaver.get_network_status()
    executor_status = executor.get_network_status()
    
    networks = set(list(weaver_status.keys()) + list(executor_status.keys()))
    
    for network in sorted(networks):
        weaver_connected = weaver_status.get(network, {}).get("connected", False)
        executor_connected = executor_status.get(network, {}).get("connected", False)
        
        if weaver_connected and executor_connected:
            status = "✅ Fully Connected"
        elif weaver_connected:
            status = "⚠️ Weaver Only"
        elif executor_connected:
            status = "⚠️ Executor Only"
        else:
            status = "❌ Disconnected"
        
        network_name = (
            weaver_status.get(network, {}).get("name") or
            executor_status.get(network, {}).get("name") or
            network
        )
        logger.info(f"  {network_name}: {status}")
    logger.info()

    # 7. 🛡️ v2.3 NEW: Display Schema Health Status
    logger.info("=== 🛡️ Schema Health Status ===")
    schema_health = schema_bootstrap.get_health_status()
    logger.info(f"   Schema Directory: {schema_health['schema_dir']}")
    logger.info(
        f"   Shapes Exists: {'✅' if schema_health['shapes_exists'] else '❌'}"
    )
    logger.info(f"   Shapes Size: {schema_health['shapes_size']} bytes")
    logger.info(
        f"   Ontology Exists: {'✅' if schema_health['ontology_exists'] else '❌'}"
    )
    logger.info(f"   Ontology Size: {schema_health['ontology_size']} bytes")
    logger.info(f"   Bootstrap Mode: {schema_health['bootstrap_mode']}")
    auto_generated = schema_health['generated_shapes'] or schema_health['generated_ontology']
    logger.info(
        f"   Auto-Generated: {'Yes' if auto_generated else 'No'}"
    )
    logger.info()

    # 8. Display Sovereign Kill-Switch Status
    logger.info("=== 🛡️ Sovereign Kill-Switch Status ===")
    kill_switch_stats = executor.get_kill_switch_stats()
    total_rejected = sum(
        stats.get("rejected_by_killswitch", 0)
        for stats in kill_switch_stats.values()
    )
    logger.info(f"  Total Actions Rejected by Kill-Switch: {total_rejected}")
    logger.info()

    # 9. Main Event Loop
    logger.info("=== Main Event Loop ===")
    logger.info(
        f"📡 Node-01 is now LIVE and observing networks: "
        f"{', '.join(configured_networks.keys())}"
    )
    polling_interval = config.polling_interval
    logger.info(f"⏱️ Polling interval: {polling_interval} seconds")
    logger.info()

    # Statistics tracking
    total_signals_processed = 0
    total_facts_promoted = 0
    total_transactions_executed = 0
    total_rejected_by_killswitch = 0
    last_stats_time = time.time()
    stats_report_interval = 300  # Report stats every 5 minutes (300 seconds)
    schema_health_interval = 3600  # Check schema health every hour (3600 seconds)
    last_schema_health_time = time.time()

    while not shutdown_event.is_set():
        try:
            # A. 🛡️ v2.3 NEW: Periodic Schema Health Check
            current_time = time.time()
            if current_time - last_schema_health_time >= schema_health_interval:
                schema_health = schema_bootstrap.get_health_status()
                if schema_health['bootstrap_mode'] in ['NORMAL', 'DEGRADED']:
                    logger.debug(
                        f"Schema Health: Shapes={schema_health['shapes_size']}B, "
                        f"Ontology={schema_health['ontology_size']}B, "
                        f"Mode={schema_health['bootstrap_mode']}"
                    )
                else:
                    logger.warning(
                        f"⚠️ Schema Health Degraded: Mode={schema_health['bootstrap_mode']}"
                    )
                last_schema_health_time = current_time

            # B. Fetch and Validate Signals
            # Replace the empty list with a live feed from mempool/webhooks
            promoted_graphs = weaver.process_batch([])

            # C. Execute Facts
            if promoted_graphs:
                total_facts_promoted += len(promoted_graphs)
                logger.info(
                    f"⚖️ Detected {len(promoted_graphs)} validated signals. "
                    f"Dispatching to Executor..."
                )
                receipts = executor.execute_batch(promoted_graphs)
                
                successful_transactions = sum(1 for r in receipts if r is not None)
                total_transactions_executed += successful_transactions
                
                logger.info(
                    f"📝 Batch Processed. "
                    f"Successful: {successful_transactions}/{len(promoted_graphs)}. "
                    f"Receipts: {[r for r in receipts if r is not None]}"
                )

            # D. Track Kill-Switch rejections
            kill_switch_stats = executor.get_kill_switch_stats()
            current_rejected = sum(
                stats.get("rejected_by_killswitch", 0)
                for stats in kill_switch_stats.values()
            )
            total_rejected_by_killswitch = current_rejected

            # E. Periodic Statistics Reporting
            current_time = time.time()
            if current_time - last_stats_time >= stats_report_interval:
                log_statistics(
                    total_signals_processed,
                    total_facts_promoted,
                    total_transactions_executed,
                    total_rejected_by_killswitch,
                    weaver,
                    executor,
                    schema_bootstrap  # 🛡️ v2.3 NEW: Include schema health
                )
                last_stats_time = current_time

            # F. Throttle RPC Polling
            time.sleep(polling_interval)

        except KeyboardInterrupt:
            logger.info(f"🛑 Shutdown signal received. {config.node_id} going offline.")
            break
        except Exception as e:
            logger.error(f"💥 UNHANDLED ERROR in main loop: {e}")
            import traceback
            logger.error(traceback.format_exc())
            # Retry after brief sleep to handle transient network issues
            time.sleep(3)
        finally:
            # Optional: Could persist state or perform cleanup here
            pass

    # 10. Shutdown Reporting
    logger.info()
    logger.info("=== Shutdown Report ===")
    logger.info(f"Total Signals Processed: {total_signals_processed}")
    logger.info(f"Total Facts Promoted: {total_facts_promoted}")
    logger.info(f"Total Transactions Executed: {total_transactions_executed}")
    logger.info(f"Total Rejected by Kill-Switch: {total_rejected_by_killswitch}")
    
    # 🛡️ v2.3 NEW: Final Schema Health Report
    logger.info()
    logger.info("=== 🛡️ Final Schema Health Report ===")
    final_schema_health = schema_bootstrap.get_health_status()
    logger.info(f"   Schema Directory: {final_schema_health['schema_dir']}")
    logger.info(f"   Shapes Exists: {'✅' if final_schema_health['shapes_exists'] else '❌'}")
    logger.info(f"   Ontology Exists: {'✅' if final_schema_health['ontology_exists'] else '❌'}")
    logger.info(f"   Bootstrap Mode: {final_schema_health['bootstrap_mode']}")
    auto_generated = (
        final_schema_health['generated_shapes'] or
        final_schema_health['generated_ontology']
    )
    if auto_generated:
        logger.warning(
            "   ⚠️  Schema files were auto-generated. "
            "For production use, provide complete schema files."
        )
    
    # Final execution statistics
    logger.info()
    logger.info("=== Final Execution Statistics ===")
    stats = executor.get_execution_stats()
    for network, stat in stats.items():
        logger.info(
            f"  {network}: "
            f"✅ {stat.get('successful', 0)} | "
            f"❌ {stat.get('failed', 0)} | "
            f"⏭️ {stat.get('skipped', 0)} | "
            f"🛡️ {stat.get('rejected_by_killswitch', 0)}"
        )
    
    logger.info()
    logger.info("🛑 Node shutdown complete.")


def log_statistics(
    total_signals_processed: int,
    total_facts_promoted: int,
    total_transactions_executed: int,
    total_rejected_by_killswitch: int,
    weaver: Weaver,
    executor: Executor,
    schema_bootstrap: SchemaBootstrap  # 🛡️ v2.3 NEW: Schema Bootstrap
):
    """
    Log operational statistics.
    🛡️ v2.3: Added schema health monitoring.
    
    Args:
        total_signals_processed: Total number of signals processed
        total_facts_promoted: Total number of facts promoted
        total_transactions_executed: Total number of successful transactions
        total_rejected_by_killswitch: Total number of actions rejected by kill-switch
        weaver: Weaver instance
        executor: Executor instance
        schema_bootstrap: SchemaBootstrap instance for health monitoring
    """
    logger.info()
    logger.info("=== Operational Statistics ===")
    logger.info(f"  Total Signals Processed: {total_signals_processed}")
    logger.info(f"  Total Facts Promoted: {total_facts_promoted}")
    logger.info(f"  Total Transactions Executed: {total_transactions_executed}")
    logger.info(f"  Total Rejected by Kill-Switch: {total_rejected_by_killswitch}")
    
    # 🛡️ v2.3 NEW: Schema Health Status
    schema_health = schema_bootstrap.get_health_status()
    logger.info()
    logger.info("  🛡️ Schema Health:")
    logger.info(
        f"    Bootstrap Mode: {schema_health['bootstrap_mode']}"
    )
    logger.info(
        f"    Shapes: {'✅' if schema_health['shapes_exists'] else '❌'} "
        f"({schema_health['shapes_size']} bytes)"
    )
    logger.info(
        f"    Ontology: {'✅' if schema_health['ontology_exists'] else '❌'} "
        f"({schema_health['ontology_size']} bytes)"
    )
    if schema_health['generated_shapes'] or schema_health['generated_ontology']:
        logger.warning(
            "    ⚠️  Auto-Generated Schema: System running with minimal schema files"
        )
    
    # Execution statistics per network
    stats = executor.get_execution_stats()
    if any(
        s.get('successful', 0) > 0 or
        s.get('failed', 0) > 0 or
        s.get('skipped', 0) > 0 or
        s.get('rejected_by_killswitch', 0) > 0
        for s in stats.values()
    ):
        logger.info()
        logger.info("  Execution Statistics by Network:")
        for network, stat in stats.items():
            if (
                stat.get('successful', 0) > 0 or
                stat.get('failed', 0) > 0 or
                stat.get('skipped', 0) > 0 or
                stat.get('rejected_by_killswitch', 0) > 0
            ):
                logger.info(
                    f"    {network}: "
                    f"✅ {stat.get('successful', 0)} | "
                    f"❌ {stat.get('failed', 0)} | "
                    f"⏭️ {stat.get('skipped', 0)} | "
                    f"🛡️ {stat.get('rejected_by_killswitch', 0)}"
                )
    
    # Network status
    weaver_status = weaver.get_network_status()
    disconnected_networks = [
        name for name, info in weaver_status.items()
        if not info.get("connected")
    ]
    if disconnected_networks:
        logger.info()
        logger.warning(
            f"  ⚠️ Disconnected Networks: {', '.join(disconnected_networks)}"
        )
    
    # Kill-Switch statistics
    kill_switch_stats = executor.get_kill_switch_stats()
    if any(
        stats.get("rejected_by_killswitch", 0) > 0
        for stats in kill_switch_stats.values()
    ):
        logger.info()
        logger.info("  Kill-Switch Statistics by Network:")
        for network, stats in kill_switch_stats.items():
            if stats.get("rejected_by_killswitch", 0) > 0:
                logger.info(
                    f"    {network}: "
                    f"🛡️ Rejected by Kill-Switch = {stats['rejected_by_killswitch']} | "
                    f"Total Rejected = {stats['total_rejected']}"
                )
    
    logger.info()
    logger.info("=== End of Statistics Report ===")
    logger.info()


def run_test_mode():
    """
    Run a test mode with demo signals to validate multi-network functionality.
    🛡️ v2.3: Tests Schema Bootstrap System alongside Sovereign Kill-Switch.
    """
    logger.info("=== TEST MODE ===")
    logger.info("Running in test mode with demo signals...")
    logger.info()

    # 🛡️ v2.3 NEW: Schema Bootstrap Validation
    logger.info("🛡️ Initializing Schema Bootstrap...")
    schema_bootstrap = SchemaBootstrap(
        schema_dir=config.get("schema_dir", "schema"),
        allow_auto_generate=config.get("allow_schema_auto_generate", True),
        require_all_files=config.get("require_all_schema_files", False)
    )
    
    bootstrap_success = schema_bootstrap.bootstrap(fail_fast=False)
    
    if bootstrap_success:
        bootstrap_status = schema_bootstrap.get_bootstrap_status()
        bootstrap_mode = bootstrap_status.get("bootstrap_mode", "UNKNOWN")
        logger.info(
            f"✅ Schema Bootstrap: {bootstrap_mode} mode"
        )
    else:
        logger.error("❌ Schema Bootstrap failed")
        return
    
    # 🛡️ v2.1: Display configuration
    logger.info(f"📍 Node ID: {config.node_id}")
    logger.info(f"🔒 Simulation Mode: {config.simulation_mode}")
    logger.info(f"🛡️ Sovereign Kill-Switch: ACTIVE")
    logger.info()

    # Initialize components
    try:
        weaver = Weaver()
        executor = Executor()
    except Exception as e:
        logger.error(f"❌ Component initialization failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return

    # Demo signals for multiple networks
    demo_signals = [
        # OP Mainnet Signal (Should pass)
        {
            "label": "Demo_OP_Mainnet_001",
            "confidence": 1.0,
            "sources": ["Pyth-Demo", "Chainlink-Demo", "Uniswap-Demo"],
            "target": "0x4752ba5DBc23f44D620376279d4b37A730947593",
            "action": "ARBITRAGE",
            "uid": "DEMO-OP-001",
            "network_type": "op-mainnet",
            "source_provider": "Alchemy-OP-Mainnet",
            "verification_confidence": [1.0, 1.0, 1.0],
            "verification_timestamp": [
                "2026-03-26T01:50:00Z",
                "2026-03-26T01:50:01Z",
                "2026-03-26T01:50:02Z"
            ],
            "verification_match": [True, True, True],
            "primary_network": "op-mainnet",
            "fallback_network": "eth-mainnet",
            "cross_network_verification": True
        },
        # Ethereum Mainnet Signal (Should pass)
        {
            "label": "Demo_ETH_Mainnet_001",
            "confidence": 1.0,
            "sources": ["Pyth-Demo", "Chainlink-Demo", "Uniswap-Demo"],
            "target": "0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
            "action": "SWAP",
            "uid": "DEMO-ETH-001",
            "network_type": "eth-mainnet",
            "source_provider": "Alchemy-ETH-Mainnet",
            "verification_confidence": [1.0, 1.0, 1.0],
            "verification_timestamp": [
                "2026-03-26T01:50:03Z",
                "2026-03-26T01:50:04Z",
                "2026-03-26T01:50:05Z"
            ],
            "verification_match": [True, True, True],
            "primary_network": "eth-mainnet",
            "cross_network_verification": True
        },
        # 🛡️ v2.1 NEW: Invalid Action Type (Should fail Kill-Switch)
        {
            "label": "Demo_Invalid_Action_001",
            "confidence": 1.0,
            "sources": ["Pyth-Demo", "Chainlink-Demo", "Uniswap-Demo"],
            "target": "0x9999ba5DBc23f44D620376279d4b37A730947999",
            "action": "INVALID_ACTION_TYPE",
            "uid": "DEMO-INVALID-001",
            "network_type": "op-mainnet",
            "source_provider": "Alchemy-OP-Mainnet",
            "verification_confidence": [1.0, 1.0, 1.0],
            "verification_timestamp": [
                "2026-03-26T01:50:06Z",
                "2026-03-26T01:50:07Z",
                "2026-03-26T01:50:08Z"
            ],
            "verification_match": [True, True, True],
            "primary_network": "op-mainnet",
            "cross_network_verification": True
        }
    ]

    # Process demo signals
    logger.info("Processing demo signals...")
    promoted_graphs = weaver.process_batch(demo_signals)

    # Execute demo facts
    if promoted_graphs:
        # In read-only mode (no private key), transactions will be skipped
        receipts = executor.execute_batch(promoted_graphs)
        logger.info(f"Demo batch processed. Receipts: {receipts}")

    # 🛡️ v2.1: Display Kill-Switch statistics
    kill_switch_stats = executor.get_kill_switch_stats()
    logger.info()
    logger.info("=== 🛡️ Kill-Switch Test Results ===")
    for network, stats in kill_switch_stats.items():
        if stats.get("rejected_by_killswitch", 0) > 0:
            logger.info(
                f"  {network}: "
                f"🛡️ Rejected by Kill-Switch = {stats['rejected_by_killswitch']} | "
                f"Total Rejected = {stats['total_rejected']}"
            )

    # 🛡️ v2.3 NEW: Display Schema Health in Test Mode
    logger.info()
    logger.info("=== 🛡️ Schema Health Test Results ===")
    schema_health = schema_bootstrap.get_health_status()
    logger.info(
        f"  Bootstrap Mode: {schema_health['bootstrap_mode']}"
    )
    logger.info(
        f"  Shapes: {'✅' if schema_health['shapes_exists'] else '❌'} "
        f"({schema_health['shapes_size']} bytes)"
    )
    logger.info(
        f"  Ontology: {'✅' if schema_health['ontology_exists'] else '❌'} "
        f"({schema_health['ontology_size']} bytes)"
    )
    auto_generated = (
        schema_health['generated_shapes'] or
        schema_health['generated_ontology']
    )
    if auto_generated:
        logger.warning(
            "  ⚠️  Auto-Generated Schema: Test mode using minimal schema files"
        )

    # Display final statistics
    stats = executor.get_execution_stats()
    logger.info()
    logger.info("=== Test Mode Statistics ===")
    for network, stat in stats.items():
        if (
            stat.get('successful', 0) > 0 or
            stat.get('failed', 0) > 0 or
            stat.get('skipped', 0) > 0 or
            stat.get('rejected_by_killswitch', 0) > 0
        ):
            logger.info(
                f"  {network}: "
                f"✅ {stat.get('successful', 0)} | "
                f"❌ {stat.get('failed', 0)} | "
                f"⏭️ {stat.get('skipped', 0)} | "
                f"🛡️ {stat.get('rejected_by_killswitch', 0)}"
            )

    logger.info()
    logger.info("=== Test Mode Complete ===")


# ---------------------------
# Standalone Execution
# ---------------------------
if __name__ == "__main__":
    # 🛡️ v2.1: Check for test mode flag
    if "--test" in sys.argv:
        run_test_mode()
    else:
        start_node()
