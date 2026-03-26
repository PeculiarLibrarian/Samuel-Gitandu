#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🏛️ PADI EXECUTOR v5.3 — Prometheus Metrics Exporter
====================================================

Purpose:
- Expose executor metrics in Prometheus format
- Enable monitoring and alerting
- Provide insights into system health and performance

Version: 5.3
Node: Nairobi-01
Timestamp: 2026-03-26 [EAT]
"""

import time
import logging
from datetime import datetime
from typing import Dict, Optional

from prometheus_client import (
    start_http_server,
    Gauge,
    Counter,
    Histogram,
    Summary
)
from web3 import Web3

# Import Executor
from executor import Executor

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("METRICS_EXPORTER")

# =====================================================
# Prometheus Metrics Definitions
# =====================================================

# Transaction Metrics
padi_exec_transactions_total = Counter(
    'padi_exec_transactions_total',
    'Total number of transactions attempted',
    ['network', 'status']
)

padi_exec_transactions_successful_total = Counter(
    'padi_exec_transactions_successful_total',
    'Total number of successful transactions',
    ['network']
)

padi_exec_transactions_failed_total = Counter(
    'padi_exec_transactions_failed_total',
    'Total number of failed transactions',
    ['network', 'reason']
)

padi_exec_transactions_skipped_total = Counter(
    'padi_exec_transactions_skipped_total',
    'Total number of skipped transactions',
    ['network']
)

padi_exec_kill_switch_rejected_total = Counter(
    'padi_exec_kill_switch_rejected_total',
    'Total number of transactions rejected by kill-switch',
    ['network', 'reason']
)

# Network Metrics
padi_exec_network_connected = Gauge(
    'padi_exec_network_connected',
    'Network connection status (1 = connected, 0 = disconnected)',
    ['network']
)

padi_exec_network_block_number = Gauge(
    'padi_exec_network_block_number',
    'Current block number for each network',
    ['network']
)

padi_exec_network_base_fee_gwei = Gauge(
    'padi_exec_network_base_fee_gwei',
    'Current base fee in Gwei for each network',
    ['network']
)

padi_exec_rpc_errors_total = Counter(
    'padi_exec_rpc_errors_total',
    'Total number of RPC connection errors',
    ['network', 'error_type']
)

# Circuit Breaker Metrics
padi_exec_circuit_breaker_state = Gauge(
    'padi_exec_circuit_breaker_state',
    'Circuit breaker state (closed=0, open=1, half-open=2)',
    ['network']
)

padi_exec_circuit_breaker_failure_count = Gauge(
    'padi_exec_circuit_breaker_failure_count',
    'Number of failures recorded by circuit breaker',
    ['network']
)

padi_exec_circuit_breaker_success_count = Gauge(
    'padi_exec_circuit_breaker_success_count',
    'Number of successes recorded by circuit breaker',
    ['network']
)

# Gas Price Metrics
padi_exec_gas_price_gwei = Gauge(
    'padi_exec_gas_price_gwei',
    'Current gas price in Gwei',
    ['network', 'type']
)

padi_exec_gas_spike_detected_total = Counter(
    'padi_exec_gas_spike_detected_total',
    'Total number of gas spike detections',
    ['network']
)

# Nonce Metrics
padi_exec_nonce_desync_total = Counter(
    'padi_exec_nonce_desync_total',
    'Total number of nonce desynchronization events',
    ['network']
)

padi_exec_nonce_value = Gauge(
    'padi_exec_nonce_value',
    'Current nonce value for each network',
    ['network', 'wallet']
)

# Health Metrics
padi_exec_health_status = Gauge(
    'padi_exec_health_status',
    'Overall health status (healthy=0, warning=1, degraded=2, critical=3)',
)

padi_exec_wallet_balance_eth = Gauge(
    'padi_exec_wallet_balance_eth',
    'Wallet balance in ETH',
    ['network']
)

# Receipt Tracker Metrics
padi_exec_pending_transactions = Gauge(
    'padi_exec_pending_transactions',
    'Number of pending transactions',
    ['network']
)

padi_exec_receipt_monitor_lag_seconds = Gauge(
    'padi_exec_receipt_monitor_lag_seconds',
    'Receipt monitoring lag in seconds',
    ['network']
)

padi_exec_receipt_total = Counter(
    'padi_exec_receipt_total',
    'Total number of receipts tracked',
    ['network', 'status']
)

# Audit Metrics
padi_exec_audit_log_size_bytes = Gauge(
    'padi_exec_audit_log_size_bytes',
    'Size of transaction log in bytes',
)

padi_exec_rdf_snapshots_count = Gauge(
    'padi_exec_rdf_snapshots_count',
    'Total number of RDF snapshots stored',
)

padi_exec_rdf_deduplicated_total = Counter(
    'padi_exec_rdf_deduplicated_total',
    'Total number of RDF snapshots deduplicated',
)

# Performance Metrics
padi_exec_transaction_duration_seconds = Histogram(
    'padi_exec_transaction_duration_seconds',
    'Transaction execution duration in seconds',
    ['network', 'action_type'],
    buckets=[0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0, 60.0, 120.0]
)

padi_exec_gas_limit_used = Summary(
    'padi_exec_gas_limit_used',
    'Gas limit used per transaction',
    ['network', 'action_type']
)

# System Metrics
padi_exec_uptime_seconds = Gauge(
    'padi_exec_uptime_seconds',
    'Executor uptime in seconds',
)

padi_exec_version_info = Gauge(
    'padi_exec_version_info',
    'Executor version information',
    ['version', 'node']
)

# =====================================================
# Metrics Updater
# =====================================================

class MetricsUpdateManager:
    """
    Updates Prometheus metrics from Executor state.
    
    Runs continuously and updates metrics at configurable intervals.
    """
    
    def __init__(self, executor: Executor, update_interval: int = 15):
        self.executor = executor
        self.update_interval = update_interval
        self.start_time = datetime.now()
        
        # Static metrics
        padi_exec_version_info.labels(
            version="5.3",
            node=executor.node_id
        ).set(1)
    
    def update_transaction_metrics(self):
        """Update transaction-related metrics."""
        stats = self.executor.get_execution_stats()
        
        for network, net_stats in stats.items():
            padi_exec_transactions_total.labels(
                network=network,
                status="successful"
            )._value._value = net_stats.get("successful", 0)
            
            padi_exec_transactions_total.labels(
                network=network,
                status="failed"
            )._value._value = net_stats.get("failed", 0)
            
            padi_exec_transactions_total.labels(
                network=network,
                status="skipped"
            )._value._value = net_stats.get("skipped", 0)
            
            padi_exec_kill_switch_rejected_total.labels(
                network=network,
                reason="kill_switch"
            )._value._value = net_stats.get("rejected_by_killswitch", 0)
    
    def update_network_metrics(self):
        """Update network-related metrics."""
        status = self.executor.get_network_status()
        
        for network, info in status.items():
            connected = 1 if info.get("connected", False) else 0
            padi_exec_network_connected.labels(network=network).set(connected)
            
            if connected:
                block_number = info.get("block_number", 0)
                if block_number:
                    padi_exec_network_block_number.labels(
                        network=network
                    ).set(block_number)
                
                base_fee = info.get("base_fee_gwei", 0)
                if base_fee:
                    padi_exec_network_base_fee_gwei.labels(
                        network=network
                    ).set(base_fee)
    
    def update_circuit_breaker_metrics(self):
        """Update circuit breaker metrics."""
        for network, breaker in self.executor.circuit_breakers.items():
            breaker_status = breaker.get_status()
            state = breaker_status.get("state", "closed")
            
            # Map state to numeric value
            state_map = {"closed": 0, "open": 1, "half-open": 2}
            state_value = state_map.get(state.lower(), 0)
            
            padi_exec_circuit_breaker_state.labels(
                network=network
            ).set(state_value)
            
            padi_exec_circuit_breaker_failure_count.labels(
                network=network
            ).set(breaker_status.get("failure_count", 0))
            
            padi_exec_circuit_breaker_success_count.labels(
                network=network
            ).set(breaker_status.get("success_count", 0))
    
    def update_gas_metrics(self):
        """Update gas-related metrics."""
        for network, cache in self.executor.gas_caches.items():
            avg_gas = cache.get_average()
            latest_gas = cache.get_latest()
            
            padi_exec_gas_price_gwei.labels(
                network=network,
                type="avg"
            ).set(float(Web3.from_wei(avg_gas, 'gwei')))
            
            padi_exec_gas_price_gwei.labels(
                network=network,
                type="latest"
            ).set(float(Web3.from_wei(latest_gas, 'gwei')))
    
    def update_health_metrics(self):
        """Update health-related metrics."""
        health = self.executor.health_check()
        
        # Map health status to numeric value
        status_map = {
            "healthy": 0,
            "warning": 1,
            "degraded": 2,
            "critical": 3
        }
        status_value = status_map.get(health.get("status", "healthy"), 0)
        padi_exec_health_status.set(status_value)
        
        # Update wallet balance (if connected)
        for network, w3 in self.executor.w3_connections.items():
            try:
                balance = w3.eth.get_balance(self.executor.address)
                padi_exec_wallet_balance_eth.labels(
                    network=network
                ).set(float(Web3.from_wei(balance, 'ether')))
            except Exception as e:
                logger.error(f"Failed to get balance for {network}: {e}")
    
    def update_receipt_tracker_metrics(self):
        """Update receipt tracker metrics."""
        tracker_stats = self.executor.receipt_tracker.get_stats()
        
        pending_txs = tracker_stats.get("pending_by_network", {})
        for network, count in pending_txs.items():
            padi_exec_pending_transactions.labels(
                network=network
            ).set(count)
        
        padi_exec_receipt_total.labels(
            network="all",
            status="confirmed"
        )._value._value = tracker_stats.get("total_confirmed", 0)
        
        padi_exec_receipt_total.labels(
            network="all",
            status="failed"
        )._value._value = tracker_stats.get("total_failed", 0)
    
    def update_audit_metrics(self):
        """Update audit-related metrics."""
        log_size = len(self.executor.transaction_log)
        padi_exec_audit_log_size_bytes.set(log_size)
        
        rdf_stats = self.executor.rdf_manager.get_stats()
        padi_exec_rdf_snapshots_count.set(
            rdf_stats.get("currently_stored", 0)
        )
    
    def update_uptime_metric(self):
        """Update uptime metric."""
        uptime = (datetime.now() - self.start_time).total_seconds()
        padi_exec_uptime_seconds.set(uptime)
    
    def update_all_metrics(self):
        """Update all metrics from executor state."""
        try:
            self.update_transaction_metrics()
            self.update_network_metrics()
            self.update_circuit_breaker_metrics()
            self.update_gas_metrics()
            self.update_health_metrics()
            self.update_receipt_tracker_metrics()
            self.update_audit_metrics()
            self.update_uptime_metric()
            
        except Exception as e:
            logger.error(f"Failed to update metrics: {e}")
    
    def run(self):
        """
        Run the metrics updater loop.
        
        Continuously updates metrics at configured interval.
        """
        logger.info("📊 Metrics updater started")
        logger.info(f"   Update interval: {self.update_interval} seconds")
        
        while True:
            try:
                self.update_all_metrics()
                time.sleep(self.update_interval)
            except KeyboardInterrupt:
                logger.info("📊 Metrics updater stopped by user")
                break
            except Exception as e:
                logger.error(f"Error in metrics update loop: {e}")
                time.sleep(5)


# =====================================================
# Main Entry Point
# =====================================================

def main():
    """
    Main entry point for metrics exporter.
    
    Starts Prometheus HTTP server and metrics updater.
    """
    logger.info("🏛️ PADI EXECUTOR v5.3 — METRICS EXPORTER")
    logger.info(f"   Node: Nairobi-01")
    logger.info(f"   Timestamp: {datetime.now().isoformat()}")
    logger.info("")
    
    # Initialize executor (simulation mode for metrics extraction)
    logger.info("Initializing executor...")
    executor = Executor(simulation_mode=True)
    logger.info("✅ Executor initialized")
    
    # Start Prometheus server
    port = 8000
    logger.info(f"📊 Starting Prometheus HTTP server on port {port}...")
    start_http_server(port)
    logger.info(f"✅ Prometheus server started: http://localhost:{port}/metrics")
    
    # Initialize and run metrics updater
    updater = MetricsUpdateManager(executor, update_interval=15)
    updater.run()


if __name__ == "__main__":
    main()
