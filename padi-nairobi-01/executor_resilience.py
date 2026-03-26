#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
🏛️ PADI EXECUTOR v5.3 — Resilience Components
==================================================

Components:
- GasPriceCache: Timestamped gas history with TTL expiration
- CircuitBreaker: Network resilience with half-open recovery

Version: 5.3
Node: Nairobi-01
Timestamp: 2026-03-26 [EAT]
"""

import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
import logging

logger = logging.getLogger("PADI-RESILIENCE")


# =====================================================
# Gas Price Cache
# =====================================================

class GasPriceCache:
    """
    Timestamped gas history to handle high-volatility spikes.
    
    Features:
    - Thread-safe operations
    - TTL-based expiration
    - Configurable history size
    - Average calculation for spike detection
    - Spike threshold checking
    
    Args:
        ttl_seconds: Time-to-live for cache entries (default: 5 minutes)
        max_history_size: Maximum number of entries to retain (default: 20)
    """
    
    def __init__(self, ttl_seconds: int = 300, max_history_size: int = 20):
        self.history: List[Tuple[datetime, int]] = []
        self.ttl = ttl_seconds
        self.max_history_size = max_history_size
        self.lock = threading.Lock()

    def add(self, price: int):
        """
        Add a gas price to the history with timestamp.
        
        Args:
            price: Gas price in wei
        """
        with self.lock:
            now = datetime.now()
            self.history.append((now, price))
            
            # Expire old entries based on TTL
            cutoff = now - timedelta(seconds=self.ttl)
            self.history = [e for e in self.history if e[0] > cutoff]
            
            # Trim to max size (if still too large)
            if len(self.history) > self.max_history_size:
                self.history = self.history[-self.max_history_size:]

    def get_average(self) -> int:
        """
        Calculate the average gas price from valid history.
        
        Returns:
            Average gas price in wei (0 if no history)
        """
        with self.lock:
            if not self.history:
                return 0
            return sum(p for _, p in self.history) // len(self.history)
    
    def get_latest(self) -> int:
        """
        Get the most recent gas price.
        
        Returns:
            Latest gas price in wei (0 if no history)
        """
        with self.lock:
            if not self.history:
                return 0
            return self.history[-1][1]
    
    def is_spike(self, current_price: int, threshold: float = 2.5) -> bool:
        """
        Check if current price exceeds threshold times average.
        
        Args:
            current_price: Current gas price in wei
            threshold: Multiplier threshold (default: 2.5x)
        
        Returns:
            True if spike detected, False otherwise
        """
        avg = self.get_average()
        return avg > 0 and current_price > (avg * threshold)
    
    def get_history_size(self) -> int:
        """Get current history size."""
        with self.lock:
            return len(self.history)
    
    def clear_history(self):
        """Clear all history entries."""
        with self.lock:
            self.history = []


# =====================================================
# Circuit Breaker
# =====================================================

class CircuitBreaker:
    """
    Circuit breaker pattern for network connections.
    
    States:
    - closed: Normal operation, requests flow through
    - open: Circuit opened due to errors, requests rejected
    - half-open: Testing if service has recovered
    
    Features:
    - Thread-safe operations
    - Configurable failure and success thresholds
    - Automatic timeout and recovery
    - Status tracking and logging
    
    Args:
        name: Circuit breaker identifier (usually network name)
        failure_threshold: Failures before opening circuit (default: 5)
        success_threshold: Successes before closing circuit (default: 3)
        timeout_seconds: Timeout before attempting recovery (default: 300s = 5min)
    """
    
    def __init__(
        self,
        name: str,
        failure_threshold: int = 5,
        success_threshold: int = 3,
        timeout_seconds: int = 300
    ):
        self.name = name
        self.state = "closed"  # closed, open, half-open
        self.failure_count = 0
        self.success_count = 0
        self.last_failure_time = None
        self.last_failure_reason = None
        self.lock = threading.Lock()
        
        # Configurable thresholds
        self.failure_threshold = failure_threshold
        self.success_threshold = success_threshold
        self.timeout_seconds = timeout_seconds
        
        # Statistics
        self.stats = {
            "total_failures": 0,
            "total_successes": 0,
            "total_opened": 0,
            "total_closed": 0
        }

    def is_open(self) -> bool:
        """
        Check if circuit is open (blocking requests).
        
        Returns:
            True if blocking requests, False otherwise
        """
        with self.lock:
            if self.state == "open":
                # Check if timeout has elapsed
                if self.last_failure_time:
                    elapsed = (datetime.now() - self.last_failure_time).total_seconds()
                    if elapsed > self.timeout_seconds:
                        self.state = "half-open"
                        logger.info(
                            f"🔌 Circuit breaker {self.name} entering HALF-OPEN state "
                            f"for recovery after {elapsed:.0f}s"
                        )
                        return False
                return True
            return False

    def record_failure(self, reason: str = ""):
        """
        Record a failure and potentially open the circuit.
        
        Args:
            reason: Description of the failure
        """
        with self.lock:
            self.failure_count += 1
            self.success_count = 0
            self.last_failure_time = datetime.now()
            self.last_failure_reason = reason[:200] if reason else "Unknown"
            self.stats["total_failures"] += 1

            if self.failure_count >= self.failure_threshold:
                if self.state != "open":
                    logger.warning(
                        f"🔌 Circuit breaker OPENED for {self.name} "
                        f"({self.failure_count} failures: {self.last_failure_reason})"
                    )
                    self.state = "open"
                    self.stats["total_opened"] += 1

    def record_success(self):
        """Record a success and potentially close the circuit."""
        with self.lock:
            self.failure_count = 0
            self.stats["total_successes"] += 1
            
            if self.state in ["open", "half-open"]:
                self.success_count += 1
                
                if self.success_count >= self.success_threshold:
                    self.state = "closed"
                    logger.info(
                        f"🔌 Circuit breaker CLOSED for {self.name} - "
                        f"Service recovered after {self.success_count} successes"
                    )
                    self.stats["total_closed"] += 1
                elif self.state == "half-open":
                    logger.info(
                        f"🔌 Circuit breaker {self.name} recovery progress: "
                        f"{self.success_count}/{self.success_threshold} successes"
                    )

    def get_status(self) -> Dict[str, Any]:
        """
        Get current circuit breaker status.
        
        Returns:
            Dictionary containing status information
        """
        with self.lock:
            return {
                "name": self.name,
                "state": self.state,
                "failure_count": self.failure_count,
                "success_count": self.success_count,
                "last_failure_time": self.last_failure_time.isoformat() if self.last_failure_time else None,
                "last_failure_reason": self.last_failure_reason,
                "thresholds": {
                    "failure": self.failure_threshold,
                    "success": self.success_threshold,
                    "timeout_seconds": self.timeout_seconds
                },
                "statistics": self.stats
            }
    
    def reset(self):
        """Reset circuit breaker to closed state."""
        with self.lock:
            self.state = "closed"
            self.failure_count = 0
            self.success_count = 0
            self.last_failure_time = None
            self.last_failure_reason = None
            logger.info(f"🔌 Circuit breaker {self.name} reset to CLOSED state")
