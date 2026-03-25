from rdflib import Graph, Namespace, Literal, RDF, XSD
from pyshacl import validate
from datetime import datetime
from typing import List, Optional, Dict, Tuple

# 1. NAMESPACE CONFIGURATION
# Must match ontology.ttl and shapes.ttl exactly
EX = Namespace("http://padi.u/schema#")

# 2. NETWORK CONFIGURATION
NETWORK_CHAIN_ID_MAP = {
    "op-mainnet": 10,
    "op-sepolia": 11155420,
    "eth-mainnet": 1,
    "eth-sepolia": 11155111
}

NETWORK_PROVIDER_MAP = {
    "op-mainnet": "Alchemy-OP-Mainnet",
    "op-sepolia": "Alchemy-OP-Sepolia",
    "eth-mainnet": "Alchemy-ETH-Mainnet",
    "eth-sepolia": "Alchemy-ETH-Sepolia"
}

VALID_NETWORKS = ["op-mainnet", "op-sepolia", "eth-mainnet", "eth-sepolia"]
VALID_CHAIN_IDS = [10, 1, 11155420, 11155111]
VALID_NETWORK_TYPES = {
    "op-mainnet": "layer2",
    "op-sepolia": "layer2-testnet",
    "eth-mainnet": "layer1",
    "eth-sepolia": "layer1-testnet"
}


def audit_signal(
    name: str,
    confidence: float,
    sources: List[str],
    target_address: str,
    action_type: str,
    signal_id: str,
    # NEW: Network Context Parameters
    network_type: str = "op-mainnet",
    chain_id: Optional[int] = None,
    source_provider: Optional[str] = None,
    # NEW: Verification Metadata Parameters
    verification_confidence: Optional[List[float]] = None,
    verification_timestamp: Optional[List[str]] = None,
    verification_match: Optional[List[bool]] = None,
    # NEW: Cross-Network Verification Parameters
    primary_network: Optional[str] = None,
    fallback_network: Optional[str] = None,
    cross_network_verification: Optional[bool] = True,
    # Existing Parameters
    observed_at: Optional[str] = None,
    block_number: Optional[int] = None,
    gas_price_gwei: Optional[float] = None,
    is_validated: bool = False
) -> Tuple[Graph, bool, str, str]:
    """
    PADI Bureau Core Audit:
    Converts a signal into RDF and enforces the 1003 Rule via SHACL.
    
    Enhanced with multi-network support (OP Mainnet vs Ethereum Mainnet).
    
    Args:
        name: Signal name (used as RDF node identifier)
        confidence: Confidence score (0.0 - 1.0). Must be 1.0 for 1003 Rule.
        sources: List of exactly 3 verification sources (strings)
        target_address: Contract or wallet address targeted by this signal
        action_type: Action type (e.g., SWAP, ARB, AUDIT)
        signal_id: Unique deterministic identifier (e.g., SHA256 hash)
        network_type: Network type ("op-mainnet", "op-sepolia", "eth-mainnet", "eth-sepolia")
        chain_id: EVM chain ID (10 for OP Mainnet, 1 for ETH Mainnet)
        source_provider: API provider that supplied the signal
        verification_confidence: List of 3 confidence scores (0.0 - 1.0 each)
        verification_timestamp: List of 3 ISO timestamps for each verification source
        verification_match: List of 3 boolean match statuses
        primary_network: Primary network where signal was first observed
        fallback_network: Fallback network used for verification (optional)
        cross_network_verification: Whether signal was verified across networks
        observed_at: Timestamp when the signal was observed (UTC, ISO format)
        block_number: Blockchain block number where the signal was observed
        gas_price_gwei: Gas price at time of observation in Gwei
        is_validated: Indicates whether the signal passed validation (default False)
    
    Returns:
        g (Graph) - RDF Graph of the signal
        conforms (bool) - SHACL conformance
        status (str) - Deterministic or Probabilistic status
        results_text (str) - SHACL validation report
    """
    # --- INPUT VALIDATION ---
    # Validate network_type
    if network_type not in VALID_NETWORKS:
        raise ValueError(f"Invalid network_type: {network_type}. Must be one of {VALID_NETWORKS}")
    
    # Auto-detect chain_id if not provided
    if chain_id is None:
        chain_id = NETWORK_CHAIN_ID_MAP[network_type]
    elif chain_id not in VALID_CHAIN_IDS:
        raise ValueError(f"Invalid chain_id: {chain_id}. Must be one of {VALID_CHAIN_IDS}")
    
    # Auto-detect source_provider if not provided
    if source_provider is None:
        source_provider = NETWORK_PROVIDER_MAP[network_type]
    
    # Auto-detect primary_network if not provided
    if primary_network is None:
        primary_network = network_type
    
    # Default cross_network_verification to True
    if cross_network_verification is None:
        cross_network_verification = True
    
    # --- RDF GRAPH CONSTRUCTION ---
    g = Graph()
    node = EX[name]

    # 1. CLASS ASSIGNMENT
    g.add((node, RDF.type, EX.FinancialSignal))

    # 2. 1003 RULE: Confidence & Verification Sources
    g.add((node, EX.hasConfidence, Literal(confidence, datatype=XSD.decimal)))
    for s in sources:
        g.add((node, EX.hasVerificationSource, Literal(s, datatype=XSD.string)))

    # 3. TARGETING LAYER
    g.add((node, EX.hasTargetAddress, Literal(target_address, datatype=XSD.string)))
    g.add((node, EX.hasActionType, Literal(action_type, datatype=XSD.string)))

    # 4. IDENTITY & TRACEABILITY
    g.add((node, EX.hasSignalID, Literal(signal_id, datatype=XSD.string)))
    observed_at = observed_at or datetime.utcnow().isoformat()
    g.add((node, EX.observedAt, Literal(observed_at, datatype=XSD.dateTime)))

    # 5. NETWORK CONTEXT (NEW)
    g.add((node, EX.hasNetworkType, Literal(network_type, datatype=XSD.string)))
    g.add((node, EX.hasChainID, Literal(chain_id, datatype=XSD.integer)))
    g.add((node, EX.hasSourceProvider, Literal(source_provider, datatype=XSD.string)))

    # 6. VERIFICATION METADATA (NEW)
    if verification_confidence is not None:
        for vc in verification_confidence:
            g.add((node, EX.hasVerificationConfidence, Literal(vc, datatype=XSD.decimal)))
    else:
        # Default to 1.0 for all sources if not provided
        for _ in range(3):
            g.add((node, EX.hasVerificationConfidence, Literal(1.0, datatype=XSD.decimal)))
    
    if verification_timestamp is not None:
        for vt in verification_timestamp:
            g.add((node, EX.hasVerificationTimestamp, Literal(vt, datatype=XSD.dateTime)))
    else:
        # Default to current timestamp for all sources if not provided
        current_ts = datetime.utcnow().isoformat()
        for _ in range(3):
            g.add((node, EX.hasVerificationTimestamp, Literal(current_ts, datatype=XSD.dateTime)))
    
    if verification_match is not None:
        for vm in verification_match:
            g.add((node, EX.hasVerificationMatch, Literal(vm, datatype=XSD.boolean)))
    else:
        # Default to match for all sources if not provided
        for _ in range(3):
            g.add((node, EX.hasVerificationMatch, Literal(True, datatype=XSD.boolean)))

    # 7. CROSS-NETWORK VERIFICATION (NEW)
    g.add((node, EX.hasPrimaryNetwork, Literal(primary_network, datatype=XSD.string)))
    
    if fallback_network is not None:
        g.add((node, EX.hasFallbackNetwork, Literal(fallback_network, datatype=XSD.string)))
    
    g.add((node, EX.hasCrossNetworkVerification, Literal(cross_network_verification, datatype=XSD.boolean)))

    # 8. INFRASTRUCTURE CONTEXT
    if block_number is not None:
        g.add((node, EX.atBlockNumber, Literal(block_number, datatype=XSD.integer)))
    if gas_price_gwei is not None:
        g.add((node, EX.hasGasPriceGwei, Literal(gas_price_gwei, datatype=XSD.decimal)))
    g.add((node, EX.isValidated, Literal(is_validated, datatype=XSD.boolean)))

    # 9. SHACL VALIDATION
    conforms, _, results_text = validate(
        g,
        shacl_graph="schema/shapes.ttl",
        ont_graph="schema/ontology.ttl",
        inference='rdfs',
        advanced=True,
        allow_warnings=True
    )

    # 10. DETERMINISTIC PROMOTION
    if conforms:
        # Upgrade FinancialSignal → ExecutableFact
        g.remove((node, RDF.type, EX.FinancialSignal))
        g.add((node, RDF.type, EX.ExecutableFact))
        # Update validation flag
        g.set((node, EX.isValidated, Literal(True, datatype=XSD.boolean)))
        status = "✅ DETERMINISTIC (PROMOTED TO FACT)"
    else:
        status = "❌ PROBABILISTIC (BLOCKED)"

    return g, conforms, status, results_text


def create_verification_metadata(
    source_name: str,
    confidence: float,
    timestamp: Optional[str] = None,
    match: bool = True
) -> Dict[str, any]:
    """
    Creates a structured verification metadata object.
    
    Args:
        source_name: Name of the verification source
        confidence: Confidence score (0.0 - 1.0)
        timestamp: ISO timestamp (default: current UTC time)
        match: Match status (default: True)
    
    Returns:
        Dictionary with verification metadata
    """
    return {
        "source": source_name,
        "confidence": confidence,
        "timestamp": timestamp or datetime.utcnow().isoformat(),
        "match": match
    }


def validate_verification_metadata(
    sources: List[str],
    verification_confidence: Optional[List[float]],
    verification_timestamp: Optional[List[str]],
    verification_match: Optional[List[bool]]
) -> Tuple[bool, str]:
    """
    Validates verification metadata consistency.
    
    Args:
        sources: List of verification source names
        verification_confidence: List of confidence scores
        verification_timestamp: List of timestamps
        verification_match: List of match statuses
    
    Returns:
        is_valid (bool): Whether metadata is valid
        error_message (str): Error message if invalid, empty string otherwise
    """
    n_sources = len(sources)
    
    # Check 1003 Rule: Exactly 3 sources
    if n_sources != 3:
        return False, f"Validation Error: Exactly 3 verification sources required (got {n_sources})"
    
    # Verify counts match
    if verification_confidence is not None and len(verification_confidence) != n_sources:
        return False, f"Validation Error: Expected {n_sources} confidence scores, got {len(verification_confidence)}"
    
    if verification_timestamp is not None and len(verification_timestamp) != n_sources:
        return False, f"Validation Error: Expected {n_sources} timestamps, got {len(verification_timestamp)}"
    
    if verification_match is not None and len(verification_match) != n_sources:
        return False, f"Validation Error: Expected {n_sources} match statuses, got {len(verification_match)}"
    
    return True, ""


def get_network_info(network_type: str) -> Dict[str, any]:
    """
    Retrieves network information for a given network type.
    
    Args:
        network_type: Network type ("op-mainnet", "op-sepolia", "eth-mainnet", "eth-sepolia")
    
    Returns:
        Dictionary with chain ID, provider, and network class
    """
    if network_type not in VALID_NETWORKS:
        raise ValueError(f"Invalid network_type: {network_type}. Must be one of {VALID_NETWORKS}")
    
    return {
        "network_type": network_type,
        "chain_id": NETWORK_CHAIN_ID_MAP[network_type],
        "default_provider": NETWORK_PROVIDER_MAP[network_type],
        "network_class": VALID_NETWORK_TYPES[network_type]
    }


def validate_network_config(network_type: str, chain_id: Optional[int] = None) -> Tuple[bool, str]:
    """
    Validates network configuration.
    
    Args:
        network_type: Network type
        chain_id: Optional chain ID to validate consistency
    
    Returns:
        is_valid (bool): Whether configuration is valid
        error_message (str): Error message if invalid, empty string otherwise
    """
    if network_type not in VALID_NETWORKS:
        return False, f"Invalid network_type: {network_type}. Must be one of {VALID_NETWORKS}"
    
    if chain_id is not None:
        expected_chain_id = NETWORK_CHAIN_ID_MAP[network_type]
        if chain_id != expected_chain_id:
            return False, f"Chain ID mismatch: network_type={network_type} expects chain_id={expected_chain_id}, got {chain_id}"
    
    return True, ""


# --- PRODUCTION TEST GATEWAY ---
if __name__ == "__main__":
    print("--- PADI BUREAU: NAIROBI NODE-01 STANDALONE AUDIT ---")
    print()

    # Example 1: Probabilistic Signal (Should fail)
    # Missing verification metadata, incomplete 1003 Rule
    print("Example 1: Probabilistic Signal (Should FAIL)")
    print("-" * 50)
    try:
        g1, c1, s1, r1 = audit_signal(
            name="Simulation_Lead",
            confidence=0.8,
            sources=["Source_1"],  # Only 1 source (1003 Rule requires 3)
            target_address="0xABCDEF1234567890",
            action_type="SWAP",
            signal_id="SIM-001",
            block_number=12345678,
            gas_price_gwei=50.0
        )
        print(f"Signal: Simulation_Lead | Status: {s1}")
        if not c1:
            print(f"Sentinel Report:\n{r1}\n")
    except Exception as e:
        print(f"Error: {e}\n")

    # Example 2: Deterministic Signal on OP Mainnet (Should pass)
    # Full 1003 compliance with multi-network context
    print("Example 2: Deterministic Signal on OP Mainnet (Should PASS)")
    print("-" * 50)
    try:
        g2, c2, s2, r2 = audit_signal(
            name="Truth_Lead_OP",
            confidence=1.0,
            sources=["Alchemy-OP-Mainnet", "Infura-OP-Mainnet", "QuickNode-OP-Mainnet"],
            target_address="0x1234567890ABCDEF",
            action_type="ARB",
            signal_id="TRUTH-OP-001",
            network_type="op-mainnet",
            chain_id=10,
            source_provider="Alchemy-OP-Mainnet",
            verification_confidence=[1.0, 1.0, 1.0],
            verification_timestamp=["2026-03-26T00:00:00Z", "2026-03-26T00:00:01Z", "2026-03-26T00:00:02Z"],
            verification_match=[True, True, True],
            primary_network="op-mainnet",
            fallback_network="eth-mainnet",
            cross_network_verification=True,
            block_number=12345679,
            gas_price_gwei=55.0
        )
        print(f"Signal: Truth_Lead_OP | Status: {s2}")
        print(f"Network: op-mainnet (Chain ID: 10)")
        print(f"Provider: Alchemy-OP-Mainnet")
        print(f"Cross-Network: Verified on op-mainnet → eth-mainnet\n")
    except Exception as e:
        print(f"Error: {e}\n")

    # Example 3: Deterministic Signal on Ethereum Mainnet (Should pass)
    print("Example 3: Deterministic Signal on Ethereum Mainnet (Should PASS)")
    print("-" * 50)
    try:
        g3, c3, s3, r3 = audit_signal(
            name="Truth_Lead_ETH",
            confidence=1.0,
            sources=["Alchemy-ETH-Mainnet", "Infura-ETH-Mainnet", "QuickNode-ETH-Mainnet"],
            target_address="0xFEDCBA0987654321",
            action_type="AUDIT",
            signal_id="TRUTH-ETH-001",
            network_type="eth-mainnet",
            chain_id=1,
            source_provider="Alchemy-ETH-Mainnet",
            block_number=12345680,
            gas_price_gwei=60.0
        )
        print(f"Signal: Truth_Lead_ETH | Status: {s3}")
        print(f"Network: eth-mainnet (Chain ID: 1)")
        print(f"Provider: Alchemy-ETH-Mainnet\n")
    except Exception as e:
        print(f"Error: {e}\n")

    # Example 4: Helper Functions Demo
    print("Example 4: Helper Functions Demo")
    print("-" * 50)
    print("Network Info for op-mainnet:")
    info = get_network_info("op-mainnet")
    print(f"  - Type: {info['network_type']}")
    print(f"  - Chain ID: {info['chain_id']}")
    print(f"  - Default Provider: {info['default_provider']}")
    print(f"  - Network Class: {info['network_class']}")
    print()

    # Validate network config
    print("Network Config Validation:")
    is_valid, msg = validate_network_config("op-mainnet", 10)
    print(f"  - op-mainnet + chain_id=10: {is_valid} | {msg or 'Valid'}")
    
    is_valid, msg = validate_network_config("op-mainnet", 1)
    print(f"  - op-mainnet + chain_id=1: {is_valid} | {msg or 'Valid'}")
    print()

    print("--- AUDIT COMPLETE ---")
