import sys
import logging
from pathlib import Path
from web3 import Web3
from rdflib import RDF, Namespace, Graph
from config import Config

# =====================================================
# 🏛️ PADI EXECUTOR v3.2 — NAIROBI NODE-01
# Phase Three: Execution Layer
# Fully aligned with ontology.ttl and shapes.ttl
# =====================================================

EX = Namespace("http://padi.u/schema#")

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
    signs and broadcasts transactions to Base L2, and maintains
    a JSON-audit log with optional read-only fallback.
    """

    def __init__(self):
        # Establish Base L2 connection
        self.w3 = Web3(Web3.HTTPProvider(Config.BASE_L2_RPC_URL))
        self.node_id = Config.NODE_ID

        # Load Wallet
        self.address = Config.PADI_WALLET_ADDRESS
        self.private_key = Config.PADI_PRIVATE_KEY
        self.chain_id = int(getattr(Config, "CHAIN_ID", 8453))  # Default Base L2

        if not self.w3.is_connected():
            logger.error(f"{self.node_id}: Failed to connect to Base L2 RPC.")
            sys.exit(1)

        if not self.private_key:
            logger.warning(f"{self.node_id}: No Private Key provided. Read-only mode engaged.")

        self.audit_log = []

    # ---------------------------
    # RDF Parsing
    # ---------------------------
    def extract_facts(self, graph: Graph):
        """
        Extract ExecutableFact individuals from RDF graph.
        Returns list of dictionaries with required properties.
        """
        facts = list(graph.subjects(RDF.type, EX.ExecutableFact))
        if not facts:
            logger.info("🛡️ Safety Lock: No ExecutableFact found in graph.")
            return []

        extracted = []
        for fact in facts:
            target = graph.value(fact, EX.hasTargetAddress)
            action = graph.value(fact, EX.hasActionType)
            signal_id = graph.value(fact, EX.hasSignalID)
            confidence = graph.value(fact, EX.hasConfidence)

            if not target or not action or not signal_id:
                logger.warning(f"⚠️ Incomplete fact detected: {fact}. Skipping.")
                continue

            extracted.append({
                "target": str(target),
                "action": str(action),
                "signal_id": str(signal_id),
                "confidence": float(confidence)
            })
        return extracted

    # ---------------------------
    # Transaction Signing
    # ---------------------------
    def sign_and_send(self, target, action_type, signal_id, gas_price=None):
        """
        Sign and broadcast a transaction to Base L2.
        Zero-value triggers by default (e.g., ARB/SWAP/AUDIT).
        """
        if not self.private_key or self.private_key.startswith("Your"):
            logger.warning(f"❌ Signing skipped for {signal_id} — Read-only mode.")
            return None

        try:
            nonce = self.w3.eth.get_transaction_count(self.address)
            gas_price_final = gas_price or self.w3.eth.gas_price

            tx = {
                'nonce': nonce,
                'to': Web3.to_checksum_address(target),
                'value': self.w3.to_wei(0, 'ether'),
                'gas': 250_000,
                'gasPrice': int(gas_price_final),
                'chainId': self.chain_id
            }

            logger.info(f"🚀 Signing {action_type} for Signal {signal_id}")
            signed_tx = self.w3.eth.account.sign_transaction(tx, self.private_key)
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
            hex_hash = self.w3.to_hex(tx_hash)
            logger.info(f"✅ Dispatched: {signal_id} | TX Hash: {hex_hash}")
            return hex_hash

        except Exception as e:
            logger.error(f"❌ Transaction FAILED for {signal_id}: {e}")
            return None

    # ---------------------------
    # Batch Execution
    # ---------------------------
    def execute_batch(self, promoted_graphs, gas_price=None):
        """
        Iterate through a batch of RDF graphs containing ExecutableFacts.
        Returns list of transaction hashes.
        """
        receipts = []

        for graph in promoted_graphs:
            fact_list = self.extract_facts(graph)
            for fact in fact_list:
                tx_hash = self.sign_and_send(
                    target=fact['target'],
                    action_type=fact['action'],
                    signal_id=fact['signal_id'],
                    gas_price=gas_price
                )
                receipts.append(tx_hash)

            # Audit log snapshot
            ttl_snapshot = graph.serialize(format='turtle')
            self.audit_log.append(ttl_snapshot)

        return receipts

# ---------------------------
# Standalone Test Entry
# ---------------------------
if __name__ == "__main__":
    if Config.validate():
        logger.info(f"--- 🏛️ PADI EXECUTOR: {Config.NODE_ID} READY ---")
        executor = Executor()

        # Example: Single Promoted Fact
        from rdflib import Graph
        g = Graph()
        node = EX["Test_Fact_01"]
        g.add((node, RDF.type, EX.ExecutableFact))
        g.add((node, EX.hasTargetAddress, "0x4752ba5DBc23f44D620376279d4b37A730947593"))
        g.add((node, EX.hasActionType, "ARBITRAGE"))
        g.add((node, EX.hasSignalID, "TX-9999"))
        g.add((node, EX.hasConfidence, 1.0))

        executor.execute_batch([g])
        logger.info("✅ Standalone test complete. Audit log captured.")
