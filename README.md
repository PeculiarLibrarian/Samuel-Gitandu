# 🏛️ PADI BUREAU — NAIROBI NODE-01

## 📡 Overview
The **PADI (Peculiar AI Deterministic Infrastructure) Bureau** is a high-fidelity semantic engine designed for **Base L2 signal auditing**.  
It functions as a "Logic Gate," converting raw mempool and on-chain signals into RDF and enforcing the **1003 Rule** via SHACL.

The Bureau ensures that no action is taken until a signal is promoted from a **probabilistic hypothesis** to a **Deterministic Executable Fact**.

---

## ⚖️ The 1003 Rule Standard
Every signal processed by this node must satisfy the following constraints:

* **1**: **Confidence** must be exactly `1.0` (decimal, single instance enforced by SHACL).  
* **3**: **Verification Sources** must be exactly 3 independent points (`hasVerificationSource`).  
* **0 Conflict / 0 Latency**: Signal is fully target-aware (`hasTargetAddress`) and block-synced (`atBlockNumber`).

---

## 🛠️ System Architecture

| Component | File | Role |
| :--- | :--- | :--- |
| **The Law** | `schema/ontology.ttl` | Defines core classes, properties, and OWL functional constraints. |
| **The Sentinel** | `schema/shapes.ttl` | SHACL enforcement of the 1003 Rule and infrastructure context. |
| **The Brain** | `bureau_core.py` | Python engine that audits signals and promotes valid ones to `ExecutableFact`. |
| **The Environment** | `requirements.txt` | Pinned production dependencies for semantic engine, blockchain, and utilities. |

---

## 🔄 Validation Lifecycle

1. **Ingestion:** Raw data is wrapped as `ex:FinancialSignal`.  
2. **Audit:** `pyshacl` validates the RDF graph against `shape.ttl`.  
3. **Promotion:**  
   * **FAIL:** Signal is blocked as `❌ PROBABILISTIC`.  
   * **PASS:** Signal is upgraded to `ex:ExecutableFact` and marked `isValidated = True`.  
4. **Context Enrichment (optional but recommended):**  
   * `observedAt` – timestamp of observation  
   * `atBlockNumber` – blockchain block number  
   * `hasGasPriceGwei` – gas price at observation  
   * `isValidated` – boolean flag for SHACL compliance  

---

## 🚀 Installation & Setup

### 1. Environment Preparation
Ensure Python 3.10+ is installed. Use a virtual environment to maintain stable dependencies.

```bash
python -m venv padi_env
source padi_env/bin/activate  # Linux/macOS
# OR
padi_env\Scripts\activate     # Windows

pip install -r requirements.txt
