from rdflib import Graph, Namespace, Literal, RDF, XSD
from pyshacl import validate

# 1. Define Namespace
EX = Namespace("http://padi.u/schema#")

def audit_lead(name, confidence, sources):
    g = Graph()
    node = EX[name]
    g.add((node, RDF.type, EX.ScoutedOpportunity))
    g.add((node, EX.hasConfidence, Literal(confidence, datatype=XSD.decimal)))
    for s in sources:
        g.add((node, EX.hasSource, Literal(s)))

    # Load Firewall
    conforms, _, results_text = validate(g, shacl_graph="schema/shapes.ttl", inference='rdfs')

    status = "✅ DETERMINISTIC" if conforms else "❌ PROBABILISTIC (BLOCKED)"
    print(f"Lead: {name} | Status: {status}")
    if not conforms:
        print(f"Reason: Cardinality or Confidence mismatch.\n")

# --- EXECUTION ---
print("--- PADI BUREAU: NAIROBI NODE-01 ---")
audit_lead("Simulation_Lead", 0.8, ["Source_1"]) # Should fail
audit_lead("Truth_Lead", 1.0, ["Source_1", "Source_2", "Source_3"]) # Should pass
