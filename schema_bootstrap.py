#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from pathlib import Path
from typing import Tuple, List, Dict, Any
import logging
from datetime import datetime

logger = logging.getLogger("PADI-SCHEMA-BOOTSTRAP")

# =====================================================
# 🛡️ SCHEMA BOOTSTRAP SYSTEM — NAIROBI NODE-01
# Pre-flight validation and auto-generation of schema files
# =====================================================

class SchemaBootstrap:
    """
    Schema Bootstrap System for PADI Bureau.
    
    Validates and generates schema files at startup to prevent
    runtime failures when schema/ folder is missing or incomplete.
    
    Features:
    - Pre-flight validation of schema files
    - Auto-generation of minimal schema files if missing
    - Graceful degradation when schema is incomplete
    - Schema health monitoring
    """
    
    # Minimal SHAML shapes for 1003 Rule enforcement
    MINIMAL_SHAPES_TTL = """
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://padi.u/schema#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

:ExecutableShape a sh:NodeShape ;
    sh:targetClass ex:FinancialSignal ;
    sh:property [
        sh:path ex:hasConfidence ;
        sh:datatype xsd:decimal ;
        sh:minInclusive 1.0 ;
        sh:maxInclusive 1.0 ;
    ] ;
    sh:property [
        sh:path ex:hasVerificationSource ;
        sh:minCount 3 ;
        sh:maxCount 3 ;
    ] .
""".strip()
    
    # Minimal ontology for ExecutableFact
    MINIMAL_ONTOLOGY_TTL = """
@prefix ex: <http://padi.u/schema#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .

ex:FinancialSignal a rdf:Class .
ex:ExecutableFact a rdf:Class .
ex:hasConfidence a rdf:Property .
ex:hasVerificationSource a rdf:Property .
ex:hasTargetAddress a rdf:Property .
ex:hasActionType a rdf:Property .
ex:hasSignalID a rdf:Property .
ex:observedAt a rdf:Property .
ex:atBlockNumber a rdf:Property .
ex:hasGasPriceGwei a rdf:Property .
ex:isValidated a rdf:Property .
""".strip()
    
    def __init__(
        self,
        schema_dir: str = "schema",
        allow_auto_generate: bool = True,
        require_all_files: bool = False
    ):
        """
        Initialize Schema Bootstrap.
        
        Args:
            schema_dir: Directory containing schema files
            allow_auto_generate: Allow auto-generation of missing schema files
            require_all_files: Require all schema files at startup (fail fast)
        """
        self.schema_dir = Path(schema_dir)
        self.allow_auto_generate = allow_auto_generate
        self.require_all_files = require_all_files
        
        self.shapes_path = self.schema_dir / "shapes.ttl"
        self.ontology_path = self.schema_dir / "ontology.ttl"
        
        self.bootstrap_status = {
            "shapes_exists": False,
            "ontology_exists": False,
            "generated_shapes": False,
            "generated_ontology": False,
            "bootstrap_mode": None,
            "warnings": [],
            "errors": []
        }
    
    def validate_schema_files(self) -> Tuple[bool, List[str], List[str]]:
        """
        Validate that schema files exist and are readable.
        
        Returns:
            is_valid (bool): True if all required schema files exist
            warnings (list): List of warning messages
            errors (list): List of error messages
        """
        warnings = []
        errors = []
        
        # Check schema directory exists
        if not self.schema_dir.exists():
            if self.allow_auto_generate:
                warnings.append(
                    f"Schema directory does not exist: {self.schema_dir.absolute()}"
                )
                # Create directory
                self.schema_dir.mkdir(parents=True, exist_ok=True)
                warnings.append(f"Created schema directory: {self.schema_dir.absolute()}")
            else:
                errors.append(
                    f"Schema directory does not exist: {self.schema_dir.absolute()}"
                )
                self.bootstrap_status["errors"].append(
                    "Schema directory does not exist"
                )
                return False, warnings, errors
        
        # Check shapes.ttl exists
        if not self.shapes_path.exists():
            if self.allow_auto_generate:
                warnings.append(
                    f"SHAML shapes file missing: {self.shapes_path.absolute()}"
                )
            else:
                errors.append(
                    f"SHAML shapes file missing: {self.shapes_path.absolute()}"
                )
                self.bootstrap_status["errors"].append("SHAML shapes file missing")
        else:
            if not self.shapes_path.is_file():
                errors.append(
                    f"SHAML shapes path is not a file: {self.shapes_path.absolute()}"
                )
                self.bootstrap_status["errors"].append(
                    "SHAML shapes path is not a file"
                )
        
        # Check ontology.ttl exists
        if not self.ontology_path.exists():
            if self.allow_auto_generate:
                warnings.append(
                    f"Ontology file missing: {self.ontology_path.absolute()}"
                )
            else:
                errors.append(
                    f"Ontology file missing: {self.ontology_path.absolute()}"
                )
                self.bootstrap_status["errors"].append("Ontology file missing")
        else:
            if not self.ontology_path.is_file():
                errors.append(
                    f"Ontology path is not a file: {self.ontology_path.absolute()}"
                )
                self.bootstrap_status["errors"].append(
                    "Ontology path is not a file"
                )
        
        # Determine bootstrap mode
        if errors:
            self.bootstrap_status["bootstrap_mode"] = "FAILED"
            return False, warnings, errors
        elif warnings:
            self.bootstrap_status["bootstrap_mode"] = "DEGRADED"
            return warnings and not errors, warnings, errors
        else:
            self.bootstrap_status["bootstrap_mode"] = "NORMAL"
            return True, warnings, errors
    
    def auto_generate_schema_files(self) -> Tuple[bool, List[str]]:
        """
        Auto-generate minimal schema files if missing.
        
        Returns:
            success (bool): True if generation succeeded
            messages (list): List of status messages
        """
        messages = []
        
        # Create schema directory if it doesn't exist
        if not self.schema_dir.exists():
            self.schema_dir.mkdir(parents=True, exist_ok=True)
            messages.append(f"Created schema directory: {self.schema_dir.absolute()}")
        
        # Generate shapes.ttl if missing
        if not self.shapes_path.exists():
            try:
                with open(self.shapes_path, 'w', encoding='utf-8') as f:
                    f.write(self.MINIMAL_SHAPES_TTL)
                messages.append(
                    f"Generated minimal SHAML shapes: {self.shapes_path.absolute()}"
                )
                self.bootstrap_status["generated_shapes"] = True
            except Exception as e:
                messages.append(
                    f"Failed to generate shapes.ttl: {e}"
                )
                return False, messages
        else:
            messages.append(
                f"SHAML shapes already exists: {self.shapes_path.absolute()}"
            )
        
        # Generate ontology.ttl if missing
        if not self.ontology_path.exists():
            try:
                with open(self.ontology_path, 'w', encoding='utf-8') as f:
                    f.write(self.MINIMAL_ONTOLOGY_TTL)
                messages.append(
                    f"Generated minimal ontology: {self.ontology_path.absolute()}"
                )
                self.bootstrap_status["generated_ontology"] = True
            except Exception as e:
                messages.append(
                    f"Failed to generate ontology.ttl: {e}"
                )
                return False, messages
        else:
            messages.append(
                f"Ontology already exists: {self.ontology_path.absolute()}"
            )
        
        return True, messages
    
    def bootstrap(self, fail_fast: bool = False) -> bool:
        """
        Bootstrap schema files at startup.
        
        Args:
            fail_fast: Fail immediately if schema files missing (no auto-generation)
        
        Returns:
            success (bool): True if bootstrap succeeded
        """
        # Step 1: Validate schema files
        self.bootstrap_status["shapes_exists"] = self.shapes_path.exists()
        self.bootstrap_status["ontology_exists"] = self.ontology_path.exists()
        
        is_valid, warnings, errors = self.validate_schema_files()
        
        # Log warnings
        for warning in warnings:
            logger.warning(warning)
        for error in errors:
            logger.error(error)
        
        self.bootstrap_status["warnings"] = warnings
        self.bootstrap_status["errors"] = errors
        
        # Step 2: Handle missing schema files
        if not is_valid:
            if fail_fast or not self.allow_auto_generate:
                logger.critical(
                    "Schema bootstrap failed. System cannot start without "
                    f"valid schema files in {self.schema_dir.absolute()}"
                )
                return False
            
            # Auto-generate schema files
            logger.warning(
                "Schema files missing. Attempting auto-generation..."
            )
            
            success, messages = self.auto_generate_schema_files()
            
            for message in messages:
                logger.info(message)
            
            if not success:
                logger.critical("Schema auto-generation failed.")
                return False
            
            # Re-validate after auto-generation
            is_valid, warnings, errors = self.validate_schema_files()
            if not is_valid:
                logger.critical("Schema validation failed after auto-generation.")
                return False
        
        # Step 3: Log bootstrap status
        bootstrap_mode = self.bootstrap_status["bootstrap_mode"]
        
        if bootstrap_mode == "NORMAL":
            logger.info(
                f"✅ Schema bootstrap successful (NORMAL mode): "
                f"Shapes: {self.shapes_path.absolute()}, "
                f"Ontology: {self.ontology_path.absolute()}"
            )
        elif bootstrap_mode == "DEGRADED":
            logger.warning(
                f"⚠️  Schema bootstrap successful (DEGRADED mode): "
                f"Auto-generated schema files. "
                f"Shapes: {self.shapes_path.absolute()}, "
                f"Ontology: {self.ontology_path.absolute()}"
            )
        else:
            logger.error(
                f"❌ Schema bootstrap failed (FAILED mode): "
                f"{self.bootstrap_status['errors']}"
            )
        
        return is_valid
    
    def get_schema_paths(self) -> Dict[str, str]:
        """
        Get the paths to schema files.
        
        Returns:
            Dictionary with 'shapes' and 'ontology' paths
        """
        return {
            "shapes": str(self.shapes_path.absolute()),
            "ontology": str(self.ontology_path.absolute())
        }
    
    def get_bootstrap_status(self) -> Dict[str, Any]:
        """
        Get the bootstrap status.
        
        Returns:
            Dictionary with bootstrap status information
        """
        return self.bootstrap_status.copy()
    
    def is_bootstrap_complete(self) -> bool:
        """
        Check if bootstrap is complete.
        
        Returns:
            True if bootstrap is complete, False otherwise
        """
        return (
            self.shapes_path.exists() and
            self.ontology_path.exists() and
            self.bootstrap_status["bootstrap_mode"] in ["NORMAL", "DEGRADED"]
        )
    
    def get_health_status(self) -> Dict[str, Any]:
        """
        Get schema health status for monitoring.
        
        Returns:
            Dictionary with health status information
        """
        return {
            "schema_dir": str(self.schema_dir.absolute()),
            "shapes_exists": self.shapes_path.exists(),
            "shapes_path": str(self.shapes_path.absolute()),
            "shapes_size": self.shapes_path.stat().st_size if self.shapes_path.exists() else 0,
            "ontology_exists": self.ontology_path.exists(),
            "ontology_path": str(self.ontology_path.absolute()),
            "ontology_size": self.ontology_path.stat().st_size if self.ontology_path.exists() else 0,
            "bootstrap_mode": self.bootstrap_status["bootstrap_mode"],
            "generated_shapes": self.bootstrap_status["generated_shapes"],
            "generated_ontology": self.bootstrap_status["generated_ontology"],
            "warnings_count": len(self.bootstrap_status["warnings"]),
            "errors_count": len(self.bootstrap_status["errors"]),
            "timestamp": datetime.now().isoformat()
        }


# =====================================================
# STANDALONE TEST
# =====================================================
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("--- PADI SCHEMA BOOTSTRAP: NAIROBI NODE-01 STANDALONE TEST ---")
    print()
    
    # Test 1: Normal bootstrap (schema files exist)
    print("Test 1: Normal Bootstrap (Schema Files Exist)")
    print("-" * 50)
    bootstrap = SchemaBootstrap(
        schema_dir="schema",
        allow_auto_generate=True,
        require_all_files=False
    )
    
    success = bootstrap.bootstrap(fail_fast=False)
    
    if success:
        print(f"✅ Bootstrap successful")
        print(f"   Shapes: {bootstrap.shapes_path}")
        print(f"   Ontology: {bootstrap.ontology_path}")
        print()
        
        # Display bootstrap status
        status = bootstrap.get_bootstrap_status()
        print("Bootstrap Status:")
        for key, value in status.items():
            print(f"  {key}: {value}")
        print()
        
        # Display health status
        health = bootstrap.get_health_status()
        print("Health Status:")
        for key, value in health.items():
            print(f"  {key}: {value}")
        print()
    else:
        print(f"❌ Bootstrap failed")
        print()
    
    # Test 2: Missing schema directory (auto-generate)
    print("Test 2: Missing Schema Directory (Auto-Generate)")
    print("-" * 50)
    bootstrap_missing = SchemaBootstrap(
        schema_dir="schema_missing",
        allow_auto_generate=True,
        require_all_files=False
    )
    
    success_missing = bootstrap_missing.bootstrap(fail_fast=False)
    
    if success_missing:
        print(f"✅ Bootstrap successful (auto-generated)")
        print(f"   Shapes: {bootstrap_missing.shapes_path}")
        print(f"   Ontology: {bootstrap_missing.ontology_path}")
        print()
        
        # Clean up generated files
        import shutil
        if bootstrap_missing.schema_dir.exists():
            shutil.rmtree(bootstrap_missing.schema_dir)
            print(f"Cleaned up: {bootstrap_missing.schema_dir}")
    else:
        print(f"❌ Bootstrap failed")
        print()
    
    print("--- TEST COMPLETE ---")
