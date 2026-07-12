"""Deterministic symbol Strength finding ledger."""

from .symbol_internal_finding import SymbolInternalFindingLedgerAccess

SYMBOL_STRENGTH_FINDINGS_COL = "symbol_strength_findings"


class SymbolStrengthLedgerAccess(SymbolInternalFindingLedgerAccess):
    collection_name = SYMBOL_STRENGTH_FINDINGS_COL
    level_field = "strength"
    terminal_status = "deprecated"


__all__ = ["SYMBOL_STRENGTH_FINDINGS_COL", "SymbolStrengthLedgerAccess"]
