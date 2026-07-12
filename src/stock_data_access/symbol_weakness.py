"""Deterministic symbol Weakness finding ledger."""

from .symbol_internal_finding import SymbolInternalFindingLedgerAccess

SYMBOL_WEAKNESS_FINDINGS_COL = "symbol_weakness_findings"


class SymbolWeaknessLedgerAccess(SymbolInternalFindingLedgerAccess):
    collection_name = SYMBOL_WEAKNESS_FINDINGS_COL
    level_field = "severity"
    terminal_status = "resolved"


__all__ = ["SYMBOL_WEAKNESS_FINDINGS_COL", "SymbolWeaknessLedgerAccess"]
