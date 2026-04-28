"""
intelligence/subsidiaries.py
Static lookup table of hyperscaler subsidiaries and known land-acquisition LLCs.
No API calls. Run once to seed output/known_subsidiaries.json and flat lookup.

Add new entries as Honor the Earth discovers them in the field.
"""

import json
from pathlib import Path

# ── MASTER TABLE ─────────────────────────────────────────────────────────────
HYPERSCALER_MAP = {

    "Amazon": {
        "parent_cik": "0001018724",
        "parent_ticker": "AMZN",
        "subsidiaries": [
            "Amazon.com Inc", "Amazon Web Services Inc",
            "Amazon Data Services Inc", "Amazon Technologies Inc",
            "Amazon Capital Services Inc", "Amazon.com Services LLC",
        ],
        "land_llcs": [
            # VADATA is the PRIMARY signal — appears on nearly all AWS land deals
            "Vadata Inc",
            "Vadata LLC",
            "Amazon Land Holdings LLC",
            "Pearl Street Capital LLC",
            "Innovation Park Holdings LLC",
            "Cumulus Data LLC",
            "Project Breeze LLC",
            "Project Cloud LLC",
            "Project Nimbus LLC",
            "Project Stratus LLC",
        ],
        "naming_patterns": [
            "VADATA",        # ← top priority watch
            "AMAZON", "AWS",
            "PEARL STREET", "CUMULUS", "INNOVATION PARK",
            "PROJECT BREEZE", "PROJECT CLOUD",
        ],
        "registered_agent_patterns": [
            "CORPORATION SERVICE COMPANY",
            "NATIONAL REGISTERED AGENTS",
        ],
        "known_dc_states": ["VA", "OR", "OH", "TX", "CA", "IN", "GA", "AZ"],
        "notes": "VADATA INC is the critical entity — flag immediately on any deed record.",
    },

    "Microsoft": {
        "parent_cik": "0000789019",
        "parent_ticker": "MSFT",
        "subsidiaries": [
            "Microsoft Corporation", "Microsoft Technology Licensing LLC",
            "Microsoft Licensing GP LLC", "LinkedIn Corporation",
            "GitHub Inc", "Nuance Communications Inc",
        ],
        "land_llcs": [
            "Bravern Land LLC",
            "One Microsoft Way LLC",
            "MSFT Land Holdings LLC",
            "Redmond Campus Holdings",
            "Global Tech Acquisitions LLC",
        ],
        "naming_patterns": [
            "MICROSOFT", "MSFT", "BRAVERN", "AZURE",
        ],
        "registered_agent_patterns": [
            "CT CORPORATION SYSTEM",
        ],
        "known_dc_states": ["WA", "VA", "TX", "AZ", "IA", "WY", "IL"],
        "notes": "Uses CT Corporation as primary agent for land LLCs.",
    },

    "Alphabet": {
        "parent_cik": "0001652044",
        "parent_ticker": "GOOGL",
        "subsidiaries": [
            "Google LLC", "Google Inc", "Alphabet Inc",
            "Google Fiber Inc", "YouTube LLC", "Waymo LLC",
            "RREEF America LLC", "Charleston East LLC", "Sidewalk Labs LLC",
        ],
        "land_llcs": [
            "RREEF America LLC",      # ← Google real estate investment vehicle
            "Charleston East LLC",    # ← known Google project name
            "Quantum Valley LLC",     # ← known Google project code name
            "Google Real Estate LLC",
            "Google LLC",             # Google sometimes acquires directly
        ],
        "naming_patterns": [
            "GOOGLE", "ALPHABET", "WAYMO",
            "CHARLESTON EAST",        # ← watch
            "QUANTUM VALLEY",         # ← watch
            "RREEF",                  # ← watch
        ],
        "registered_agent_patterns": [
            "CORPORATION SERVICE COMPANY",
            "CT CORPORATION SYSTEM",
        ],
        "known_dc_states": ["VA", "NC", "SC", "OK", "TX", "OR", "IA", "GA"],
        "notes": "Quantum Valley and Charleston East are active project code names.",
    },

    "Meta": {
        "parent_cik": "0001326801",
        "parent_ticker": "META",
        "subsidiaries": [
            "Meta Platforms Inc", "Facebook Inc", "Instagram LLC",
            "WhatsApp Inc", "Oculus VR LLC",
        ],
        "land_llcs": [
            "Graceland Acquisitions LLC",  # ← Oklahoma tribal area — confirmed
            "Papyrus Acquisitions LLC",    # ← known Meta project name
            "Cold Spring Land LLC",
            "Altoona Land Holdings LLC",
            "Meta Platforms Real Estate LLC",
            "Facebook Real Estate LLC",
            "Meta Properties LLC",
        ],
        "naming_patterns": [
            "META", "FACEBOOK", "INSTAGRAM",
            "GRACELAND",    # ← Oklahoma — active
            "PAPYRUS",      # ← active
            "COLD SPRING",
        ],
        "registered_agent_patterns": [
            "CORPORATION SERVICE COMPANY",
            "NATIONAL REGISTERED AGENTS INC",
        ],
        "known_dc_states": ["OR", "IA", "TX", "OH", "MN", "VA", "NM", "OK"],
        "notes": "Oklahoma is an ACTIVE target. Graceland + Papyrus are the LLC names to watch.",
    },

    "OpenAI_Stargate": {
        "parent_cik": None,  # private
        "parent_ticker": None,
        "subsidiaries": [
            "OpenAI Inc", "OpenAI LLC", "OpenAI OpCo LLC", "OpenAI Global LLC",
        ],
        "land_llcs": [
            # Stargate = $500B joint venture (OpenAI + Oracle + SoftBank + Microsoft)
            "Stargate LLC",           # ← top priority watch
            "Stargate Operator LLC",
            "SGP Operator LLC",       # ← top priority watch
            "Stargate Abilene LLC",
            "Stargate Milam County LLC",
        ],
        "naming_patterns": [
            "OPENAI",
            "STARGATE",    # ← highest priority new entrant
            "SGP",
        ],
        "registered_agent_patterns": [
            "CORPORATION SERVICE COMPANY",
            "UNITED AGENT GROUP",
        ],
        "known_dc_states": ["TX", "WY", "PA", "IL", "OK"],
        "notes": "$500B buildout aggressively targeting rural + tribal secondary markets.",
    },

    "Oracle": {
        "parent_cik": "0001341439",
        "parent_ticker": "ORCL",
        "subsidiaries": [
            "Oracle America Inc", "Oracle Corporation",
            "Oracle Cloud Infrastructure",
        ],
        "land_llcs": [
            "Oracle America Inc",
            "Oracle Real Estate LLC",
            "Stargate LLC",       # Stargate joint venture partner
            "SGP Operator LLC",
        ],
        "naming_patterns": ["ORACLE", "SGP", "STARGATE"],
        "registered_agent_patterns": ["CT CORPORATION SYSTEM"],
        "known_dc_states": ["TX", "TN", "UT", "OK", "VA", "AZ"],
        "notes": "Key Stargate partner — actively acquiring in TX and OK.",
    },

    "Apple": {
        "parent_cik": "0000320193",
        "parent_ticker": "AAPL",
        "subsidiaries": [
            "Apple Inc", "Apple Operations International Limited",
            "iTunes LLC", "Beats Electronics LLC", "Braeburn Capital Inc",
        ],
        "land_llcs": [
            "Apple Inc", "Braeburn Capital Inc", "Apple Real Estate LLC",
        ],
        "naming_patterns": ["APPLE", "BRAEBURN"],
        "registered_agent_patterns": ["CT CORPORATION SYSTEM"],
        "known_dc_states": ["NC", "IA", "AZ", "NV", "OR"],
        "notes": "Less aggressive on tribal lands currently but expanding.",
    },

    "Equinix": {
        "parent_cik": "0001101239",
        "parent_ticker": "EQIX",
        "subsidiaries": [
            "Equinix Inc", "Switch and Data Facilities Company LLC", "Nimbus LLC",
        ],
        "land_llcs": ["Equinix Real Estate LLC", "Nimbus Data LLC"],
        "naming_patterns": ["EQUINIX", "NIMBUS", "SWITCH AND DATA"],
        "registered_agent_patterns": ["CT CORPORATION SYSTEM"],
        "known_dc_states": ["VA", "TX", "CA", "NY", "IL"],
        "notes": "Colocation REIT — more transparent than hyperscalers.",
    },
}

# Registered agents that commonly serve hyperscaler shell companies
HYPERSCALER_REGISTERED_AGENTS = [
    "CT CORPORATION SYSTEM",
    "CORPORATION SERVICE COMPANY",
    "NATIONAL REGISTERED AGENTS",
    "THE PRENTICE-HALL CORPORATION SYSTEM",
    "UNITED AGENT GROUP",
    "COGENCY GLOBAL",
    "INCORP SERVICES",
]

# Hard-coded CRITICAL flags — any match here is immediate priority
# regardless of fuzzy confidence score
CRITICAL_ENTITIES = {
    "VADATA INC":                  "Amazon",
    "VADATA LLC":                  "Amazon",
    "STARGATE LLC":                "OpenAI_Stargate",
    "SGP OPERATOR LLC":            "OpenAI_Stargate",
    "STARGATE OPERATOR LLC":       "OpenAI_Stargate",
    "GRACELAND ACQUISITIONS LLC":  "Meta",
    "PAPYRUS ACQUISITIONS LLC":    "Meta",
    "QUANTUM VALLEY LLC":          "Alphabet",
    "CHARLESTON EAST LLC":         "Alphabet",
    "RREEF AMERICA LLC":           "Alphabet",
}


def build_flat_lookup() -> dict:
    """
    Flatten all known names to {NAME_UPPER: parent_company}.
    O(1) lookup for any name — check this first before any API call.
    """
    lookup = {}
    # Critical entities first (highest priority)
    for name, parent in CRITICAL_ENTITIES.items():
        lookup[name.upper()] = parent
    # Then all other known names
    for company, data in HYPERSCALER_MAP.items():
        for name in (data.get("subsidiaries", []) +
                     data.get("land_llcs", []) +
                     data.get("naming_patterns", [])):
            key = name.upper().strip()
            if key and key not in lookup:
                lookup[key] = company
    return lookup


def save(output_dir: str = "output") -> None:
    """Write known_subsidiaries.json and known_subsidiaries_flat.json."""
    out = Path(output_dir)
    out.mkdir(exist_ok=True)

    # Full table
    full_path = out / "known_subsidiaries.json"
    full_path.write_text(json.dumps(HYPERSCALER_MAP, indent=2))

    # Flat lookup
    flat = build_flat_lookup()
    flat_path = out / "known_subsidiaries_flat.json"
    flat_path.write_text(json.dumps(flat, indent=2))

    print(f"[subsidiaries] Saved full table to {full_path}")
    print(f"[subsidiaries] Saved flat lookup to {flat_path} ({len(flat)} entries)")

    print("\n=== Coverage ===")
    for company, data in HYPERSCALER_MAP.items():
        total = (len(data.get("subsidiaries", [])) +
                 len(data.get("land_llcs", [])))
        states = ", ".join(data.get("known_dc_states", [])[:5])
        print(f"  {company:20s}: {total:3d} entities | {states}")

    print(f"\n⚡ CRITICAL watch entities: {len(CRITICAL_ENTITIES)}")
    for name, parent in CRITICAL_ENTITIES.items():
        print(f"  {name:40s} → {parent}")


def load_flat(output_dir: str = "output") -> dict:
    """Load flat lookup from disk. Returns {} if file missing."""
    path = Path(output_dir) / "known_subsidiaries_flat.json"
    if path.exists():
        return json.loads(path.read_text())
    return {}


if __name__ == "__main__":
    save()
