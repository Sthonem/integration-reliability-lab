"""Synthetic reference data for the mock providers. Every name, phone and email is invented.

Phones use the +90 555 010 xxxx block and emails use example.com (reserved for
documentation), so nothing here can reach a real person.
"""

from typing import Any

POS_STORES: dict[str, dict[str, Any]] = {
    s["id"]: s
    for s in [
        {"id": "ST-IST-01", "name": "Demo Bistro Kadıköy", "city": "Istanbul", "timezone": "Europe/Istanbul"},
        {"id": "ST-IST-02", "name": "Demo Bistro Beşiktaş", "city": "Istanbul", "timezone": "Europe/Istanbul"},
        {"id": "ST-ANK-01", "name": "Demo Bistro Çankaya", "city": "Ankara", "timezone": "Europe/Istanbul"},
    ]
}

# Prices in kuruş (1 TRY = 100 kuruş).
POS_MENU: dict[str, dict[str, Any]] = {
    p["sku"]: p
    for p in [
        {"sku": "SKU-LATTE", "name": "Latte", "price_minor": 9500},
        {"sku": "SKU-AMERICANO", "name": "Americano", "price_minor": 8000},
        {"sku": "SKU-TEA", "name": "Turkish tea", "price_minor": 2500},
        {"sku": "SKU-SIMIT", "name": "Simit", "price_minor": 3500},
        {"sku": "SKU-CHEESECAKE", "name": "San Sebastian cheesecake", "price_minor": 14000},
    ]
}


def loyalty_members() -> dict[str, dict[str, Any]]:
    """Fresh copy per mock instance (members' balances are mutable)."""
    members = [
        ("LM-1001", "Ayşe Yılmaz", "+905550101001", "gold", 1240),
        ("LM-1002", "Mehmet Kaya", "+905550101002", "silver", 310),
        ("LM-1003", "Elif Demir", "+905550101003", "bronze", 45),
        ("LM-1004", "Can Öztürk", "+905550101004", "silver", 520),
        ("LM-1005", "Sarah Collins", "+905550101005", "bronze", 0),
    ]
    return {
        mid: {
            "id": mid,
            "full_name": name,
            "phone": phone,
            "email": f"{mid.lower()}@example.com",
            "tier": tier,
            "points_balance": points,
            "version": 1,
        }
        for mid, name, phone, tier, points in members
    }
