import random
from collections import deque
from datetime import UTC, datetime, timedelta
from typing import Deque

from config import ProducerConfig


class TransactionGenerator:
    """Generates synthetic transaction events with controlled fraud injection."""

    def __init__(self, config: ProducerConfig) -> None:
        self.config = config
        self.rng = random.Random(config.random_seed)
        self.users = [f"U{1000 + i}" for i in range(1, 201)]
        self.categories = [
            "electronics",
            "grocery",
            "travel",
            "fashion",
            "fuel",
            "healthcare",
            "gaming",
            "utilities",
            "restaurants",
        ]
        self.countries = [
            "Sri Lanka",
            "Germany",
            "United Kingdom",
            "Singapore",
            "India",
            "United Arab Emirates",
            "Australia",
            "Canada",
            "Japan",
        ]
        self.user_last_transactions: dict[str, dict[str, datetime | str]] = {}
        self.recent_users: Deque[str] = deque(maxlen=1000)

    def _normal_amount(self) -> float:
        amount = max(1.0, self.rng.gauss(180, 120))
        return round(amount, 2)

    def _high_value_amount(self) -> float:
        amount = self.rng.uniform(self.config.high_value_threshold + 50, 15000)
        return round(amount, 2)

    def _pick_user(self) -> str:
        if self.rng.random() < 0.65 and self.recent_users:
            return self.rng.choice(list(self.recent_users))
        return self.rng.choice(self.users)

    def _normal_transaction(self, now: datetime | None = None) -> dict:
        now = now or datetime.now(UTC)
        user_id = self._pick_user()
        tx = {
            "user_id": user_id,
            "timestamp": now.isoformat(),
            "merchant_category": self.rng.choice(self.categories),
            "amount": self._normal_amount(),
            "location": self.rng.choice(self.countries),
        }
        self._remember(tx)
        return tx

    def _inject_high_value_fraud(self, tx: dict) -> dict:
        tx["amount"] = self._high_value_amount()
        return tx

    def _inject_impossible_travel_fraud(self, now: datetime | None = None) -> dict | None:
        now = now or datetime.now(UTC)
        candidate_users = [u for u, v in self.user_last_transactions.items() if isinstance(v.get("timestamp"), datetime)]
        if not candidate_users:
            return None

        user_id = self.rng.choice(candidate_users)
        last = self.user_last_transactions[user_id]
        last_ts = last["timestamp"]
        last_location = str(last["location"])

        if not isinstance(last_ts, datetime):
            return None

        within_10_min_ts = last_ts + timedelta(minutes=self.rng.randint(1, self.config.impossible_travel_window_minutes))
        event_ts = max(within_10_min_ts, now)

        far_locations = [country for country in self.countries if country != last_location]
        tx = {
            "user_id": user_id,
            "timestamp": event_ts.isoformat(),
            "merchant_category": self.rng.choice(self.categories),
            "amount": self._normal_amount(),
            "location": self.rng.choice(far_locations),
        }
        self._remember(tx)
        return tx

    def _remember(self, tx: dict) -> None:
        ts = datetime.fromisoformat(tx["timestamp"])
        self.user_last_transactions[tx["user_id"]] = {
            "timestamp": ts,
            "location": tx["location"],
        }
        self.recent_users.append(tx["user_id"])

    def generate(self) -> dict:
        now = datetime.now(UTC)
        tx = self._normal_transaction(now=now)

        if self.rng.random() >= self.config.fraud_probability:
            return tx

        fraud_type = self.rng.choice(["high_value", "impossible_travel"])
        if fraud_type == "high_value":
            return self._inject_high_value_fraud(tx)

        impossible_travel_tx = self._inject_impossible_travel_fraud(now=now)
        if impossible_travel_tx:
            return impossible_travel_tx

        # Fallback ensures configured fraud probability still produces a fraud-like signal.
        return self._inject_high_value_fraud(tx)
