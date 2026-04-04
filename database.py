import asyncpg
import logging
from typing import List, Optional
from datetime import datetime, timezone
from config import config

logger = logging.getLogger(__name__)


class Database:
    """
    PostgreSQL database with asyncpg connection pool.
    Supports: users, subscriptions, referrals, seen_items, tracked_items.
    """

    def __init__(self):
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        """Create PostgreSQL connection pool."""
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                dsn=config.DATABASE_URL,
                min_size=2,
                max_size=10,
                command_timeout=60,
            )
            logger.info("PostgreSQL connection pool created.")

    async def disconnect(self):
        """Close the connection pool."""
        if self._pool:
            await self._pool.close()
            self._pool = None
            logger.info("PostgreSQL connection pool closed.")

    @property
    def pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError("Database pool is not initialized. Call await db.init() first.")
        return self._pool

    async def init(self):
        """Initialize: connect + create/migrate tables."""
        await self.connect()
        async with self.pool.acquire() as conn:
            # ── Users ──────────────────────────────────────────────────────────
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    user_id     BIGINT PRIMARY KEY,
                    username    TEXT,
                    created_at  TIMESTAMPTZ DEFAULT NOW(),
                    discount_threshold INTEGER DEFAULT 0
                )
            """)
            # Migration: add columns
            await conn.execute("""
                ALTER TABLE users ADD COLUMN IF NOT EXISTS username TEXT
            """)
            await conn.execute("""
                ALTER TABLE users ADD COLUMN IF NOT EXISTS discount_threshold INTEGER DEFAULT 0
            """)

            # ── Subscriptions ──────────────────────────────────────────────────
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS subscriptions (
                    user_id         BIGINT PRIMARY KEY REFERENCES users(user_id) ON DELETE CASCADE,
                    is_active       BOOLEAN NOT NULL DEFAULT FALSE,
                    expires_at      TIMESTAMPTZ,
                    stars_paid      INTEGER DEFAULT 0,
                    activated_at    TIMESTAMPTZ,
                    updated_at      TIMESTAMPTZ DEFAULT NOW(),
                    last_notified_at TIMESTAMPTZ
                )
            """)

            # ── Referrals ──────────────────────────────────────────────────────
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS referrals (
                    id              SERIAL PRIMARY KEY,
                    referrer_id     BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                    referred_id     BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                    created_at      TIMESTAMPTZ DEFAULT NOW(),
                    rewarded        BOOLEAN NOT NULL DEFAULT FALSE,
                    UNIQUE (referred_id)
                )
            """)

            # Migration: add last_notified_at to subscriptions if it doesn't exist
            await conn.execute("""
                ALTER TABLE subscriptions ADD COLUMN IF NOT EXISTS last_notified_at TIMESTAMPTZ;
            """)

            # ── Seen Items (dedup) ─────────────────────────────────────────────
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS seen_items (
                    item_id   TEXT PRIMARY KEY,
                    added_at  TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            # ── Tracked Items (watchlist) ──────────────────────────────────────
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS tracked_items (
                    id          SERIAL PRIMARY KEY,
                    user_id     BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
                    shop        TEXT NOT NULL,
                    url         TEXT NOT NULL,
                    last_price  BIGINT NOT NULL,
                    added_at    TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            # Indexes
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_tracked_user ON tracked_items(user_id)
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_id)
            """)

        logger.info("Database tables initialized.")

    # ─────────────────────── Users ────────────────────────────────────────────

    async def add_user(self, user_id: int, username: str = None):
        """Register user (upsert username)."""
        await self.pool.execute(
            """
            INSERT INTO users (user_id, username)
            VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username
            """,
            user_id, username
        )
        # Ensure subscription row exists
        await self.pool.execute(
            "INSERT INTO subscriptions (user_id) VALUES ($1) ON CONFLICT DO NOTHING",
            user_id
        )

    async def get_user_threshold(self, user_id: int) -> int:
        """Get user's discount threshold (%) or 0 if not found."""
        row = await self.pool.fetchrow("SELECT discount_threshold FROM users WHERE user_id = $1", user_id)
        return row["discount_threshold"] if row else 0

    async def set_user_threshold(self, user_id: int, threshold: int):
        """Set user's discount threshold (%) (0-100)."""
        await self.pool.execute(
            "UPDATE users SET discount_threshold = $1 WHERE user_id = $2",
            max(0, min(100, threshold)), user_id
        )

    async def get_users_with_threshold(self, min_discount: int, premium_only: bool = True) -> List[int]:
        """Get all users (optionally premium only) whose threshold is met by min_discount."""
        if premium_only:
            query = """
                SELECT u.user_id FROM users u
                INNER JOIN subscriptions s ON u.user_id = s.user_id
                WHERE u.discount_threshold <= $1
                  AND s.is_active = TRUE
                  AND (s.expires_at IS NULL OR s.expires_at > NOW())
            """
        else:
            query = "SELECT user_id FROM users WHERE discount_threshold <= $1"
        
        rows = await self.pool.fetch(query, min_discount)
        return [row["user_id"] for row in rows]

    async def get_all_users(self) -> List[int]:
        """All user IDs."""
        rows = await self.pool.fetch("SELECT user_id FROM users")
        return [row["user_id"] for row in rows]

    async def get_premium_users(self) -> List[int]:
        """All active premium user IDs."""
        rows = await self.pool.fetch(
            """
            SELECT user_id FROM subscriptions
            WHERE is_active = TRUE AND (expires_at IS NULL OR expires_at > NOW())
            """
        )
        return [row["user_id"] for row in rows]

    async def get_user_count(self) -> int:
        row = await self.pool.fetchrow("SELECT COUNT(*) AS cnt FROM users")
        return row["cnt"]

    # ─────────────────────── Subscriptions ────────────────────────────────────

    async def activate_subscription(self, user_id: int, days: int, stars_paid: int = 0):
        """Activate or extend premium subscription."""
        await self.pool.execute(
            """
            INSERT INTO subscriptions (user_id, is_active, expires_at, stars_paid, activated_at, updated_at)
            VALUES ($1, TRUE,
                    NOW() + ($2 || ' days')::INTERVAL,
                    $3, NOW(), NOW())
            ON CONFLICT (user_id) DO UPDATE
                SET is_active    = TRUE,
                    expires_at   = GREATEST(subscriptions.expires_at, NOW()) + ($2 || ' days')::INTERVAL,
                    stars_paid   = subscriptions.stars_paid + $3,
                    activated_at = COALESCE(subscriptions.activated_at, NOW()),
                    updated_at   = NOW()
            """,
            user_id, str(days), stars_paid
        )
        logger.info(f"Subscription activated for user {user_id}, +{days} days, {stars_paid} stars.")

    async def deactivate_subscription(self, user_id: int):
        """Manually deactivate a subscription."""
        await self.pool.execute(
            "UPDATE subscriptions SET is_active = FALSE, expires_at = NULL, updated_at = NOW() WHERE user_id = $1",
            user_id
        )

    async def grant_permanent_premium(self, user_id: int):
        """
        Grant permanent (lifetime) premium — expires_at stays NULL forever.
        Safe to call on new users: creates the subscription row if needed.
        """
        # Ensure user exists in subscriptions table
        await self.pool.execute(
            "INSERT INTO subscriptions (user_id) VALUES ($1) ON CONFLICT DO NOTHING",
            user_id
        )
        await self.pool.execute(
            """
            UPDATE subscriptions
            SET is_active    = TRUE,
                expires_at   = NULL,
                activated_at = COALESCE(activated_at, NOW()),
                updated_at   = NOW()
            WHERE user_id = $1
            """,
            user_id
        )
        logger.info(f"Permanent premium granted to user {user_id}.")

    async def get_all_premium_list(self) -> list:
        """Returns list of all premium users with details (for admin)."""
        rows = await self.pool.fetch(
            """
            SELECT s.user_id, u.username, s.is_active, s.expires_at, s.stars_paid, s.activated_at
            FROM subscriptions s
            LEFT JOIN users u ON u.user_id = s.user_id
            WHERE s.is_active = TRUE
            ORDER BY s.activated_at DESC
            """
        )
        return [dict(row) for row in rows]

    async def is_premium(self, user_id: int) -> bool:
        """Check if user has active premium right now."""
        row = await self.pool.fetchrow(
            """
            SELECT is_active, expires_at FROM subscriptions WHERE user_id = $1
            """,
            user_id
        )
        if not row:
            return False
        if not row["is_active"]:
            return False
        if row["expires_at"] and row["expires_at"] < datetime.now(timezone.utc):
            # Auto-expire
            await self.deactivate_subscription(user_id)
            return False
        return True

    async def get_subscription_info(self, user_id: int) -> Optional[dict]:
        """Get full subscription info for a user."""
        row = await self.pool.fetchrow(
            "SELECT * FROM subscriptions WHERE user_id = $1",
            user_id
        )
        return dict(row) if row else None

    async def get_expiring_subscriptions(self, days_before: int = 3) -> list:
        """
        Get active subscriptions expiring within the next `days_before` days.
        Excludes permanent (expires_at IS NULL) ones.
        Only returns if not notified in the last 24 hours.
        """
        rows = await self.pool.fetch(
            """
            SELECT s.user_id, s.expires_at, u.username
            FROM subscriptions s
            LEFT JOIN users u ON u.user_id = s.user_id
            WHERE s.is_active = TRUE
              AND s.expires_at IS NOT NULL
              AND s.expires_at BETWEEN NOW() AND NOW() + ($1 || ' days')::INTERVAL
              AND (s.last_notified_at IS NULL OR s.last_notified_at < NOW() - INTERVAL '24 hours')
            """,
            str(days_before)
        )
        return [dict(row) for row in rows]

    async def update_last_notified(self, user_id: int):
        """Update last_notified_at timestamp for a user."""
        await self.pool.execute(
            "UPDATE subscriptions SET last_notified_at = NOW() WHERE user_id = $1",
            user_id
        )

    async def get_expired_subscriptions(self) -> list:
        """Get subscriptions that are marked active but have already expired."""
        rows = await self.pool.fetch(
            """
            SELECT user_id, expires_at
            FROM subscriptions
            WHERE is_active = TRUE
              AND expires_at IS NOT NULL
              AND expires_at < NOW()
            """
        )
        return [dict(row) for row in rows]

    # ─────────────────────── Referrals ────────────────────────────────────────

    async def register_referral(self, referrer_id: int, referred_id: int) -> bool:
        """
        Register that referred_id joined via referrer_id's link.
        Returns True if successfully registered (first time), False if already registered.
        """
        if referrer_id == referred_id:
            return False  # Can't refer yourself
        try:
            await self.pool.execute(
                """
                INSERT INTO referrals (referrer_id, referred_id)
                VALUES ($1, $2)
                ON CONFLICT DO NOTHING
                """,
                referrer_id, referred_id
            )
            # Check if it really was inserted (not a conflict)
            row = await self.pool.fetchrow(
                "SELECT rewarded FROM referrals WHERE referred_id = $1",
                referred_id
            )
            return row is not None
        except Exception as e:
            logger.error(f"register_referral error: {e}")
            return False

    async def get_referral_count(self, referrer_id: int) -> int:
        """Number of users who joined via this referrer."""
        row = await self.pool.fetchrow(
            "SELECT COUNT(*) AS cnt FROM referrals WHERE referrer_id = $1",
            referrer_id
        )
        return row["cnt"]

    async def get_referrals(self, referrer_id: int) -> List[dict]:
        """Get all referral records for a referrer."""
        rows = await self.pool.fetch(
            """
            SELECT r.referred_id, r.created_at, r.rewarded, u.username
            FROM referrals r
            LEFT JOIN users u ON u.user_id = r.referred_id
            WHERE r.referrer_id = $1
            ORDER BY r.created_at DESC
            """,
            referrer_id
        )
        return [dict(row) for row in rows]

    async def get_referrer_of(self, user_id: int) -> Optional[int]:
        """Who referred this user?"""
        row = await self.pool.fetchrow(
            "SELECT referrer_id FROM referrals WHERE referred_id = $1",
            user_id
        )
        return row["referrer_id"] if row else None

    async def has_referred_before(self, referred_id: int) -> bool:
        """Was this user already referred by someone?"""
        row = await self.pool.fetchrow(
            "SELECT 1 FROM referrals WHERE referred_id = $1",
            referred_id
        )
        return row is not None

    async def reward_referral(self, referrer_id: int, referred_id: int, bonus_days: int = 7):
        """
        Mark referral as rewarded and give referrer bonus subscription days.
        Called when the referred user activates a paid subscription.
        """
        result = await self.pool.execute(
            """
            UPDATE referrals SET rewarded = TRUE
            WHERE referrer_id = $1 AND referred_id = $2 AND rewarded = FALSE
            """,
            referrer_id, referred_id
        )
        # Check if it was actually updated (not already rewarded)
        if result == "UPDATE 1":
            await self.activate_subscription(referrer_id, days=bonus_days, stars_paid=0)
            logger.info(
                f"Referral reward: {referrer_id} gets +{bonus_days} days (referred: {referred_id})"
            )
            return True
        return False

    async def get_unrewarded_referral_count(self, referrer_id: int) -> int:
        """Count pending (unrewarded) referrals."""
        row = await self.pool.fetchrow(
            "SELECT COUNT(*) AS cnt FROM referrals WHERE referrer_id = $1 AND rewarded = FALSE",
            referrer_id
        )
        return row["cnt"]

    # ─────────────────────── Seen Items (dedup) ───────────────────────────────

    async def is_new_item(self, item_id: str) -> bool:
        """
        Returns True if item is new (not seen before), inserts it.
        """
        try:
            async with self.pool.acquire() as conn:
                result = await conn.execute(
                    "INSERT INTO seen_items (item_id) VALUES ($1) ON CONFLICT DO NOTHING",
                    item_id
                )
            inserted = int(result.split()[-1])
            return inserted > 0
        except Exception as e:
            logger.error(f"is_new_item error: {e}")
            return False

    async def clear_old_seen_items(self, days: int = 30):
        """Delete seen items older than N days."""
        deleted = await self.pool.execute(
            "DELETE FROM seen_items WHERE added_at < NOW() - ($1 || ' days')::INTERVAL",
            str(days)
        )
        logger.info(f"Cleared old seen_items: {deleted}")

    # ─────────────────────── Tracked Items ────────────────────────────────────

    async def add_tracked_item(self, user_id: int, shop: str, url: str, last_price: int):
        await self.pool.execute(
            "INSERT INTO tracked_items (user_id, shop, url, last_price) VALUES ($1, $2, $3, $4)",
            user_id, shop, url, last_price
        )

    async def get_user_tracked_items(self, user_id: int) -> List[dict]:
        rows = await self.pool.fetch(
            "SELECT * FROM tracked_items WHERE user_id = $1 ORDER BY added_at DESC",
            user_id
        )
        return [dict(row) for row in rows]

    async def get_all_tracked_items(self) -> List[dict]:
        rows = await self.pool.fetch("SELECT * FROM tracked_items ORDER BY id")
        return [dict(row) for row in rows]

    async def update_tracked_price(self, item_id: int, new_price: int):
        await self.pool.execute(
            "UPDATE tracked_items SET last_price = $1 WHERE id = $2",
            new_price, item_id
        )

    async def remove_tracked_item(self, item_id: int, user_id: int):
        await self.pool.execute(
            "DELETE FROM tracked_items WHERE id = $1 AND user_id = $2",
            item_id, user_id
        )

    async def remove_all_tracked_items(self, user_id: int):
        await self.pool.execute(
            "DELETE FROM tracked_items WHERE user_id = $1",
            user_id
        )

    # ─────────────────────── Stats ────────────────────────────────────────────

    async def get_stats(self) -> dict:
        async with self.pool.acquire() as conn:
            user_count = (await conn.fetchrow("SELECT COUNT(*) AS c FROM users"))["c"]
            premium_count = (await conn.fetchrow(
                "SELECT COUNT(*) AS c FROM subscriptions WHERE is_active = TRUE AND (expires_at IS NULL OR expires_at > NOW())"
            ))["c"]
            tracked_count = (await conn.fetchrow("SELECT COUNT(*) AS c FROM tracked_items"))["c"]
            seen_count = (await conn.fetchrow("SELECT COUNT(*) AS c FROM seen_items"))["c"]
            referral_count = (await conn.fetchrow("SELECT COUNT(*) AS c FROM referrals"))["c"]
        return {
            "users": user_count,
            "premium": premium_count,
            "tracked_items": tracked_count,
            "seen_items": seen_count,
            "referrals": referral_count,
        }


db = Database()