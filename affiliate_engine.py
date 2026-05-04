"""
Viralityy — M8: Affiliate Programme Engine
-------------------------------------------
Manages the full affiliate lifecycle:
  - Generating unique referral codes per user
  - Tracking signups through referral links
  - Recording subscription conversions
  - Calculating 30% commission on each payment
  - Tracking payout status (pending → paid)
  - Providing dashboard summary data

Commission structure:
  - 30% of each referred user's subscription payment
  - Applies for the lifetime of the referred user's subscription
  - Minimum payout threshold: $50
  - Payout method: Stripe Connect (or manual for Pakistan / Payoneer)

MongoDB collections used:
  - affiliates         (one doc per affiliate user)
  - affiliate_clicks   (one doc per link click)
  - affiliate_referrals(one doc per referred user signup)
  - affiliate_commissions (one doc per commission event)
"""

import os
import asyncio
import hashlib
import secrets
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

try:
    from motor.motor_asyncio import AsyncIOMotorClient
    HAS_MOTOR = True
except ImportError:
    HAS_MOTOR = False

logging.basicConfig(level=logging.INFO, format='[Affiliate] %(levelname)s %(message)s')
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# COMMISSION CONFIG
# ---------------------------------------------------------------------------
COMMISSION_RATE     = 0.30    # 30%
MIN_PAYOUT_USD      = 50.00   # minimum to trigger payout
COOKIE_DAYS         = 30      # referral cookie lifetime in days

PLAN_PRICES_USD = {
    'shorts_starter':  29.00,
    'shorts_pro':      59.00,
    'longform_starter':49.00,
    'longform_pro':    89.00,
    'combo_pro':       99.00,
    'combo_agency':   199.00,
}


class AffiliateEngine:

    def __init__(self, mongo_uri: str = None):
        self.mongo_uri = mongo_uri or os.environ.get('MONGODB_URI', 'mongodb://localhost:27017')
        self.db = None

    async def _get_db(self):
        if self.db:
            return self.db
        if not HAS_MOTOR:
            raise RuntimeError("pip install motor")
        client = AsyncIOMotorClient(self.mongo_uri)
        self.db = client['viralityy']
        return self.db

    # ==================================================================
    # AFFILIATE ACCOUNT MANAGEMENT
    # ==================================================================

    async def get_or_create_affiliate(self, user_id: str, user_email: str) -> dict:
        """
        Returns existing affiliate record or creates one.
        Every Viralityy user can be an affiliate — opt-in on first visit
        to the affiliate dashboard page.
        """
        db = await self._get_db()
        existing = await db.affiliates.find_one({'userId': user_id})
        if existing:
            return existing

        code = self._generate_code(user_id)
        affiliate = {
            'userId':           user_id,
            'email':            user_email,
            'referralCode':     code,
            'referralUrl':      f"https://viralityy.com/?ref={code}",
            'status':           'active',
            'joinedAt':         datetime.now(timezone.utc).isoformat(),
            'totalClicks':      0,
            'totalSignups':     0,
            'totalConversions': 0,
            'totalEarnedUsd':   0.0,
            'pendingPayoutUsd': 0.0,
            'paidOutUsd':       0.0,
            'payoutMethod':     None,   # set by user: 'stripe' | 'payoneer' | 'bank'
            'payoutDetails':    {},
        }
        await db.affiliates.insert_one(affiliate)
        log.info(f"Created affiliate account for user {user_id}, code: {code}")
        return affiliate

    async def get_affiliate(self, user_id: str) -> Optional[dict]:
        db = await self._get_db()
        return await db.affiliates.find_one({'userId': user_id})

    async def get_affiliate_by_code(self, code: str) -> Optional[dict]:
        db = await self._get_db()
        return await db.affiliates.find_one({'referralCode': code})

    # ==================================================================
    # CLICK TRACKING
    # ==================================================================

    async def track_click(self, referral_code: str, ip_hash: str,
                          user_agent: str = '') -> bool:
        """
        Records a click on a referral link.
        Deduplicates by IP hash within 24h to avoid artificial inflation.
        Returns True if click was recorded, False if deduplicated.
        """
        db = await self._get_db()
        affiliate = await self.get_affiliate_by_code(referral_code)
        if not affiliate:
            return False

        # Dedup: same IP hash within 24h
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        existing = await db.affiliate_clicks.find_one({
            'referralCode': referral_code,
            'ipHash':       ip_hash,
            'clickedAt':    {'$gte': cutoff},
        })
        if existing:
            return False

        await db.affiliate_clicks.insert_one({
            'referralCode': referral_code,
            'affiliateId':  str(affiliate['_id']),
            'ipHash':       ip_hash,
            'userAgent':    user_agent[:200],
            'clickedAt':    datetime.now(timezone.utc).isoformat(),
        })

        await db.affiliates.update_one(
            {'referralCode': referral_code},
            {'$inc': {'totalClicks': 1}}
        )
        return True

    # ==================================================================
    # SIGNUP TRACKING
    # ==================================================================

    async def track_signup(self, new_user_id: str, referral_code: str) -> bool:
        """
        Called when a new user registers via a referral link.
        Links the new user to the affiliate for commission tracking.
        """
        db = await self._get_db()
        affiliate = await self.get_affiliate_by_code(referral_code)
        if not affiliate:
            return False

        # Check this user wasn't already referred
        existing = await db.affiliate_referrals.find_one({'referredUserId': new_user_id})
        if existing:
            return False

        await db.affiliate_referrals.insert_one({
            'affiliateUserId':  str(affiliate['userId']),
            'referralCode':     referral_code,
            'referredUserId':   new_user_id,
            'status':           'signed_up',   # signed_up → converted → churned
            'signedUpAt':       datetime.now(timezone.utc).isoformat(),
            'convertedAt':      None,
            'plan':             None,
            'lifetimeCommission': 0.0,
        })

        await db.affiliates.update_one(
            {'referralCode': referral_code},
            {'$inc': {'totalSignups': 1}}
        )

        # Tag the new user with their referral source
        await db.users.update_one(
            {'_id': new_user_id},
            {'$set': {'referredBy': referral_code, 'referredByUserId': str(affiliate['userId'])}}
        )

        log.info(f"Signup tracked: user {new_user_id} referred by {referral_code}")
        return True

    # ==================================================================
    # COMMISSION RECORDING
    # ==================================================================

    async def record_commission(self, referred_user_id: str, payment_amount_usd: float,
                                plan: str, stripe_payment_id: str = None) -> Optional[dict]:
        """
        Called by the Stripe webhook handler when a referred user makes a payment.
        Calculates 30% commission and adds it to the affiliate's pending payout.
        """
        db = await self._get_db()

        # Find which affiliate referred this user
        referral = await db.affiliate_referrals.find_one({'referredUserId': referred_user_id})
        if not referral:
            return None  # user not referred by anyone

        affiliate = await db.affiliates.find_one({'userId': referral['affiliateUserId']})
        if not affiliate or affiliate.get('status') != 'active':
            return None

        commission_usd = round(payment_amount_usd * COMMISSION_RATE, 2)

        # Record commission event
        commission = {
            'affiliateUserId':  referral['affiliateUserId'],
            'referralCode':     referral['referralCode'],
            'referredUserId':   referred_user_id,
            'plan':             plan,
            'paymentAmountUsd': payment_amount_usd,
            'commissionUsd':    commission_usd,
            'commissionRate':   COMMISSION_RATE,
            'stripePaymentId':  stripe_payment_id,
            'status':           'pending',   # pending → approved → paid
            'earnedAt':         datetime.now(timezone.utc).isoformat(),
            'paidAt':           None,
        }
        result = await db.affiliate_commissions.insert_one(commission)

        # Update affiliate totals
        await db.affiliates.update_one(
            {'userId': referral['affiliateUserId']},
            {'$inc': {
                'totalEarnedUsd':   commission_usd,
                'pendingPayoutUsd': commission_usd,
                'totalConversions': 1,
            }}
        )

        # Update referral record
        await db.affiliate_referrals.update_one(
            {'referredUserId': referred_user_id},
            {'$set':  {'status': 'converted', 'convertedAt': datetime.now(timezone.utc).isoformat(), 'plan': plan},
             '$inc':  {'lifetimeCommission': commission_usd}}
        )

        log.info(f"Commission recorded: ${commission_usd} for affiliate {referral['affiliateUserId']}")
        return {**commission, '_id': str(result.inserted_id)}

    # ==================================================================
    # PAYOUTS
    # ==================================================================

    async def get_pending_payouts(self) -> list:
        """Returns all affiliates with pending balance >= MIN_PAYOUT_USD."""
        db = await self._get_db()
        return await db.affiliates.find(
            {'pendingPayoutUsd': {'$gte': MIN_PAYOUT_USD}, 'status': 'active'}
        ).to_list(length=500)

    async def mark_paid(self, affiliate_user_id: str, amount_usd: float,
                        payout_reference: str = '') -> bool:
        """Mark a payout as completed — called after Stripe/Payoneer transfer."""
        db = await self._get_db()
        result = await db.affiliates.update_one(
            {'userId': affiliate_user_id},
            {
                '$inc': {'paidOutUsd': amount_usd, 'pendingPayoutUsd': -amount_usd},
                '$push': {'payoutHistory': {
                    'amountUsd':  amount_usd,
                    'reference':  payout_reference,
                    'paidAt':     datetime.now(timezone.utc).isoformat(),
                }}
            }
        )
        # Mark individual commission records as paid
        await db.affiliate_commissions.update_many(
            {'affiliateUserId': affiliate_user_id, 'status': 'pending'},
            {'$set': {'status': 'paid', 'paidAt': datetime.now(timezone.utc).isoformat()}}
        )
        return result.modified_count > 0

    # ==================================================================
    # DASHBOARD DATA
    # ==================================================================

    async def get_dashboard_data(self, user_id: str) -> dict:
        """Full dashboard summary for the affiliate page."""
        db = await self._get_db()
        affiliate = await db.affiliates.find_one({'userId': user_id})
        if not affiliate:
            return {'exists': False}

        # Recent referrals
        referrals = await db.affiliate_referrals.find(
            {'affiliateUserId': user_id},
            sort=[('signedUpAt', -1)]
        ).to_list(length=20)

        # Recent commissions
        commissions = await db.affiliate_commissions.find(
            {'affiliateUserId': user_id},
            sort=[('earnedAt', -1)]
        ).to_list(length=20)

        # Monthly breakdown (last 6 months)
        monthly = await self._monthly_breakdown(db, user_id)

        return {
            'exists':           True,
            'referralCode':     affiliate['referralCode'],
            'referralUrl':      affiliate['referralUrl'],
            'totalClicks':      affiliate['totalClicks'],
            'totalSignups':     affiliate['totalSignups'],
            'totalConversions': affiliate['totalConversions'],
            'totalEarnedUsd':   affiliate['totalEarnedUsd'],
            'pendingPayoutUsd': affiliate['pendingPayoutUsd'],
            'paidOutUsd':       affiliate['paidOutUsd'],
            'conversionRate':   round(
                affiliate['totalConversions'] / max(affiliate['totalSignups'], 1) * 100, 1
            ),
            'minPayoutUsd':     MIN_PAYOUT_USD,
            'payoutReady':      affiliate['pendingPayoutUsd'] >= MIN_PAYOUT_USD,
            'recentReferrals':  [self._clean_referral(r) for r in referrals],
            'recentCommissions':[self._clean_commission(c) for c in commissions],
            'monthlyBreakdown': monthly,
            'joinedAt':         affiliate['joinedAt'],
        }

    async def _monthly_breakdown(self, db, user_id: str) -> list:
        """Commission totals grouped by month for the last 6 months."""
        pipeline = [
            {'$match': {'affiliateUserId': user_id}},
            {'$addFields': {'month': {'$substr': ['$earnedAt', 0, 7]}}},
            {'$group': {
                '_id':            '$month',
                'commissionUsd':  {'$sum': '$commissionUsd'},
                'conversions':    {'$sum': 1},
            }},
            {'$sort': {'_id': -1}},
            {'$limit': 6}
        ]
        docs = await db.affiliate_commissions.aggregate(pipeline).to_list(length=6)
        return [{'month': d['_id'], 'commissionUsd': round(d['commissionUsd'], 2),
                 'conversions': d['conversions']} for d in docs]

    def _clean_referral(self, r: dict) -> dict:
        return {
            'referredUserId': r['referredUserId'][:8] + '…',  # partial for privacy
            'status':         r['status'],
            'plan':           r.get('plan'),
            'signedUpAt':     r['signedUpAt'][:10],
            'lifetimeCommission': r.get('lifetimeCommission', 0),
        }

    def _clean_commission(self, c: dict) -> dict:
        return {
            'plan':           c['plan'],
            'paymentAmount':  c['paymentAmountUsd'],
            'commission':     c['commissionUsd'],
            'status':         c['status'],
            'earnedAt':       c['earnedAt'][:10],
        }

    # ==================================================================
    # HELPERS
    # ==================================================================

    def _generate_code(self, user_id: str) -> str:
        """Generates a short, unique, readable referral code."""
        raw = hashlib.sha256(f"{user_id}:{secrets.token_hex(8)}".encode()).hexdigest()
        return raw[:8].upper()   # e.g. "A3F8C21B"
