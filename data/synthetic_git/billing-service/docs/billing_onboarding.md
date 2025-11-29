# Billing Onboarding

1. Collect the cart total.
2. Call `/v1/payments/create` with `amount` and `currency`.
3. Record the `payment_id` for future reconciliation.

VAT codes are *not* required for the current contract.
