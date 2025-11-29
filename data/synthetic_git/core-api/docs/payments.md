# Payments API

The `/v1/payments/create` endpoint submits a new payment using the shared
library. Initial integration only requires `amount` and `currency`.

Downstream teams embed this module directly or call over HTTP.
