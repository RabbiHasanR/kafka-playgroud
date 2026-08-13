"""The three services that react to order lifecycle events.

All three run from this one package. A service is *data* — a name, a consumer group,
and a map from event type to handler — not a separate program (D8).
"""
