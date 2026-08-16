"""The three services that react to order lifecycle events.

All three run from this one package. A service is *data* — a name and a map from event
type to handler — not a separate program (D8). Its group id is derived from the name,
or overridden by the environment (D12).
"""
