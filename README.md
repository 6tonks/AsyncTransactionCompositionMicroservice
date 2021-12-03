Basic Transaction Composition that uses async programing

USAGE:
    POST '/stockTransaction'
with body like
{
    "user_id": "102",
    "ticker": "ABC", "quantity": 10, "price": 5000.0, "transaction_type": "BUY"
}