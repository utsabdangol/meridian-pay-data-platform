UPDATE demo.payments.fact_transactions
SET status = 'REFUNDED', last_updated_at = current_timestamp()
WHERE transaction_id = 't-00010e956833';
