CREATE NAMESPACE IF NOT EXISTS demo.payments;
CALL demo.system.register_table(table => 'payments.fact_transactions', metadata_file => 's3://meridian-pay-lakehouse-utsab/payments/fact_transactions/metadata/00030-42e30d3b-2d36-435c-adb5-0ba419807c64.metadata.json');
