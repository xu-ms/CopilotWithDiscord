UPDATE liveness_leases
SET kind = 'observed_background'
WHERE kind = 'background';
