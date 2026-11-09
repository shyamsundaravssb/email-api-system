-- Allow terminal failure state.
ALTER TABLE generation_handoffs
DROP CONSTRAINT generation_handoffs_status_check;

ALTER TABLE generation_handoffs
ADD CONSTRAINT generation_handoffs_status_check
CHECK (
    status IN ('pending', 'processing', 'submitted', 'failed')
);

-- Record when a handoff permanently fails.
ALTER TABLE generation_handoffs
ADD COLUMN failed_at TIMESTAMPTZ;