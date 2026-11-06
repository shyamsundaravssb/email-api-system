INSERT INTO accounts (
    name,
    api_key_hash,
    from_email,
    from_name
)
VALUES (
    'Grasshopper Development',
    encode(sha256('dev-api-key'::bytea), 'hex'),
    'test@grasshopper.optimoze.com',
    'Grasshopper'
)
ON CONFLICT (api_key_hash) DO NOTHING;
