CREATE TABLE attachment_inline_variants (
    manifest_id TEXT NOT NULL,
    item_index INTEGER NOT NULL,
    mime_type TEXT NOT NULL,
    byte_size INTEGER NOT NULL,
    sha256 TEXT NOT NULL,
    local_path TEXT NOT NULL,
    PRIMARY KEY (manifest_id, item_index),
    FOREIGN KEY (manifest_id, item_index)
        REFERENCES attachment_items(manifest_id, item_index)
        ON DELETE CASCADE
);
