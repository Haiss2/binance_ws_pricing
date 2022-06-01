CREATE TABLE "public"."prices" (
    "symbol" text NOT NULL,
    "price" decimal NOT NULL,
    "timestamp" bigint NOT NULL
) WITH (oids = false);

CREATE INDEX idx_symbol ON prices("symbol");
CREATE INDEX idx_timestamp ON prices("timestamp");
CREATE UNIQUE INDEX unique_symbol_timestamp ON prices("symbol", "timestamp");
