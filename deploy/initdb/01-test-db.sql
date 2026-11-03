-- A separate database for `make test-db`, so tests never touch local data.
CREATE DATABASE webhooks_test OWNER webhooks;
