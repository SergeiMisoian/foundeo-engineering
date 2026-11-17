# Foundeo engineering

I built and launched [Foundeo](https://foundeo.io/), a Telegram outreach platform with AI-assisted lead enrichment. This case study covers one decision behind its AI columns and includes a runnable example of retry-safe background processing.

## Case study · Running AI work in batches

Foundeo users can define a column with a prompt and run it across selected rows in a lead list. The interface needs to register the work immediately while model calls finish in the background.

Creating the column and scheduling its first batch in separate transactions can leave an empty column behind if scheduling fails. I made those two changes commit together. The worker retries temporary provider failures within a fixed budget and stops the run on an authentication failure. This lets users recover from a temporary outage without repeatedly sending requests with invalid credentials.

Foundeo's tests check that a successful request saves both the column and its first run, and that a scheduling failure rolls back the column. Error-path tests cover temporary provider failures and invalid credentials.

## Run the example

The Python example uses synthetic document events and SQLite. Python 3.11 or newer is sufficient.

```sh
python3 event_inbox.py
python3 -m unittest -v
```

The demo accepts an event, ignores a duplicate delivery, records a simulated timeout, and completes the event on the next attempt.

- An event ID identifies an immutable payload. Reusing it with a different payload raises an error.
- The worker persists attempt counts and stops retrying after three temporary failures.
- The update that records completion also saves the result. A repeated call returns that stored result.

The example uses a local, deterministic handler and commits all persistent effects in SQLite. A handler that calls an external service would also need that service's idempotency mechanism.
