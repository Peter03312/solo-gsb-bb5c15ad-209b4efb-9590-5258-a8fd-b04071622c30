"""Regression tests for content-identity (fingerprint) comparisons.

Calibration identifiers and units are caller-provided strings and may contain
the characters older builds happened to use as fingerprint delimiters
(``|``, ``=``, newlines). Fingerprints must therefore be unambiguous: two
payloads that differ in any field value must never be judged the same content
on single ingest, on preview, or for whole-package idempotency; a package
fingerprint ignores only the order revisions are listed in.
"""


def _event_body(rev, parent, key, *, sensor="S1", start=0, end=100,
                value=1.0, unit="C", withdrawn=False, batch="B1"):
    return {
        "batch_id": batch,
        "sensor_id": sensor,
        "revision_id": rev,
        "parent_id": parent,
        "idempotency_key": key,
        "start_ns": start,
        "end_ns": end,
        "value": value,
        "unit": unit,
        "withdrawn": withdrawn,
    }


def _rev_in_package(rev, parent, key, **kw):
    body = _event_body(rev, parent, key, **kw)
    del body["batch_id"]
    return body


def _package(*revs, batch="B1", package_id="PKG-1"):
    return {"batch_id": batch, "package_id": package_id,
            "revisions": list(revs)}


# ---------------------------------------------------------------------------
# Single ingest: different field values must not collide on the delimiter
# ---------------------------------------------------------------------------


def test_same_key_distinct_sensor_revision_split_is_conflict(client):
    # sensor_id "S|X" / revision_id "R" and sensor_id "S" / revision_id
    # "X|R" produced identical legacy fingerprints.
    first = client.post(
        "/api/v1/batches/B1/events",
        json=_event_body("R", "ROOT", "dup-key", sensor="S|X"),
    )
    assert first.status_code == 200
    assert first.json()["status"] == "accepted"

    collision = client.post(
        "/api/v1/batches/B1/events",
        json=_event_body("X|R", "ROOT", "dup-key", sensor="S"),
    )
    assert collision.status_code == 409
    # The conflict must not have written the second revision.
    result = client.get("/api/v1/batches/B1/result").json()
    assert result["revisions_total"] == 1
    assert {s["sensor_id"] for s in result["sensors"]} == {"S|X"}


def test_same_revision_id_distinct_parent_split_is_conflict(client):
    # parent_id "P|x" vs parent_id "P" used to be comparable only through a
    # fingerprint that could not tell field boundaries apart.
    first = client.post(
        "/api/v1/batches/B1/events",
        json=_event_body("R", "P|x", "k1", sensor="S"),
    )
    assert first.json()["status"] == "accepted"
    collision = client.post(
        "/api/v1/batches/B1/events",
        json=_event_body("R", "P", "k2", sensor="S"),
    )
    assert collision.status_code == 409
    result = client.get("/api/v1/batches/B1/result").json()
    assert result["revisions_total"] == 1


def test_same_revision_id_distinct_unit_with_delimiter_is_conflict(client):
    client.post(
        "/api/v1/batches/B1/events",
        json=_event_body("R", "ROOT", "k1", unit="C"),
    )
    # A different unit is different content even when it contains "|".
    collision = client.post(
        "/api/v1/batches/B1/events",
        json=_event_body("R", "ROOT", "k2", unit="C|D"),
    )
    assert collision.status_code == 409


def test_identical_content_with_delimiter_replays_as_duplicate(client):
    body = _event_body("R", "ROOT", "k1", sensor="S|X", unit="C|D")
    assert client.post("/api/v1/batches/B1/events", json=body).json()[
        "status"
    ] == "accepted"
    # Same payload (same key) -> duplicate; same content with a new key also
    # deduplicates and registers the new key.
    assert client.post("/api/v1/batches/B1/events", json=body).json()[
        "status"
    ] == "duplicate"
    body_new_key = dict(body, idempotency_key="k2")
    assert client.post(
        "/api/v1/batches/B1/events", json=body_new_key
    ).json()["status"] == "duplicate"
    # The newly registered key also sees the original request as a duplicate.
    assert client.post("/api/v1/batches/B1/events", json=body).json()[
        "status"
    ] == "duplicate"


# ---------------------------------------------------------------------------
# Preview: same rules as ingest, no writes on conflict
# ---------------------------------------------------------------------------


def test_preview_distinguishes_fields_on_delimiter_collision(client):
    client.post(
        "/api/v1/batches/B1/events",
        json=_event_body("R", "ROOT", "dup-key", sensor="S|X"),
    )
    collision = client.post(
        "/api/v1/batches/B1/events:preview",
        json=_event_body("X|R", "ROOT", "dup-key", sensor="S"),
    )
    assert collision.status_code == 409

    # Same revision id, genuinely different parent -> conflict as well.
    client.post(
        "/api/v1/batches/B1/events",
        json=_event_body("Q", "P|x", "qk", sensor="S2"),
    )
    assert client.post(
        "/api/v1/batches/B1/events:preview",
        json=_event_body("Q", "P", "qk2", sensor="S2"),
    ).status_code == 409


def test_preview_exact_replay_with_delimiter_is_duplicate(client):
    body = _event_body("R", "ROOT", "k1", sensor="S|X")
    client.post("/api/v1/batches/B1/events", json=body)
    preview = client.post(
        "/api/v1/batches/B1/events:preview", json=body
    ).json()
    assert preview["status"] == "duplicate"
    assert preview["before_timeline_hash"] == preview["after_timeline_hash"]
    assert preview["changed_intervals"] == []


# ---------------------------------------------------------------------------
# Packages: order-insensitive identity, any field/key change is conflict
# ---------------------------------------------------------------------------


def test_package_order_only_difference_replays_first_result(client):
    body = _package(
        _rev_in_package("v2", "v1", "e2", start=100, end=200, value=3.5),
        _rev_in_package("v1", "ROOT", "e1", start=0, end=300, value=1.0),
    )
    first = client.post("/api/v1/batches/B1/packages", json=body).json()
    assert first["status"] == "accepted"

    reordered = dict(body, revisions=list(reversed(body["revisions"])))
    replay = client.post("/api/v1/batches/B1/packages", json=reordered).json()
    assert replay["status"] == "duplicate"
    assert replay["result"] == first["result"]
    # The stored per-revision statuses are returned verbatim (first order).
    assert replay["revisions"] == first["revisions"]


def test_package_delimiter_collision_content_change_is_conflict(client):
    # First package: one revision with sensor "S|X", revision "R".
    body = _package(
        _rev_in_package("R", "ROOT", "K", sensor="S|X"),
        package_id="PKG-D",
    )
    assert client.post(
        "/api/v1/batches/B1/packages", json=body
    ).json()["status"] == "accepted"

    # Same package id, legacy-colliding field split -> 409, nothing written.
    changed = _package(
        _rev_in_package("X|R", "ROOT", "K", sensor="S"),
        package_id="PKG-D",
    )
    resp = client.post("/api/v1/batches/B1/packages", json=changed)
    assert resp.status_code == 409
    result = client.get("/api/v1/batches/B1/result").json()
    assert result["revisions_total"] == 1
    assert {s["sensor_id"] for s in result["sensors"]} == {"S|X"}


def test_package_idempotency_key_change_is_conflict(client):
    body = _package(_rev_in_package("v1", "ROOT", "e1"), package_id="PKG-K")
    assert client.post(
        "/api/v1/batches/B1/packages", json=body
    ).json()["status"] == "accepted"

    changed = _package(_rev_in_package("v1", "ROOT", "e1-other"),
                       package_id="PKG-K")
    assert client.post(
        "/api/v1/batches/B1/packages", json=changed
    ).status_code == 409


def test_package_any_field_change_is_conflict(client):
    body = _package(
        _rev_in_package("v1", "ROOT", "e1", value=1.0, unit="C"),
        package_id="PKG-F",
    )
    client.post("/api/v1/batches/B1/packages", json=body)

    # A unit carrying a delimiter is still a different value, not a replay.
    changed_unit = _package(
        _rev_in_package("v1", "ROOT", "e1", value=1.0, unit="C|D"),
        package_id="PKG-F",
    )
    assert client.post(
        "/api/v1/batches/B1/packages", json=changed_unit
    ).status_code == 409

    # Adding a revision is different content too.
    added = _package(
        _rev_in_package("v1", "ROOT", "e1", value=1.0, unit="C"),
        _rev_in_package("v2", "v1", "e2", start=0, end=10, value=2.0),
        package_id="PKG-F",
    )
    assert client.post(
        "/api/v1/batches/B1/packages", json=added
    ).status_code == 409


def test_invalid_package_still_422_and_writes_nothing(client):
    # Duplicate idempotency key inside the package -> 422.
    body = _package(
        _rev_in_package("v1", "ROOT", "same-key"),
        _rev_in_package("v2", "v1", "same-key", start=0, end=10, value=2.0),
        package_id="PKG-BAD",
    )
    resp = client.post("/api/v1/batches/B1/packages", json=body)
    assert resp.status_code == 422
    assert client.get("/api/v1/batches/B1/result").status_code == 404

    # Invalid revision (end <= start) -> 422.
    bad = _package(
        _rev_in_package("v1", "ROOT", "e1", start=100, end=100, value=1.0),
        package_id="PKG-BAD2",
    )
    assert client.post(
        "/api/v1/batches/B1/packages", json=bad
    ).status_code == 422
    assert client.get("/api/v1/batches/B1/result").status_code == 404


def test_concurrent_same_key_distinct_payloads_one_accepted_rest_409(client):
    import threading

    # Two genuinely different revisions racing for one idempotency key:
    # exactly one is accepted and every other contender conflicts (never a
    # false "duplicate"), regardless of delimiter-bearing ids.
    bodies = [
        _event_body("R", "ROOT", "race-key", sensor="S|X"),
        _event_body("X|R", "ROOT", "race-key", sensor="S"),
    ]
    barrier = threading.Barrier(2 * 8)
    responses: list = []

    def submit(body):
        barrier.wait(timeout=10)
        responses.append(
            client.post("/api/v1/batches/B1/events", json=body)
        )

    threads = []
    for body in bodies:
        for _ in range(8):
            threads.append(threading.Thread(target=submit, args=(body,)))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    assert not any(thread.is_alive() for thread in threads)

    statuses = sorted(r.status_code for r in responses)
    # The first contender wins (200 accepted); its exact replays are also
    # 200 duplicates, while all replays of the *other* content get 409.
    accepted = [r for r in responses if r.status_code == 200
                and r.json()["status"] == "accepted"]
    assert len(accepted) == 1
    winner_sensor = accepted[0].json()["sensor_id"]
    for r in responses:
        if r.status_code == 200:
            assert r.json()["sensor_id"] == winner_sensor
        else:
            assert r.status_code == 409
    assert statuses.count(409) == 8
    result = client.get("/api/v1/batches/B1/result").json()
    assert result["revisions_total"] == 1
