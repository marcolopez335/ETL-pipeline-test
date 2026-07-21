#!/usr/bin/env python3
"""Generate a deterministic mock STORIES extract for the dashboard mockup.

Fabricates a realistic slice of the data the real pipeline exports to
``STORIES.hyper`` — programs, teams, PIs, sprints, features, stories, and
weekly Monday snapshots — using only the standard library, so it runs
anywhere without the pipeline's dependencies.

Outputs:
    dashboard/data/stories_mock.csv    Flat extract mirroring STORIES.hyper
                                       (history snapshots + current summary,
                                       Title Case columns like the pipeline)
    dashboard/data/stories_mock.json   Compact story/feature model consumed
                                       by dashboard/index.html
    dashboard/index.html               Data re-injected between the
                                       stories-data <script> markers, if the
                                       file exists
    output/STORIES_MOCK.hyper          Only with --hyper, and only if
                                       polars + pantab are installed

Everything is seeded and the "now" timestamp is pinned, so re-running the
script produces byte-identical output (stable git diffs). Pass --seed to
get a different but equally deterministic dataset.
"""
from __future__ import annotations

import argparse
import csv
import json
import random
import re
from datetime import date, datetime, timedelta
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO_ROOT = HERE.parent
DATA_DIR = HERE / "data"
INDEX_HTML = HERE / "index.html"
HYPER_PATH = REPO_ROOT / "output" / "STORIES_MOCK.hyper"

# ---------------------------------------------------------------------------
# Fixed clock — the mock equivalent of the pipeline's LAST_UPDATED / snapshot
# fill. Pinned so regenerating the data never churns the repo.
# ---------------------------------------------------------------------------
GENERATED_AT = datetime(2026, 7, 21, 6, 30, 0)
SNAPSHOTS = [date(2026, 5, 18) + timedelta(weeks=i) for i in range(10)]
CURRENT_SNAPSHOT = SNAPSHOTS[-1]

STATUSES = ["Open", "In Progress", "In Review", "Blocked", "Done"]

# PI calendar: 5 two-week dev sprints + a 3-week IP sprint = 13 weeks.
PIS: dict[str, tuple[date, date]] = {
    "PI 25.4": (date(2026, 2, 16), date(2026, 5, 17)),
    "PI 26.1": (date(2026, 5, 18), date(2026, 8, 16)),
    "PI 26.2": (date(2026, 8, 17), date(2026, 11, 15)),
}
FIX_VERSIONS = {"PI 25.4": "2026.R1", "PI 26.1": "2026.R2", "PI 26.2": "2026.R3"}

PROGRAMS: dict[str, dict] = {
    "AMMM": {
        "name": "AMMM",
        "teams": ["Team Falcon", "Team Osprey", "Team Kestrel"],
        "components": ["Scheduler", "Telemetry", "Ops Console"],
        "nouns": ["telemetry ingest", "mission scheduler", "crew rostering",
                  "ops console", "maintenance forecasting", "sortie planning",
                  "readiness scoring", "work-order routing"],
    },
    "GDX": {
        "name": "Ground Data Exchange",
        "teams": ["Team Nimbus", "Team Cirrus", "Team Vega"],
        "components": ["Ingest", "Catalog", "API Gateway"],
        "nouns": ["data catalog", "ingest pipeline", "API gateway",
                  "schema registry", "archive tiering", "event streaming",
                  "access auditing", "downlink processing"],
    },
    "VIP": {
        "name": "Vehicle Integration Platform",
        "teams": ["Team Raptor", "Team Lynx"],
        "components": ["Diagnostics", "Provisioning", "Firmware"],
        "nouns": ["fault diagnostics", "fleet provisioning", "firmware rollout",
                  "sensor calibration", "health monitoring", "config baselining",
                  "test harness", "release gating"],
    },
}

ADJECTIVES = ["Automated", "Unified", "Real-time", "Self-service", "Resilient",
              "Predictive", "Streamlined", "Federated"]

FIRST = ["Rae", "Miguel", "Priya", "Dana", "Kofi", "Elena", "Tom", "Aisha",
         "Viktor", "June", "Omar", "Lena", "Sam", "Noor", "Iris", "Hugo",
         "Tessa", "Ravi", "Cleo", "Marcus", "Ana", "Felix", "Wren", "Idris"]
LAST = ["Vance", "Okafor", "Lindqvist", "Marsh", "Ito", "Delgado", "Barros",
        "Chen", "Novak", "Ferris", "Haddad", "Sorensen", "Quinn", "Mbeki",
        "Petrov", "Alvarez", "Kowalski", "Rhee", "Duarte", "Ellison"]

LABEL_POOL = ["Committed", "Committed", "Committed", "Stretch", "PI-Objective",
              "Dependency", "", "", ""]

# Feature health archetypes → completion fraction at the current snapshot.
PROFILES = {
    "done":     (1.00, 1.00),
    "ahead":    (0.86, 0.96),
    "on_track": (0.68, 0.83),
    "at_risk":  (0.40, 0.53),
    "behind":   (0.16, 0.34),
    "blocked":  (0.35, 0.50),
    "planned":  (0.00, 0.06),
}
PI_PLAN = [
    ("PI 25.4", ["done"] * 10),
    ("PI 26.1", ["done"] * 4 + ["ahead"] * 5 + ["on_track"] * 8
                + ["at_risk"] * 4 + ["behind"] * 2 + ["blocked"]),
    ("PI 26.2", ["planned"] * 11),
]

POINTS = [1, 2, 3, 3, 5, 5, 8, 13]
DONE_SNAP_WEIGHTS = [2, 3, 4, 5, 6, 6, 5, 5, 4, 3]  # gentle S-curve across the window


def sprint_windows(pi: str) -> list[tuple[str, date, date]]:
    """Sprint calendar for a PI: 26.1 → [("26.1.1", start, end), …, ("26.1.IP", …)]."""
    start = PIS[pi][0]
    ver = pi.removeprefix("PI ")
    out = []
    for i in range(5):
        s = start + timedelta(weeks=2 * i)
        out.append((f"{ver}.{i + 1}", s, s + timedelta(days=13)))
    ip_start = start + timedelta(weeks=10)
    out.append((f"{ver}.IP", ip_start, ip_start + timedelta(days=20)))
    return out


def sprint_for(pi: str, when: date) -> str:
    for ver, s, e in sprint_windows(pi):
        if s <= when <= e:
            return ver
    wins = sprint_windows(pi)
    return wins[0][0] if when < wins[0][1] else wins[-1][0]


def sprint_sort_key(ver: str) -> tuple:
    major, minor, part = ver.split(".")
    return (int(major), int(minor), 99 if part == "IP" else int(part))


def build_dataset(seed: int) -> dict:
    rng = random.Random(seed)

    # Stable people per team so assignees repeat believably.
    names = [f"{f} {l}" for f, l in zip(rng.sample(FIRST, 24), rng.sample(LAST, 20) + rng.sample(LAST, 4))]
    rng.shuffle(names)
    roster: dict[str, list[str]] = {}
    i = 0
    for cfg in PROGRAMS.values():
        for team in cfg["teams"]:
            roster[team] = names[i:i + 3]
            i += 3

    # Unique feature names per program: adjective × domain noun.
    name_pool = {
        key: rng.sample([f"{a} {n}" for a in ADJECTIVES for n in cfg["nouns"]], 40)
        for key, cfg in PROGRAMS.items()
    }

    features: list[dict] = []
    stories: list[dict] = []
    feat_seq = 1400
    epic_seq: dict[str, int] = {k: 100 for k in PROGRAMS}
    story_seq: dict[str, int] = {k: 2400 for k in PROGRAMS}
    prog_cycle = [k for k in PROGRAMS for _ in PROGRAMS[k]["teams"]]  # weight by team count

    for pi, profiles in PI_PLAN:
        for n, profile in enumerate(profiles):
            prog_key = prog_cycle[(len(features) + n) % len(prog_cycle)]
            cfg = PROGRAMS[prog_key]
            team = rng.choice(cfg["teams"])
            feat_seq += rng.randint(1, 3)
            if len(features) % 3 == 0:
                epic_seq[prog_key] += 1
            feature = {
                "key": f"FEAT-{feat_seq}",
                "id": str(700000 + feat_seq),
                "name": name_pool[prog_key].pop(),
                "epic": f"{prog_key}-E{epic_seq[prog_key]}",
                "prog": prog_key,
                "progName": cfg["name"],
                "team": team,
                "pi": pi,
                "comp": rng.choice(cfg["components"]),
                "fixVersion": FIX_VERSIONS[pi],
                "profile": profile,
            }
            fi = len(features)
            features.append(feature)

            n_stories = rng.randint(4, 8) if profile == "planned" else rng.randint(6, 14)
            pts = [rng.choice(POINTS) for _ in range(n_stories)]
            total = sum(pts)
            target = rng.uniform(*PROFILES[profile])

            order = list(range(n_stories))
            rng.shuffle(order)
            done_set, done_pts = set(), 0
            for idx in order:
                if done_pts / total >= target:
                    break
                done_set.add(idx)
                done_pts += pts[idx]

            n_blocked = rng.randint(2, 3) if profile == "blocked" else (
                1 if profile in ("at_risk", "behind") and rng.random() < 0.4 else 0)
            todo = [i2 for i2 in range(n_stories) if i2 not in done_set]
            rng.shuffle(todo)
            blocked_set = set(todo[:n_blocked])

            for si in range(n_stories):
                story_seq[prog_key] += rng.randint(1, 4)
                key = f"{prog_key}-{story_seq[prog_key]}"
                is_done = si in done_set

                # Created / done snapshot indices drive burnup + status history.
                if pi == "PI 25.4":
                    c = 0
                    d = 0 if is_done else None
                    created = PIS[pi][0] + timedelta(days=rng.randint(-21, 30))
                elif pi == "PI 26.2":
                    c = rng.randint(7, 9)
                    d = c if is_done else None  # early refinement spike
                    created = SNAPSHOTS[c] - timedelta(days=rng.randint(0, 6))
                else:
                    c = 0 if rng.random() < 0.82 else rng.randint(1, 6)
                    d = None
                    if is_done:
                        d = max(c, rng.choices(range(10), weights=DONE_SNAP_WEIGHTS)[0])
                    created = (PIS[pi][0] - timedelta(days=rng.randint(0, 28)) if c == 0
                               else SNAPSHOTS[c] - timedelta(days=rng.randint(0, 6)))

                if is_done:
                    status = "Done"
                elif si in blocked_set:
                    status = "Blocked"
                elif profile == "planned":
                    status = "In Progress" if si == 0 and rng.random() < 0.3 else "Open"
                else:
                    status = rng.choices(["In Progress", "In Review", "Open"],
                                         weights=[38, 14, 48])[0]

                if d is not None:
                    if pi == "PI 25.4":
                        resolved = created + timedelta(days=rng.randint(10, 60))
                        resolved = min(resolved, PIS[pi][1])
                    else:
                        resolved = SNAPSHOTS[d] - timedelta(days=rng.randint(0, 6))
                        resolved = max(resolved, created + timedelta(days=1))
                    sprint = sprint_for(pi, resolved)
                else:
                    resolved = None
                    active = sprint_for(pi, CURRENT_SNAPSHOT) if pi != "PI 26.2" else None
                    sprint = (rng.choice(["26.2.1", "26.2.1", "26.2.2"]) if pi == "PI 26.2"
                              else (active if status != "Open" or rng.random() < 0.6
                                    else sprint_windows(pi)[-1][0]))

                stories.append({
                    "k": key,
                    "f": fi,
                    "st": status,
                    "pts": pts[si],
                    "sp": sprint,
                    "as": rng.choice(roster[team]),
                    "pr": rng.choices(["Low", "Medium", "High", "Critical"],
                                      weights=[15, 55, 25, 5])[0],
                    "ty": rng.choices(["Story", "Task", "Bug"], weights=[72, 18, 10])[0],
                    "c": c,
                    "d": d,
                    "res": "Fixed" if is_done else None,
                    "cr": created.isoformat(),
                    "rs": resolved.isoformat() if resolved else None,
                    "lb": ";".join(x for x in {rng.choice(LABEL_POOL)} if x) or None,
                })

    # Sprint range per feature — the AgileSprintRange concept (IP sorts last).
    for fi, feature in enumerate(features):
        vers = sorted({s["sp"] for s in stories if s["f"] == fi}, key=sprint_sort_key)
        feature["minSprint"], feature["maxSprint"] = vers[0], vers[-1]
        feature.pop("profile")

    return {
        "meta": {
            "generatedAt": GENERATED_AT.isoformat(sep=" ", timespec="minutes"),
            "currentSnapshot": CURRENT_SNAPSHOT.isoformat(),
            "snapshots": [s.isoformat() for s in SNAPSHOTS],
            "statuses": STATUSES,
            "pis": {
                pi: {"start": w[0].isoformat(), "end": w[1].isoformat(),
                     "sprints": [{"v": v, "s": s.isoformat(), "e": e.isoformat()}
                                 for v, s, e in sprint_windows(pi)]}
                for pi, w in PIS.items()
            },
            "programs": {k: {"name": c["name"], "teams": c["teams"]}
                         for k, c in PROGRAMS.items()},
        },
        "features": features,
        "stories": stories,
    }


# ---------------------------------------------------------------------------
# CSV extract — mirrors the shape of STORIES.hyper: weekly history snapshots
# unioned with a current summary row per story, Title Case columns included.
# ---------------------------------------------------------------------------
CSV_COLUMNS = [
    "Story Number", "Feature Id", "Feature Key", "Epic Key", "Project Name",
    "Project Key", "Status", "Resolution", "Issue Type", "Priority", "Assignee",
    "Reporter", "Labels", "Components", "Sprint Name", "Fix Version",
    "Program Increment", "Snapshot Date", "Created Date", "Updated Date",
    "Resolved Date", "Due Date", "Begin Date", "End Date", "Story Points",
    "Original Estimate", "Remaining Estimate", "Time Spent", "Is Synthetic",
    "Last Updated", "Project Name Version", "Sprint Name Alt",
    "Snapshot Date Alt", "Pi From Sprint",
]


def status_at(story: dict, snap_idx: int) -> str | None:
    """Workflow state of a story at a given snapshot (None = not created yet)."""
    if snap_idx < story["c"]:
        return None
    d = story["d"]
    if d is not None:
        if snap_idx >= d:
            return "Done"
        if snap_idx == d - 1 and d - 1 >= story["c"]:
            return "In Review"
        if snap_idx == d - 2 and d - 2 >= story["c"]:
            return "In Progress"
        return "Open"
    current = story["st"]
    lead = {"In Progress": 2, "In Review": 1, "Blocked": 2}.get(current, 0)
    if current != "Open" and snap_idx >= max(story["c"], len(SNAPSHOTS) - 1 - lead):
        return current if snap_idx > len(SNAPSHOTS) - 2 - lead else "In Progress"
    return "Open"


def csv_rows(data: dict, rng: random.Random) -> list[dict]:
    rows = []
    reporters = {f["team"]: f"{rng.choice(FIRST)} {rng.choice(LAST)}"
                 for f in data["features"]}
    for story in data["stories"]:
        feature = data["features"][story["f"]]
        pi = feature["pi"]
        windows = {v: (s, e) for v, s, e in sprint_windows(pi)}
        begin, end = windows.get(story["sp"], (None, None))
        base = {
            "Story Number": story["k"],
            "Feature Id": feature["id"],
            "Feature Key": feature["key"],
            "Epic Key": feature["epic"],
            "Project Name": feature["progName"],
            "Project Key": feature["prog"],
            "Resolution": story["res"] or "",
            "Issue Type": story["ty"],
            "Priority": story["pr"],
            "Assignee": story["as"],
            "Reporter": reporters[feature["team"]],
            "Labels": story["lb"] or "",
            "Components": feature["comp"],
            "Sprint Name": f"{feature['team']} {story['sp']}",
            "Fix Version": feature["fixVersion"],
            "Program Increment": pi,
            "Created Date": story["cr"],
            "Resolved Date": story["rs"] or "",
            "Due Date": end.isoformat() if end else "",
            "Begin Date": begin.isoformat() if begin else "",
            "End Date": end.isoformat() if end else "",
            "Story Points": story["pts"],
            "Original Estimate": story["pts"] * 6.0,
            "Last Updated": GENERATED_AT.isoformat(sep=" "),
            "Project Name Version": f"{feature['progName']} {feature['fixVersion']}",
            "Sprint Name Alt": story["sp"],
            "Pi From Sprint": story["sp"][:4],
        }
        snapshots: list[tuple[str, str, bool]] = [
            (SNAPSHOTS[i].isoformat(), st, False)
            for i in range(len(SNAPSHOTS))
            if (st := status_at(story, i)) is not None
        ]
        # Current summary row — the pipeline stamps null snapshots with "now".
        snapshots.append((GENERATED_AT.date().isoformat(), story["st"], True))
        for snap_date, st, is_summary in snapshots:
            done = st == "Done"
            remaining = 0.0 if done else round(base["Original Estimate"]
                                               * rng.uniform(0.2, 0.9), 1)
            rows.append(base | {
                "Status": st,
                "Snapshot Date": snap_date,
                "Updated Date": story["rs"] or snap_date,
                "Remaining Estimate": remaining,
                "Time Spent": round(base["Original Estimate"] - remaining, 1),
                "Is Synthetic": False,
                "Snapshot Date Alt": (GENERATED_AT.isoformat(sep=" ")
                                      if is_summary else snap_date),
            })
    return rows


def inject_into_html(payload: str) -> bool:
    if not INDEX_HTML.exists():
        return False
    html = INDEX_HTML.read_text(encoding="utf-8")
    pattern = re.compile(
        r'(<script id="stories-data" type="application/json">).*?(</script>)',
        re.DOTALL,
    )
    if not pattern.search(html):
        return False
    safe = payload.replace("</", r"<\/")
    INDEX_HTML.write_text(pattern.sub(lambda m: m.group(1) + safe + m.group(2), html),
                          encoding="utf-8")
    return True


def export_hyper(rows: list[dict]) -> bool:
    """Write a real .hyper like the pipeline would — only if deps are present."""
    try:
        import pantab  # noqa: F401
        import polars as pl
    except ImportError as exc:
        print(f"  ~ skipped .hyper export ({exc.name} not installed; "
              f"pip install polars pantab)")
        return False
    HYPER_PATH.parent.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame(rows, infer_schema_length=None)
    pantab.frame_to_hyper(df, HYPER_PATH, table="Stories")
    print(f"  ✓ {HYPER_PATH.relative_to(REPO_ROOT)}  ({df.height:,} rows)")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--seed", type=int, default=26, help="RNG seed (default: 26)")
    parser.add_argument("--hyper", action="store_true",
                        help="Also export output/STORIES_MOCK.hyper (needs polars + pantab)")
    args = parser.parse_args()

    data = build_dataset(args.seed)
    rows = csv_rows(data, random.Random(args.seed + 1))

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(data, separators=(",", ":"))
    (DATA_DIR / "stories_mock.json").write_text(payload + "\n", encoding="utf-8")

    with (DATA_DIR / "stories_mock.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)

    n_done = sum(1 for s in data["stories"] if s["st"] == "Done")
    print(f"Mock STORIES extract (seed {args.seed}):")
    print(f"  ✓ {len(data['features'])} features · {len(data['stories'])} stories "
          f"({n_done} done) · {len(SNAPSHOTS)} weekly snapshots")
    print(f"  ✓ dashboard/data/stories_mock.json  ({len(payload) / 1024:.0f} KB)")
    print(f"  ✓ dashboard/data/stories_mock.csv   ({len(rows):,} rows)")

    if inject_into_html(payload):
        print("  ✓ dashboard/index.html — embedded data refreshed")
    else:
        print("  ~ dashboard/index.html not found or missing data markers; skipped")

    if args.hyper:
        export_hyper(rows)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
