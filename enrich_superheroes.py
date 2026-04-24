import requests
import pandas as pd
import json
import time
import sys
from pathlib import Path


SUPERHERO_API_TOKEN = "b441bcc091a1248a5d85414037f0cd73"   

OUTPUT_FILE   = "superheroes_full.csv"
CACHE_FILE    = "scrape_cache.json"
TOTAL_HEROES  = 731         
RATE_DELAY    = 0.25        
SAVE_EVERY    = 50           # save cache every 50 heroes

BASE_URL = f"https://superheroapi.com/api/{SUPERHERO_API_TOKEN}"


def load_cache() -> dict:
    if Path(CACHE_FILE).exists():
        with open(CACHE_FILE, "r") as f:
            return json.load(f)
    return {}


def save_cache(cache: dict):
    with open(CACHE_FILE, "w") as f:
        json.dump(cache, f, indent=2)



# Getting the api
def fetch_hero(hero_id: int) -> dict | None:
    """Fetch a single hero by ID. Returns raw API dict or None on failure."""
    url = f"{BASE_URL}/{hero_id}"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        if data.get("response") == "success":
            return data
        else:
            print(f"  ⚠  ID {hero_id}: API returned error — {data.get('error', 'unknown')}")
            return None
    except requests.exceptions.Timeout:
        print(f"  ⚠  ID {hero_id}: Request timed out")
        return None
    except requests.exceptions.RequestException as e:
        print(f"  ⚠  ID {hero_id}: Request failed — {e}")
        return None
    finally:
        time.sleep(RATE_DELAY)


#parsing the stats
def parse_cm(height_list: list) -> float | None:
    """Extract numeric cm value from height list like ['6\\'2"', '188 cm']."""
    if not isinstance(height_list, list):
        return None
    for h in height_list:
        if "cm" in str(h):
            try:
                return float(str(h).replace("cm", "").strip())
            except ValueError:
                pass
    return None


def parse_kg(weight_list: list) -> float | None:
    """Extract numeric kg value from weight list like ['181 lb', '82 kg']."""
    if not isinstance(weight_list, list):
        return None
    for w in weight_list:
        if "kg" in str(w):
            try:
                return float(str(w).replace("kg", "").strip())
            except ValueError:
                pass
    return None


def safe_int(val) -> int | None:
    """Convert a stat string to int; return None for 'null' or invalid."""
    try:
        v = int(val)
        return v if v >= 0 else None
    except (TypeError, ValueError):
        return None


def clean_str(val) -> str | None:
    """Return None for empty/placeholder strings."""
    if val is None:
        return None
    s = str(val).strip()
    if s.lower() in ("", "-", "null", "none", "no alter egos found.",
                     "no aliases", "unknown", "n/a"):
        return None
    return s


def parse_hero(raw: dict) -> dict:
    """Flatten a raw API hero record into a clean row dict."""
    ps   = raw.get("powerstats", {})
    bio  = raw.get("biography", {})
    app  = raw.get("appearance", {})
    work = raw.get("work", {})
    conn = raw.get("connections", {})
    img  = raw.get("image", {})

    # Power stats
    intel  = safe_int(ps.get("intelligence"))
    strn   = safe_int(ps.get("strength"))
    speed  = safe_int(ps.get("speed"))
    dur    = safe_int(ps.get("durability"))
    power  = safe_int(ps.get("power"))
    combat = safe_int(ps.get("combat"))
    stats  = [intel, strn, speed, dur, power, combat]
    total  = sum(stats) if all(v is not None for v in stats) else None

    aliases_raw = bio.get("aliases", [])
    if isinstance(aliases_raw, list):
        aliases = "; ".join(a for a in aliases_raw if clean_str(a)) or None
    else:
        aliases = clean_str(aliases_raw)

    # height and weight
    height_cm = parse_cm(app.get("height", []))
    weight_kg = parse_kg(app.get("weight", []))

    # Alignment capitalised
    alignment_raw = clean_str(bio.get("alignment"))
    alignment = alignment_raw.capitalize() if alignment_raw else None

    return {
        # Identifiers
        "ID":               int(raw.get("id", 0)),
        "Name":             clean_str(raw.get("name")),

        # stats
        "Intelligence":     intel,
        "Strength":         strn,
        "Speed":            speed,
        "Durability":       dur,
        "Power":            power,
        "Combat":           combat,
        "Total":            total,

        #biography
        "FullName":         clean_str(bio.get("full-name")),
        "AlterEgos":        clean_str(bio.get("alter-egos")),
        "Aliases":          aliases,
        "PlaceOfBirth":     clean_str(bio.get("place-of-birth")),
        "FirstAppearance":  clean_str(bio.get("first-appearance")),
        "Publisher":        clean_str(bio.get("publisher")),
        "Alignment":        alignment,

        #appearance
        "Gender":           clean_str(app.get("gender")),
        "Race":             clean_str(app.get("race")),
        "Height_cm":        height_cm,
        "Weight_kg":        weight_kg,
        "EyeColor":         clean_str(app.get("eye-color")),
        "HairColor":        clean_str(app.get("hair-color")),
        "Occupation":       clean_str(work.get("occupation")),
        "Base":             clean_str(work.get("base")),
        "GroupAffiliation": clean_str(conn.get("group-affiliation")),
        "Relatives":        clean_str(conn.get("relatives")),

        #Image
        "ImageURL":         clean_str(img.get("url")),
    }

def main():
    if SUPERHERO_API_TOKEN == "YOUR_TOKEN_HERE":
        print("  Please set your SUPERHERO_API_TOKEN at the top of the script.")
        print("    Get a free token at: https://superheroapi.com/")
        sys.exit(1)

    print("=" * 55)
    print("  Superhero API — Full Dataset Scraper")
    print("=" * 55)
    print(f"  Target  : {TOTAL_HEROES} heroes (IDs 1–{TOTAL_HEROES})")
    print(f"  Output  : {OUTPUT_FILE}")
    print(f"  Cache   : {CACHE_FILE}")
    print("=" * 55 + "\n")

    cache = load_cache()
    already_done = len(cache)
    if already_done:
        print(f"💾  Resuming — {already_done} heroes already cached\n")

    heroes = []
    failed_ids = []

    for hero_id in range(1, TOTAL_HEROES + 1):
        id_str = str(hero_id)

        # use cache if available
        if id_str in cache:
            raw = cache[id_str]
        else:
            print(f"  Fetching [{hero_id:>3}/{TOTAL_HEROES}] ...", end=" ", flush=True)
            raw = fetch_hero(hero_id)
            if raw:
                cache[id_str] = raw
                print(f"✓  {raw.get('name', '?')}")
            else:
                cache[id_str] = None
                failed_ids.append(hero_id)
                print("✗  failed")

            #periodic cache save
            if hero_id % SAVE_EVERY == 0:
                save_cache(cache)
                print(f"\n  💾 Progress saved ({hero_id}/{TOTAL_HEROES})\n")

        if raw:
            heroes.append(parse_hero(raw))

    # Final cache save
    save_cache(cache)

    # Build DataFrame
    df = pd.DataFrame(heroes)
    df = df.sort_values("ID").reset_index(drop=True)

    # Save to csv
    df.to_csv(OUTPUT_FILE, index=False)

    print("\n" + "=" * 55)
    print("  ✅  Scrape Complete!")
    print("=" * 55)
    print(f"  Heroes collected : {len(df):,}")
    print(f"  Failed IDs       : {len(failed_ids)} {failed_ids if failed_ids else ''}")
    print(f"  Output file      : {OUTPUT_FILE}")
    print(f"  Columns          : {df.shape[1]}")
    print()
    print("  Missing values per column:")
    for col in df.columns:
        missing = df[col].isnull().sum()
        if missing > 0:
            pct = missing / len(df) * 100
            print(f"    {col:<20} {missing:>4} missing ({pct:.1f}%)")
    print()
    print("  Publisher breakdown:")
    pub_counts = df["Publisher"].value_counts().head(10)
    for pub, count in pub_counts.items():
        print(f"    {pub:<30} {count:>4}")
    print()
    print("  Alignment breakdown:")
    for align, count in df["Alignment"].value_counts().items():
        print(f"    {align:<15} {count:>4}")


if __name__ == "__main__":
    main()