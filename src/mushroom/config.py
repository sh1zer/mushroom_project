import os
from pathlib import Path

PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).resolve().parents[2]))

RAW_DIR = PROJECT_ROOT / "data" / "raw"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
CHECKPOINT_DIR = PROJECT_ROOT / "checkpoints"
MODELS_DIR = PROJECT_ROOT / "models"
REPORTS_DIR = PROJECT_ROOT / "reports"

SECONDARY_CSV = RAW_DIR / "secondary_data.csv"

MUSHROOMS_PARQUET = PROCESSED_DIR / "mushrooms.parquet"
ANALYSIS_REPORT = REPORTS_DIR / "analysis.json"
METRICS_REPORT = REPORTS_DIR / "metrics.json"
BEST_MODEL_DIR = MODELS_DIR / "best_model"

ROWS_PER_SPECIES = 353
MISSING = "missing"
SEED = 42

NUMERIC_FEATURES = ["cap_diameter", "stem_height", "stem_width"]

CATEGORICAL_FEATURES = [
    "cap_shape",
    "cap_surface",
    "cap_color",
    "does_bruise_or_bleed",
    "gill_attachment",
    "gill_spacing",
    "gill_color",
    "stem_root",
    "stem_surface",
    "stem_color",
    "veil_type",
    "veil_color",
    "has_ring",
    "ring_type",
    "spore_print_color",
    "habitat",
    "season",
]

FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9094")
TOPIC_OBSERVATIONS = "mushroom-observations"
TOPIC_PREDICTIONS = "mushroom-predictions"
SAFETY_THRESHOLD = 0.4

COLOR_CODES = {
    "n": "brązowy", "b": "płowy", "g": "szary", "r": "zielony", "p": "różowy",
    "u": "fioletowy", "e": "czerwony", "w": "biały", "y": "żółty", "l": "niebieski",
    "o": "pomarańczowy", "k": "czarny", "f": "brak",
}
SURFACE_CODES = {
    "i": "włóknista", "g": "bruzdowana", "y": "łuskowata", "s": "gładka", "h": "błyszcząca",
    "l": "skórzasta", "k": "jedwabista", "t": "lepka", "w": "pomarszczona", "e": "mięsista",
    "f": "brak",
}

CODEBOOK = {
    "cap_shape": {"b": "dzwonkowaty", "c": "stożkowaty", "x": "wypukły", "f": "płaski",
                  "s": "wklęsły", "p": "kulisty", "o": "inny"},
    "cap_surface": {**SURFACE_CODES, "d": "kod 'd' (nieopisany w metadanych)"},
    "cap_color": COLOR_CODES,
    "does_bruise_or_bleed": {"t": "tak", "f": "nie"},
    "gill_attachment": {"a": "przyrośnięte", "x": "zatokowato wycięte", "d": "zbiegające",
                        "e": "wolne", "s": "falisto wycięte", "p": "rurki (pory)", "f": "brak",
                        "?": "nieznane"},
    "gill_spacing": {"c": "gęste", "d": "rzadkie", "f": "brak"},
    "gill_color": COLOR_CODES,
    "stem_root": {"b": "bulwiasta", "s": "zgrubiała", "c": "maczugowata", "u": "pochwa",
                  "e": "równa", "z": "ryzomorfy", "r": "korzeniasta", "f": "brak"},
    "stem_surface": SURFACE_CODES,
    "stem_color": COLOR_CODES,
    "veil_type": {"p": "częściowa", "u": "całkowita"},
    "veil_color": COLOR_CODES,
    "has_ring": {"t": "tak", "f": "nie"},
    "ring_type": {"c": "pajęczynowaty", "e": "nietrwały", "r": "rozszerzony", "g": "bruzdowany",
                  "l": "duży", "p": "zwisający", "s": "pochwiasty", "z": "strefowy",
                  "y": "łuskowaty", "m": "ruchomy", "f": "brak", "?": "nieznany"},
    "spore_print_color": COLOR_CODES,
    "habitat": {"g": "trawy", "l": "liście", "m": "łąki", "p": "ścieżki", "h": "wrzosowiska",
                "u": "tereny miejskie", "w": "nieużytki", "d": "lasy"},
    "season": {"s": "wiosna", "u": "lato", "a": "jesień", "w": "zima"},
}

FEATURE_LABELS = {
    "cap_diameter": "Średnica kapelusza [cm]",
    "stem_height": "Wysokość trzonu [cm]",
    "stem_width": "Szerokość trzonu [mm]",
    "cap_shape": "Kształt kapelusza",
    "cap_surface": "Powierzchnia kapelusza",
    "cap_color": "Kolor kapelusza",
    "does_bruise_or_bleed": "Siniaczenie / wydzielanie soku",
    "gill_attachment": "Przyczep blaszek",
    "gill_spacing": "Rozstaw blaszek",
    "gill_color": "Kolor blaszek",
    "stem_root": "Nasada trzonu",
    "stem_surface": "Powierzchnia trzonu",
    "stem_color": "Kolor trzonu",
    "veil_type": "Typ osłony",
    "veil_color": "Kolor osłony",
    "has_ring": "Pierścień",
    "ring_type": "Typ pierścienia",
    "spore_print_color": "Kolor wysypu zarodników",
    "habitat": "Siedlisko",
    "season": "Pora roku",
}
