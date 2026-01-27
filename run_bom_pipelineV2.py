#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_bom_pipeline.py

One-command BOM cleaning + normalization pipeline (rule-based, Notebook V3 logic),
optionally enriched with DistilBERT NER inference outputs.

This script is designed to be handed over to another engineering team.
It is intentionally verbose with comments and clear separation of concerns.

What it does (high level)
-------------------------
1) Read a customer BOM Excel (header row may NOT be the first row).
2) Normalize column names (only standardizes the Description field into `description_raw`).
   - All original columns are preserved "as-is".
3) Rule-based normalization (ported from BOM_pipeline0923V3.ipynb):
   - Build a normalized base text (_BASE_) for robust regex matching
   - Classify part category (RES/CAP/IC/CN/ID/OT)
   - Extract key specs by regex
   - Build "正規化Description" and "顯示名20"
   - Produce a "群組彙總" sheet for "same PN different names" checks
4) Optional: run NER model inference (DistilBERT token classification):
   - Adds NER_Result and Vendor_Name_Model columns
5) Output:
   - <stem>_final.xlsx (Main sheet + Group summary)
   - <stem>_AUTO.xlsx  (rows that pass basic validation)
   - <stem>_REVIEW.xlsx (rows needing human review)

Usage
-----
Recommended (explicit input path):

    python run_bom_pipeline.py --input "BOM.xlsx" --out_dir "outputs"

If the sheet name is unknown (common), omit --sheet (default: auto-detect).

Optional: include model inference outputs (DistilBERT NER):

    python run_bom_pipeline.py --input "BOM.xlsx" --out_dir "outputs" --model_dir "distilbert_ner_final"

Convenience: you may also pass the input path as a *positional* argument:

    python run_bom_pipeline.py "BOM.xlsx"

Notes
-----
- This script keeps "rule normalization" as the source of truth for normalized description.
- NER output is attached as additional evidence; you can later enhance the merge strategy.

Author: (handoff)
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import json
import re
import unicodedata
from typing import Dict, List, Optional, Tuple


def pick_excel_file_via_dialog(initial_dir: Path) -> "Path | None":
    """Open a native file picker dialog to select an Excel file.

    Returns
    -------
    Path | None
        - Path to the selected file, or
        - None if user cancels / GUI not available.

    Notes for maintainers
    ---------------------
    - We keep this function small and dependency-free (tkinter is part of the
      standard library on most Python installs).
    - On some minimal Python distributions, tkinter may not be installed.
      In that case we fall back to requiring --input.
    """
    try:
        import tkinter as tk
        from tkinter import filedialog
    except Exception as e:
        print("[WARN] tkinter not available; cannot open file picker. "
              "Install tkinter or use --input with a file path.")
        return None

    root = tk.Tk()
    root.withdraw()  # hide root window
    root.attributes("-topmost", True)

    file_path = filedialog.askopenfilename(
        initialdir=str(initial_dir),
        title="Select BOM Excel file",
        filetypes=[
            ("Excel files", "*.xlsx *.xlsm *.xls"),
            ("All files", "*.*"),
        ],
    )
    try:
        root.destroy()
    except Exception:
        pass

    if not file_path:
        return None
    return Path(file_path)

import pandas as pd


# -----------------------------------------------------------------------------
# 0) Excel reading helpers
# -----------------------------------------------------------------------------
def _normalize_colname(x: object) -> str:
    """Normalize a column name for matching only (does NOT change the source data)."""
    s = "" if x is None else str(x)
    s = s.replace("\n", " ")
    s = " ".join(s.split())
    return s.strip()


def _detect_header_row_in_sheet(
    path: Path,
    sheet_name: str | int,
    header_search_rows: int = 50,
    required_col_regex: str = r"^description$",
) -> Optional[int]:
    """Return the header row index if a header row containing 'Description' is found."""
    df_raw = pd.read_excel(path, sheet_name=sheet_name, header=None, nrows=header_search_rows)
    target_re = re.compile(required_col_regex, re.I)
    for r in range(df_raw.shape[0]):
        row_vals = df_raw.iloc[r].tolist()
        for v in row_vals:
            if v is None:
                continue
            if target_re.search(_normalize_colname(v)):
                return r
    return None


def read_excel_auto_detect_sheet_and_header(
    path: Path,
    sheet: Optional[str] = None,
    header_search_rows: int = 50,
    required_col_regex: str = r"^description$",
) -> Tuple[pd.DataFrame, str]:
    """
    Read an Excel BOM where:
      - The target sheet is *usually* the first sheet, but the name may vary.
      - The header row may not be the first row.

    Behavior:
      1) If `sheet` is provided: only scan that sheet.
      2) Else: scan sheets in order and pick the first sheet that contains a row
         with a cell matching `required_col_regex` (default: '^description$').
      3) If no sheet contains 'Description' within the first `header_search_rows` rows,
         fall back to the first sheet with header=0.

    Returns:
      (df, chosen_sheet_name)
    """
    xls = pd.ExcelFile(path)
    sheet_names = xls.sheet_names
    if not sheet_names:
        raise ValueError(f"No sheets found in Excel: {path}")

    # Decide which sheets to scan
    scan_sheets: List[str] = [sheet] if sheet else list(sheet_names)

    chosen_sheet = scan_sheets[0]
    chosen_header_row: Optional[int] = None

    for sh in scan_sheets:
        hdr = _detect_header_row_in_sheet(
            path=path,
            sheet_name=sh,
            header_search_rows=header_search_rows,
            required_col_regex=required_col_regex,
        )
        if hdr is not None:
            chosen_sheet = sh
            chosen_header_row = hdr
            break

    if chosen_header_row is None:
        # Fall back to first sheet, header=0
        chosen_sheet = sheet if sheet else sheet_names[0]
        df = pd.read_excel(path, sheet_name=chosen_sheet, header=0)
    else:
        df = pd.read_excel(path, sheet_name=chosen_sheet, header=chosen_header_row)

    df.columns = [_normalize_colname(c) for c in df.columns]
    return df, chosen_sheet


# -----------------------------------------------------------------------------
# 1) Text normalization utilities (ported from BOM_pipeline0923V3.ipynb)
# -----------------------------------------------------------------------------
def nfkc(s: str) -> str:
    """Unicode NFKC normalization."""
    return unicodedata.normalize("NFKC", "" if s is None else str(s))


def upper_ascii(s: str) -> str:
    """
    Normalize common BOM symbols and return uppercase ASCII-ish text.

    Examples:
    - µ -> u
    - Ω -> O
    - ％ -> %
    """
    t = nfkc(s)
    t = (
        t.replace("µ", "u")
         .replace("Ω", "O")
         .replace("％", "%")
         .replace("＋", "+")
         .replace("－", "-")
    )
    t = t.replace("+/-", "±")
    t = t.replace("≦", "≤").replace("<=", "≤").replace("＜=", "≤")
    t = t.replace("℃", "°C")
    t = re.sub(r"\s+", " ", t)
    return t.upper().strip()


# Words to drop in base normalization (ported list; tune as needed)
DROP_WORDS = [
    "PLEASE", "W/", "WITH", "WITHOUT", "SMD", "SMT", "TH", "T/H",
    "GENERIC", "GENERAL", "STANDARD", "TYPE", "KIND",
]

def normalize_base_text(desc: str) -> str:
    """
    Create a robust base string for regex extraction:
    - Normalize unicode and common symbols
    - Remove parentheses content
    - Convert OHM/OHMS -> O
    - Replace separators , | / with spaces
    - Drop some noisy words
    - Collapse whitespace
    """
    s = upper_ascii(desc)

    # Remove (...) content, keep outside text
    s = re.sub(r"\([^)]*\)", " ", s)
    s = re.sub(r"（[^）]*）", " ", s)

    # OHM/OHMS -> O
    s = re.sub(r"\bOHMS?\b", "O", s)

    # separators
    s = s.replace(",", " ").replace("|", " ").replace("/", " ")

    # drop noisy words
    for w in DROP_WORDS:
        s = re.sub(rf"\b{re.escape(w)}\b", " ", s)

    s = re.sub(r"\s+", " ", s).strip()
    return s


def find_col(df: pd.DataFrame, patterns: List[str]) -> Optional[str]:
    """Find a column whose name matches any regex in `patterns`."""
    for c in df.columns:
        for p in patterns:
            if re.search(p, str(c), re.I):
                return c
    return None


# -----------------------------------------------------------------------------
# 2) Category classification (ported from BOM_pipeline0923V3.ipynb)
# -----------------------------------------------------------------------------
# IMPORTANT (handoff note)
# ------------------------
# The *category strings* are customer-facing outputs used in the normalized BOM.
# Keep them stable, consistent, and human-readable.
#
# In earlier iterations, inductors were incorrectly labeled as "ID" (confusable
# with diodes). The user request is to show inductors explicitly as "IND".
#
# If your company later wants more fine-grained categories (e.g., DIODE vs IND),
# extend `CAT_RULES` and the downstream logic will work without further changes.

# Display prefix used by `顯示名20`. To avoid confusing abbreviations, we use the
# category itself as prefix.
PREFIX = {"RES": "RES", "CAP": "CAP", "IND": "IND", "IC": "IC", "CN": "CN", "OT": "OT"}

# Category keywords (ported; extend/tune per company)
CAT_RULES = [
    ("RES", [r"\bRES\b", r"\bRESIST", r"\bOHM\b", r"\b[0-9]+R[0-9]*\b", r"\b[0-9.]+K\b", r"\b[0-9.]+M\b"]),
    ("CAP", [r"\bCAP\b", r"\bCAPAC", r"\bUF\b", r"\bNF\b", r"\bPF\b"]),
    # Inductor / Ferrite bead
    ("IND", [r"\bIND\b", r"\bINDUCT", r"\bFERRITE\b", r"\bBEAD\b", r"\bUH\b", r"\bNH\b", r"\bMH\b"]),
    ("IC",  [r"\bIC\b", r"\bMCU\b", r"\bDRIVER\b", r"\bREGULATOR\b", r"\bOPAMP\b", r"\bAMPLIFIER\b"]),
    ("CN",  [r"\bCONN\b", r"\bCONNECT", r"\bHEADER\b", r"\bSOCKET\b", r"\bUSB\b", r"\bRJ45\b"]),
]


def classify(base_text: str) -> str:
    """Return a coarse category label based on keyword rules."""
    s = base_text
    for cat, pats in CAT_RULES:
        for p in pats:
            if re.search(p, s, re.I):
                return cat
    return "OT"


def pick_category(row: pd.Series) -> str:
    """Pick category from _BASE_ or existing column if present."""
    base = row.get("_BASE_", "")
    return classify(str(base))


# -----------------------------------------------------------------------------
# 3) Regex extractors (ported from BOM_pipeline0923V3.ipynb; minimal core set)
#    These can be extended without changing pipeline plumbing.
# -----------------------------------------------------------------------------
# Generic measurements
RES_RE = re.compile(r"\b(\d+(?:\.\d+)?)(R|K|M)?(\d+)?\b", re.I)  # handles 4K7, 10R, 1M
CAP_RE = re.compile(r"\b(\d+(?:\.\d+)?)(U|N|P)F\b", re.I)
CAP_CODE_RE = re.compile(r"\b(\d{3})\b")  # e.g., 104 code (best-effort)
# Voltage patterns
#  - Range: 1.65~3.6V, 1.65-3.6V, 1.65 to 3.6V
#  - Single: 3.6V, 5VDC
VOLT_RANGE_RE = re.compile(
    r"\b(\d+(?:\.\d+)?)\s*(?:~|\-|TO)\s*(\d+(?:\.\d+)?)\s*V(?:DC)?\b",
    re.I,
)
VOLT_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*V(?:DC)?\b", re.I)
TOL_RE = re.compile(r"\b±?\s?(\d+(?:\.\d+)?)\s?%\b", re.I)
PWR_RE = re.compile(r"\b(\d+/\d+|\d+(?:\.\d+)?)\s?(W|MW)\b", re.I)

# Package / size
PKG_RE = re.compile(r"\b(0402|0603|0805|1206|1210|2010|2512|1608|2012|3216)\b", re.I)
PKG_WORD_RE = re.compile(r"\b(SOT-?\d+|SOD-?\d+|SOP-?\d+|SOIC-?\d+|QFN-?\d+|DFN-?\d+|TO-?\d+)\b", re.I)


def canon_pkg(s: str) -> str:
    """Canonicalize package string: remove spaces/dashes, uppercase."""
    t = upper_ascii(s)
    t = re.sub(r"[\s\-]+", "", t)
    return t


def normalize_power_token(p: str) -> str:
    """
    Normalize power token to a compact representation.
    Examples:
    - 1/8W -> 0.125W (optional)
    - 250mW -> 0.25W (optional)
    For now keep original upper form to avoid unintended conversion.
    """
    return upper_ascii(p)


def _format_resistance(token: str) -> str:
    """
    Best-effort normalize resistance:
    - 4K7 -> 4.7K
    - 10R -> 10R
    - 1M0 -> 1.0M
    """
    t = upper_ascii(token)
    m = re.fullmatch(r"(\d+(?:\.\d+)?)([RKM])(\d+)?", t)
    if not m:
        return t
    a, unit, b = m.group(1), m.group(2), m.group(3)
    if b:
        return f"{a}.{b}{unit}"
    return f"{a}{unit}"


def shortest_res_string(cands: List[str]) -> str:
    """Pick shortest resistance representation."""
    if not cands:
        return ""
    return min(cands, key=len)


def shortest_cap_string(cands: List[str]) -> str:
    """Pick shortest capacitance representation."""
    if not cands:
        return ""
    return min(cands, key=len)


def extract_generic_meas(base: str) -> Dict[str, str]:
    """
    Extract common measurements from normalized base text.
    Returns a dict of extracted fields (strings).
    """
    s = base
    out: Dict[str, str] = {}

    # Resistance (look for tokens ending with R/K/M patterns)
    res_tokens = []
    for tok in s.split():
        if re.fullmatch(r"\d+(?:\.\d+)?[RKM]\d*", tok, re.I):
            res_tokens.append(_format_resistance(tok))
    if res_tokens:
        out["阻值"] = shortest_res_string(res_tokens) + "O"  # the notebook uses 'O' as ohm suffix

    # Capacitance (e.g., 10UF/100NF/1PF)
    cap_tokens = []
    for m in CAP_RE.finditer(s):
        cap_tokens.append(f"{m.group(1)}{m.group(2).upper()}F")
    if cap_tokens:
        out["容量"] = shortest_cap_string(cap_tokens)

    # Voltage
    # Prefer range voltage (e.g., 1.65~3.6V) if present.
    m = VOLT_RANGE_RE.search(s)
    if m:
        out["電壓"] = f"{m.group(1)}~{m.group(2)}V"
    else:
        m = VOLT_RE.search(s)
        if m:
            out["電壓"] = f"{m.group(1)}V"

    # Tolerance
    m = TOL_RE.search(s)
    if m:
        out["容差"] = f"{m.group(1)}%"

    # Power
    m = PWR_RE.search(s)
    if m:
        out["功率"] = normalize_power_token(m.group(0))

    # Package / size
    m = PKG_RE.search(s)
    if m:
        out["尺寸"] = upper_ascii(m.group(1))
    m = PKG_WORD_RE.search(s)
    if m:
        out["封裝"] = canon_pkg(m.group(1))

    return out


def extract_tokens(base: str, cat: str) -> Dict[str, str]:
    """
    Category-aware extraction wrapper.
    Extend this function to add more rules per category.
    """
    out = extract_generic_meas(base)

    # Simple dielectric extraction for CAP (best-effort)
    if cat == "CAP":
        m = re.search(r"\b(C0G|NP0|X7R|X5R|Y5V)\b", base, re.I)
        if m:
            out["介質"] = m.group(1).upper()

    # Connector direction example
    if cat == "CN":
        m = re.search(r"\b(RIGHT\s?ANGLE|VERTICAL)\b", base, re.I)
        if m:
            out["方向"] = upper_ascii(m.group(1))

    return out


# -----------------------------------------------------------------------------
# 4) Build normalized description & display name (ported idea)
# -----------------------------------------------------------------------------
def build_normalized_desc(cat: str, t: Dict[str, str]) -> str:
    """
    Construct a human-readable normalized description.
    Keep it stable because it will be used for grouping/dedup in DB.

    This is a simplified port of the notebook logic:
    prefix + key fields in a fixed order.
    """
    # Use a simple de-dup mechanism to avoid repeated tokens
    # (e.g., voltage being appended twice via different extraction paths).
    parts: List[str] = []
    seen: set[str] = set()

    def _add(x: str) -> None:
        x = str(x).strip()
        if not x:
            return
        if x in seen:
            return
        parts.append(x)
        seen.add(x)

    _add(cat)

    # Common order (extend as needed)
    for k in ["阻值", "容量", "電壓", "容差", "功率", "介質", "尺寸", "封裝", "方向"]:
        _add(t.get(k, ""))

    # Any remaining fields can be appended at the end (optional)
    return " ".join(parts).strip()


def display20(cat: str, norm: str, others: Optional[Dict[str, str]] = None, min_len: int = 8, max_len: int = 20) -> str:
    """
    Build a compact display name (<=20 chars) for UI fields or quick scanning.
    Strategy:
      - Prefix by category (RES/CAP/IND/IC/CN/OT)
      - Remove spaces from tail tokens
      - If too short, backfill using other available fields
    """
    prefix = PREFIX.get(cat, "OT")
    tail = norm.split(" ", 1)[1] if " " in norm else ""
    tokens = tail.split()

    # De-duplicate tail tokens while preserving order (prevents IC3.6V3.6V style outputs).
    dedup_tokens: List[str] = []
    seen_tok: set[str] = set()
    for tok in tokens:
        if tok in seen_tok:
            continue
        dedup_tokens.append(tok)
        seen_tok.add(tok)

    base = prefix + "".join(dedup_tokens)

    if len(base) >= min_len:
        return base[:max_len]

    # backfill
    if others:
        # Backfill only with values that are NOT already present in base.
        extra_parts: List[str] = []
        for v in others.values():
            v = str(v).strip()
            if not v:
                continue
            if v in base:
                continue
            extra_parts.append(v)
        extra = "".join(extra_parts)
        base2 = (base + extra)[:max_len]
        return base2

    return base[:max_len]


def pipe_view(cat: str, t: Dict[str, str]) -> str:
    """
    A debug-friendly view string that shows which fields were extracted.
    Engineers can use it to tune rules quickly.
    """
    keys = ["阻值", "容量", "電壓", "容差", "功率", "介質", "尺寸", "封裝", "方向"]
    parts = [f"{k}={t.get(k,'')}" for k in keys if t.get(k, "")]
    return f"{cat}|" + "|".join(parts)


# -----------------------------------------------------------------------------
# 5) Optional NER inference (ported from apply_model_to_bom.py core idea)
# -----------------------------------------------------------------------------
@dataclass
class NerInferenceConfig:
    model_dir: Path
    max_len: int = 64


def _simple_tokenize_for_ner(text: str) -> List[str]:
    """
    Tokenization matching the NER training setup.
    This is a minimal version; replace with your training tokenizer if needed.
    """
    if text is None:
        return []
    # split by whitespace first
    parts = re.findall(r"\S+", str(text))
    tokens: List[str] = []
    for p in parts:
        # split into alnum+symbols chunks
        tokens.extend(re.findall(r"[A-Za-z0-9\.]+|[^A-Za-z0-9\.]", p))
    return [t for t in tokens if t.strip()]


def ner_infer_dataframe(df: pd.DataFrame, desc_col: str, cfg: NerInferenceConfig) -> pd.DataFrame:
    """
    Run DistilBERT token classification on each row description.

    Output columns:
      - NER_Result: stringified list of (token, label)
      - Vendor_Name_Model: simple concatenation of extracted fields (best-effort)

    Note: This keeps inference optional; if transformers/torch are unavailable, it fails clearly.
    """
    try:
        import torch
        from transformers import DistilBertTokenizerFast, DistilBertForTokenClassification
    except Exception as e:
        raise RuntimeError("transformers/torch not available for NER inference") from e

    tokenizer = DistilBertTokenizerFast.from_pretrained(str(cfg.model_dir))
    model = DistilBertForTokenClassification.from_pretrained(str(cfg.model_dir))

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model.to(device)
    model.eval()

    id2label = model.config.id2label

    def infer_one(text: str) -> Tuple[List[Tuple[str, str]], Dict[str, str], str]:
        tokens = _simple_tokenize_for_ner(text)
        if not tokens:
            return [], {}, ""
        enc = tokenizer(tokens, is_split_into_words=True, return_tensors="pt",
                        truncation=True, padding="max_length", max_length=cfg.max_len)
        enc = {k: v.to(device) for k, v in enc.items()}
        with torch.no_grad():
            out = model(**enc)
        pred_ids = out.logits.argmax(dim=-1).squeeze(0).tolist()
        word_ids = tokenizer(tokens, is_split_into_words=True, truncation=True,
                             padding="max_length", max_length=cfg.max_len).word_ids()

        pairs: List[Tuple[str, str]] = []
        seen = set()
        for i, wid in enumerate(word_ids):
            if wid is None or wid in seen:
                continue
            seen.add(wid)
            if wid < len(tokens):
                pairs.append((tokens[wid], id2label.get(pred_ids[i], "O")))

        # Aggregate into fields (very light; you can reuse your exact tokens_to_fields if desired)
        fields: Dict[str, List[str]] = {}
        for tok, lab in pairs:
            if lab == "O" or lab == "IGNORE":
                continue
            fields.setdefault(lab, []).append(tok)
        fields_str = {k: " ".join(v) for k, v in fields.items()}

        # Simple vendor name: concatenate in stable order if present
        order = ["Category", "Type", "Resistance", "Capacitance", "Voltage", "Tolerance", "Power", "Package", "Size", "Pin_Count"]
        vendor = "".join([fields_str.get(k, "") for k in order]).replace(" ", "")
        return pairs, fields_str, vendor

    ner_results = []
    vendor_names = []
    for _, row in df.iterrows():
        pairs, _, vendor = infer_one(row.get(desc_col, ""))
        ner_results.append(str(pairs))
        vendor_names.append(vendor)

    df = df.copy()
    df["NER_Result"] = ner_results
    df["Vendor_Name_Model"] = vendor_names
    return df


# -----------------------------------------------------------------------------
# 6) Validation & AUTO/REVIEW routing
# -----------------------------------------------------------------------------
def decide_status(row: pd.Series) -> Tuple[str, str]:
    """
    Decide if a row is AUTO or NEED_REVIEW.

    Current rule (simple, safe):
      - If category is OT -> REVIEW (often needs manual)
      - If normalized description missing core fields for RES/CAP -> REVIEW
      - Otherwise AUTO

    You can strengthen this by adding confidence thresholds (model) or constraint checks.
    """
    cat = str(row.get("類別", "")).strip()
    norm = str(row.get("正規化Description", "")).strip()
    coverage = row.get("判別比例", None)

    # Rule requested by user: if recognized/decidable characters < 15% -> human review.
    # "判別比例" is a best-effort proxy for how much of the description is explained
    # by extracted normalized fields. Engineers can tune this metric later.
    try:
        if coverage is not None and float(coverage) < 0.15:
            return "NEED_REVIEW", f"low_coverage<{float(coverage):.2f}"
    except Exception:
        pass

    if not norm:
        return "NEED_REVIEW", "normalized_description_empty"
    if cat == "OT":
        return "NEED_REVIEW", "category_OT"
    if cat == "RES" and "阻值" not in norm:
        return "NEED_REVIEW", "missing_resistance"
    if cat == "CAP" and "容量" not in norm:
        return "NEED_REVIEW", "missing_capacitance"

    return "AUTO", ""


# -----------------------------------------------------------------------------
# 7) Main pipeline
# -----------------------------------------------------------------------------
def run_pipeline(
    input_path: Path,
    out_dir: Path,
    sheet: Optional[str],
    model_dir: Optional[Path],
    verbose: bool,
    debug: bool,
) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1) Read Excel with detected header row
    df, chosen_sheet = read_excel_auto_detect_sheet_and_header(input_path, sheet=sheet)
    original_cols = list(df.columns)  # for clean output: keep exactly what customer provided
    if verbose:
        print(f"[INFO] Using sheet: {chosen_sheet}")

    # 2) Only standardize Description column name into description_raw
    #    Keep original columns untouched (we only ADD new columns).
    col_desc = find_col(df, [r"^description(_修)?$", r"描述", r"品名", r"名稱", r"name", r"title"]) or "Description"
    if col_desc not in df.columns:
        # last resort: try original 'Description'
        raise ValueError("Cannot find Description column in the provided Excel.")

    df["description_raw"] = df[col_desc].astype(str).fillna("")

    # Preserve a user-friendly original description column for outputs (debug only)
    if debug:
        df["原始Description"] = df["description_raw"]

    # 3) Rule-based normalization
    df["_BASE_"] = df["description_raw"].map(normalize_base_text)
    df["類別"] = df.apply(pick_category, axis=1)

    rows = []
    for _, row in df.iterrows():
        base = str(row.get("_BASE_", ""))
        cat = str(row.get("類別", "OT"))
        tks = extract_tokens(base, cat)

        # Coverage ratio: how much of the base description is explained by extracted fields.
        # This is used to route ambiguous rows to human review (threshold default 15%).
        base_compact = re.sub(r"\s+", "", base)
        total_chars = max(len(base_compact), 1)
        explained = "".join([str(v) for v in tks.values() if v])
        explained_chars = len(re.sub(r"\s+", "", explained))
        coverage_ratio = explained_chars / total_chars

        norm = build_normalized_desc(cat, tks)
        disp = display20(cat, norm, tks)

        out_row = dict(row)  # copy all original columns + added working columns
        # extracted fields
        out_row.update({
            "阻值": tks.get("阻值",""),
            "容量": tks.get("容量",""),
            "電壓": tks.get("電壓",""),
            "容差": tks.get("容差",""),
            "功率": tks.get("功率",""),
            "介質": tks.get("介質",""),
            "尺寸": tks.get("尺寸",""),
            "封裝": tks.get("封裝",""),
            "方向": tks.get("方向",""),
            "其餘規格": tks.get("其餘規格",""),
            "判別比例": round(coverage_ratio, 4),
            "正規化Description": norm,
            "顯示名20": disp,
            # Debug-only: helps engineers tune rules quickly
            "Pipe檢視": pipe_view(cat, tks) if debug else "",
        })
        rows.append(out_row)

    out_main = pd.DataFrame(rows)

    # 4) Optional NER model inference (adds extra columns)
    if model_dir:
        if verbose:
            print(f"[INFO] Running NER inference using model_dir={model_dir}")
        out_main = ner_infer_dataframe(out_main, desc_col="description_raw", cfg=NerInferenceConfig(model_dir=model_dir))

    # 5) Group summary sheet (same PN different normalized descriptions)
    col_pn = find_col(out_main, [r"^dicon\s*p/?n$", r"^dicon", r"料號", r"^p/?n$"]) or "DiCon P/N"
    if col_pn in out_main.columns:
        grp = out_main.groupby(col_pn)["正規化Description"].agg(lambda s: sorted(set(map(str, s))))
        summary = grp.reset_index()
        summary["正規化描述數"] = summary["正規化Description"].map(len)
        summary["同料不同名?"] = summary["正規化描述數"].map(lambda n: "同料不同名" if n > 1 else "—")
        disp_map = out_main.groupby(col_pn)["顯示名20"].first().to_dict()
        summary["顯示名20(例)"] = summary[col_pn].map(disp_map)
        out_group = summary[[col_pn, "顯示名20(例)", "正規化描述數", "同料不同名?", "正規化Description"]]
    else:
        out_group = pd.DataFrame({"NOTE": ["PN column not found; group summary skipped."]})

    # 6) AUTO/REVIEW routing
    statuses = out_main.apply(lambda r: decide_status(r), axis=1, result_type="expand")
    out_main["status"] = statuses[0]
    out_main["review_reason"] = statuses[1]

    out_auto = out_main[out_main["status"] == "AUTO"].copy()
    out_review = out_main[out_main["status"] != "AUTO"].copy()

    # 6.5) Output column policy (important for handoff)
    # Company users usually want:
    #   - All original input columns
    #   - A small, stable set of normalized columns
    # and do NOT want intermediate debug columns.
    normalized_cols = [
        "類別",
        "阻值",
        "容量",
        "電壓",
        "容差",
        "功率",
        "介質",
        "尺寸",
        "封裝",
        "方向",
        "其餘規格",
        "正規化Description",
        "顯示名20",
        "判別比例",
        "status",
        "review_reason",
    ]
    model_cols = [c for c in ["NER_Result", "Vendor_Name_Model"] if c in out_main.columns]
    keep_cols = [c for c in (original_cols + normalized_cols + model_cols) if c in out_main.columns]

    if not debug:
        # Drop internal engineering columns unless debug mode.
        drop_internal = [c for c in ["description_raw", "原始Description", "_BASE_", "Pipe檢視"] if c in out_main.columns]
        out_main = out_main.drop(columns=drop_internal, errors="ignore")
        out_auto = out_auto.drop(columns=drop_internal, errors="ignore")
        out_review = out_review.drop(columns=drop_internal, errors="ignore")

    # Reorder to the desired column order (if some columns do not exist, they are skipped).
    out_main = out_main[keep_cols]
    out_auto = out_auto[keep_cols]
    # REVIEW sheet benefits from showing 判別比例 even if not used elsewhere.
    out_review = out_review[keep_cols]

    # 7) Write outputs
    stem = input_path.stem
    final_path = out_dir / f"{stem}_final.xlsx"
    auto_path = out_dir / f"{stem}_AUTO.xlsx"
    review_path = out_dir / f"{stem}_REVIEW.xlsx"

    with pd.ExcelWriter(final_path, engine="openpyxl") as w:
        out_main.to_excel(w, index=False, sheet_name="主分頁")
        out_group.to_excel(w, index=False, sheet_name="群組彙總")

    out_auto.to_excel(auto_path, index=False)
    out_review.to_excel(review_path, index=False)

    if verbose:
        print(f"[DONE] Final:  {final_path}")
        print(f"[DONE] AUTO:   {auto_path}  (rows={len(out_auto)})")
        print(f"[DONE] REVIEW: {review_path} (rows={len(out_review)})")


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="One-command BOM cleaning + normalization pipeline (rule + optional NER).")
    # `--input` is optional to reduce friction for non-technical users.
    # Priority order:
    #   1) positional INPUT
    #   2) --input
    #   3) auto-pick the most recently modified .xlsx/.xlsm in the current folder
    p.add_argument("input_pos", nargs="?", help="(Optional) Input BOM Excel path as a positional argument.")
    p.add_argument("--input", required=False, type=str, help="Input BOM Excel file path.")
    p.add_argument("--out_dir", default="outputs", type=str, help="Output directory.")
    p.add_argument(
        "--sheet",
        default=None,
        type=str,
        help="Excel sheet name. Default: auto-detect the first sheet containing a 'Description' header.",
    )
    p.add_argument("--model_dir", default=None, type=str, help="Optional HuggingFace model directory for NER inference.")
    p.add_argument("--gui", action="store_true", help="Open a file picker to select the Excel input.")
    p.add_argument(
        "--debug",
        action="store_true",
        help="Include intermediate/debug columns in outputs (for engineers). Default: OFF (clean outputs).",
    )
    p.add_argument("--verbose", action="store_true", help="Print progress logs.")
    return p


def main() -> None:
    args = build_arg_parser().parse_args()

    # Resolve input path with fallbacks
    input_arg = args.input_pos or args.input

    # 1) GUI file picker has the highest priority (requested by user for company use).
    if args.gui:
        picked = pick_excel_file_via_dialog(initial_dir=Path.cwd())
        if picked is None:
            raise SystemExit("No file selected (cancelled).")
        input_path = picked.resolve()
    # 2) CLI input
    elif input_arg:
        input_path = Path(input_arg).expanduser().resolve()
    # 3) No input provided: try GUI first (if available), otherwise auto-pick newest Excel
    else:
        picked = pick_excel_file_via_dialog(initial_dir=Path.cwd())
        if picked is not None:
            input_path = picked.resolve()
        else:
            candidates = sorted(
                list(Path.cwd().glob("*.xlsx")) + list(Path.cwd().glob("*.xlsm")) + list(Path.cwd().glob("*.xls")),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )
            if not candidates:
                raise SystemExit(
                    "No input provided and no Excel files found in the current folder.\n"
                    "Please run: python run_bom_pipeline.py --gui  (recommended)\n"
                    "or: python run_bom_pipeline.py --input <BOM.xlsx>"
                )
            input_path = candidates[0].resolve()
            if args.verbose:
                print(f"[INFO] Auto-selected input: {input_path}")

    out_dir = Path(args.out_dir).expanduser().resolve()
    sheet = args.sheet
    model_dir = Path(args.model_dir).expanduser().resolve() if args.model_dir else None

    if not input_path.exists():
        raise FileNotFoundError(f"Input not found: {input_path}")
    if model_dir and not model_dir.exists():
        # Company-friendly behavior: do NOT crash; continue in rule-only mode.
        print(f"[WARN] Model dir not found; running RULE-ONLY mode: {model_dir}")
        model_dir = None

    run_pipeline(
        input_path=input_path,
        out_dir=out_dir,
        sheet=sheet,
        model_dir=model_dir,
        verbose=args.verbose,
        debug=args.debug,
    )


if __name__ == "__main__":
    main()
