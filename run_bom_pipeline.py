#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_bom_pipeline.py

一鍵式 BOM 清理 + 正規化流水線（規則式處理，移植自 Notebook V3 邏輯），
可選擇性地結合 DistilBERT NER 推論輸出進行強化。

本腳本設計為可交接給其他工程團隊使用。
刻意使用詳細的註解和清晰的關注點分離。

高階功能概述
-----------
1) 讀取客戶 BOM Excel（標頭列可能不在第一列）。
2) 正規化欄位名稱（僅將 Description 欄位標準化為 `description_raw`）。
   - 所有原始欄位均保留原樣。
3) 規則式正規化（移植自 BOM_pipeline0923V3.ipynb）：
   - 建立正規化基底文字 (_BASE_) 供穩健的正規表達式比對
   - 分類零件類別 (RES/CAP/IC/CN/ID/OT)
   - 透過正規表達式提取關鍵規格
   - 建立「正規化Description」和「顯示名20」
   - 產生「群組彙總」工作表以檢查「同料號不同名」
4) 可選：執行 NER 模型推論（DistilBERT Token Classification）：
   - 新增 NER_Result 和 Vendor_Name_Model 欄位
5) 輸出：
   - <stem>_final.xlsx（主分頁 + 群組彙總）
   - <stem>_AUTO.xlsx（通過基本驗證的資料列）
   - <stem>_REVIEW.xlsx（需人工審核的資料列）

使用方式
-------
建議用法（明確指定輸入路徑）：

    python run_bom_pipeline.py --input "BOM.xlsx" --out_dir "outputs"

若不確定工作表名稱（常見情況），可省略 --sheet（預設：自動偵測）。

可選：包含模型推論輸出（DistilBERT NER）：

    python run_bom_pipeline.py --input "BOM.xlsx" --out_dir "outputs" --model_dir "distilbert_ner_final"

便利用法：也可以 *位置參數* 方式傳入輸入路徑：

    python run_bom_pipeline.py "BOM.xlsx"

備註
----
- 本腳本以「規則正規化」作為正規化描述的權威來源。
- NER 輸出附加為額外證據；後續可強化合併策略。

作者：(交接用)
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
import json
import re
import unicodedata
from typing import Dict, List, Optional, Tuple

import pandas as pd

# 可選的 GUI 檔案選擇器（tkinter）。若 tkinter 不可用，流水線仍可透過 CLI 參數運作。
try:
    import tkinter as _tk
    from tkinter import filedialog as _filedialog
except Exception:  # pragma: no cover
    _tk = None
    _filedialog = None


def pick_excel_file_via_dialog(initial_dir: Optional[Path] = None) -> Optional[Path]:
    """開啟原生檔案選擇器並回傳選擇的 Excel 路徑（若取消則回傳 None）。"""
    if _tk is None or _filedialog is None:
        return None
    root = _tk.Tk()
    root.withdraw()
    # 盡力嘗試：將對話框帶到最上層
    try:
        root.attributes("-topmost", True)
    except Exception:
        pass
    filetypes = [
        ("Excel 檔案", "*.xlsx *.xlsm *.xls"),
        ("所有檔案", "*.*"),
    ]
    path = _filedialog.askopenfilename(
        title="選擇 BOM Excel 檔案",
        initialdir=str(initial_dir or Path.cwd()),
        filetypes=filetypes,
    )
    try:
        root.destroy()
    except Exception:
        pass
    if not path:
        return None
    return Path(path)



# -----------------------------------------------------------------------------
# 0) Excel 讀取輔助函式
# -----------------------------------------------------------------------------
def _normalize_colname(x: object) -> str:
    """正規化欄位名稱（僅供比對用，不改變原始資料）。"""
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
    """若找到包含 'Description' 的標頭列，則回傳該列索引。"""
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
    讀取 Excel BOM，處理以下情況：
      - 目標工作表 *通常* 是第一個工作表，但名稱可能不同。
      - 標頭列可能不在第一列。

    行為說明：
      1) 若提供 `sheet` 參數：僅掃描該工作表。
      2) 否則：依序掃描工作表，選擇第一個包含符合
         `required_col_regex` 儲存格的工作表（預設：'^description$'）。
      3) 若在前 `header_search_rows` 列中沒有工作表包含 'Description'，
         則退回至第一個工作表並以 header=0 讀取。

    回傳值：
      (df, 選定的工作表名稱)
    """
    xls = pd.ExcelFile(path)
    sheet_names = xls.sheet_names
    if not sheet_names:
        raise ValueError(f"Excel 中未找到任何工作表：{path}")

    # 決定要掃描的工作表
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
        # 退回至第一個工作表，header=0
        chosen_sheet = sheet if sheet else sheet_names[0]
        df = pd.read_excel(path, sheet_name=chosen_sheet, header=0)
    else:
        df = pd.read_excel(path, sheet_name=chosen_sheet, header=chosen_header_row)

    df.columns = [_normalize_colname(c) for c in df.columns]
    return df, chosen_sheet


# -----------------------------------------------------------------------------
# 1) 文字正規化工具（移植自 BOM_pipeline0923V3.ipynb）
# -----------------------------------------------------------------------------
def nfkc(s: str) -> str:
    """Unicode NFKC 正規化。"""
    return unicodedata.normalize("NFKC", "" if s is None else str(s))


def upper_ascii(s: str) -> str:
    """
    正規化常見 BOM 符號並回傳大寫 ASCII 格式文字。

    範例：
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


# 基底正規化時要移除的詞彙（移植清單；可依需求調整）
DROP_WORDS = [
    "PLEASE", "W/", "WITH", "WITHOUT", "SMD", "SMT", "TH", "T/H",
    "GENERIC", "GENERAL", "STANDARD", "TYPE", "KIND",
]

def normalize_base_text(desc: str) -> str:
    """
    建立穩健的基底字串供正規表達式提取：
    - 正規化 Unicode 和常見符號
    - 移除括號內容
    - 將 OHM/OHMS 轉換為 O
    - 將分隔符 , | / 替換為空格
    - 移除雜訊詞彙
    - 合併多餘空白
    """
    s = upper_ascii(desc)

    # 移除 (...) 內容，保留括號外文字
    s = re.sub(r"\([^)]*\)", " ", s)
    s = re.sub(r"（[^）]*）", " ", s)

    # OHM/OHMS -> O
    s = re.sub(r"\bOHMS?\b", "O", s)

    # 分隔符處理
    s = s.replace(",", " ").replace("|", " ").replace("/", " ")

    # 移除雜訊詞彙
    for w in DROP_WORDS:
        s = re.sub(rf"\b{re.escape(w)}\b", " ", s)

    s = re.sub(r"\s+", " ", s).strip()
    return s


def find_col(df: pd.DataFrame, patterns: List[str]) -> Optional[str]:
    """找出欄位名稱符合 `patterns` 中任一正規表達式的欄位。"""
    for c in df.columns:
        for p in patterns:
            if re.search(p, str(c), re.I):
                return c
    return None


# -----------------------------------------------------------------------------
# 2) 類別分類（移植自 BOM_pipeline0923V3.ipynb）
# -----------------------------------------------------------------------------
PREFIX = {"RES": "RS", "CAP": "CP", "ID": "ID", "IC": "IC", "CN": "CN", "OT": "OT"}

# 類別關鍵字規則（移植版；可依公司需求擴充/調整）
CAT_RULES = [
    ("RES", [r"\bRES\b", r"\bRESIST", r"\bOHM\b", r"\b[0-9]+R[0-9]*\b", r"\b[0-9.]+K\b", r"\b[0-9.]+M\b"]),
    ("CAP", [r"\bCAP\b", r"\bCAPAC", r"\bUF\b", r"\bNF\b", r"\bPF\b"]),
    ("ID",  [r"\bIND\b", r"\bINDUCT", r"\bBEAD\b", r"\bUH\b", r"\bNH\b", r"\bMH\b"]),
    ("IC",  [r"\bIC\b", r"\bMCU\b", r"\bDRIVER\b", r"\bREGULATOR\b", r"\bOPAMP\b", r"\bAMPLIFIER\b"]),
    ("CN",  [r"\bCONN\b", r"\bCONNECT", r"\bHEADER\b", r"\bSOCKET\b", r"\bUSB\b", r"\bRJ45\b"]),
]


def classify(base_text: str) -> str:
    """根據關鍵字規則回傳粗略類別標籤。"""
    s = base_text
    for cat, pats in CAT_RULES:
        for p in pats:
            if re.search(p, s, re.I):
                return cat
    return "OT"


def pick_category(row: pd.Series) -> str:
    """從 _BASE_ 或現有欄位（若存在）中選取類別。"""
    base = row.get("_BASE_", "")
    return classify(str(base))


# -----------------------------------------------------------------------------
# 3) 正規表達式提取器（移植自 BOM_pipeline0923V3.ipynb；核心最小集）
#    可在不變更流水線架構的情況下擴充這些規則。
# -----------------------------------------------------------------------------
# 通用量測值
RES_RE = re.compile(r"\b(\d+(?:\.\d+)?)(R|K|M)?(\d+)?\b", re.I)  # 處理 4K7, 10R, 1M
CAP_RE = re.compile(r"\b(\d+(?:\.\d+)?)(U|N|P)F\b", re.I)
CAP_CODE_RE = re.compile(r"\b(\d{3})\b")  # 如 104 代碼（盡力匹配）
VOLT_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s?V(?:DC)?\b", re.I)
TOL_RE = re.compile(r"\b±?\s?(\d+(?:\.\d+)?)\s?%\b", re.I)
PWR_RE = re.compile(r"\b(\d+/\d+|\d+(?:\.\d+)?)\s?(W|MW)\b", re.I)

# 封裝 / 尺寸
PKG_RE = re.compile(r"\b(0402|0603|0805|1206|1210|2010|2512|1608|2012|3216)\b", re.I)
PKG_WORD_RE = re.compile(r"\b(SOT-?\d+|SOD-?\d+|SOP-?\d+|SOIC-?\d+|QFN-?\d+|DFN-?\d+|TO-?\d+)\b", re.I)


def canon_pkg(s: str) -> str:
    """標準化封裝字串：移除空格/破折號，轉大寫。"""
    t = upper_ascii(s)
    t = re.sub(r"[\s\-]+", "", t)
    return t


def normalize_power_token(p: str) -> str:
    """
    將功率標記正規化為簡潔表示。
    範例：
    - 1/8W -> 0.125W（可選）
    - 250mW -> 0.25W（可選）
    目前保留原始大寫形式以避免非預期轉換。
    """
    return upper_ascii(p)


def _format_resistance(token: str) -> str:
    """
    盡力正規化電阻值：
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
    """選取最短的電阻表示法。"""
    if not cands:
        return ""
    return min(cands, key=len)


def shortest_cap_string(cands: List[str]) -> str:
    """選取最短的電容表示法。"""
    if not cands:
        return ""
    return min(cands, key=len)


def extract_generic_meas(base: str) -> Dict[str, str]:
    """
    從正規化基底文字中提取常見量測值。
    回傳提取欄位的字典（字串形式）。
    """
    s = base
    out: Dict[str, str] = {}

    # 電阻值（尋找以 R/K/M 模式結尾的標記）
    res_tokens = []
    for tok in s.split():
        if re.fullmatch(r"\d+(?:\.\d+)?[RKM]\d*", tok, re.I):
            res_tokens.append(_format_resistance(tok))
    if res_tokens:
        out["阻值"] = shortest_res_string(res_tokens) + "O"  # 筆記本使用 'O' 作為歐姆後綴

    # 電容值（如 10UF/100NF/1PF）
    cap_tokens = []
    for m in CAP_RE.finditer(s):
        cap_tokens.append(f"{m.group(1)}{m.group(2).upper()}F")
    if cap_tokens:
        out["容量"] = shortest_cap_string(cap_tokens)

    # 電壓
    m = VOLT_RE.search(s)
    if m:
        out["電壓"] = f"{m.group(1)}V"

    # 容差
    m = TOL_RE.search(s)
    if m:
        out["容差"] = f"{m.group(1)}%"

    # 功率
    m = PWR_RE.search(s)
    if m:
        out["功率"] = normalize_power_token(m.group(0))

    # 封裝 / 尺寸
    m = PKG_RE.search(s)
    if m:
        out["尺寸"] = upper_ascii(m.group(1))
    m = PKG_WORD_RE.search(s)
    if m:
        out["封裝"] = canon_pkg(m.group(1))

    return out


def extract_tokens(base: str, cat: str) -> Dict[str, str]:
    """
    類別感知的提取包裝器。
    擴充此函式以針對各類別新增更多規則。
    """
    out = extract_generic_meas(base)

    # CAP 的簡易介電質提取（盡力處理）
    if cat == "CAP":
        m = re.search(r"\b(C0G|NP0|X7R|X5R|Y5V)\b", base, re.I)
        if m:
            out["介質"] = m.group(1).upper()

    # 連接器方向範例
    if cat == "CN":
        m = re.search(r"\b(RIGHT\s?ANGLE|VERTICAL)\b", base, re.I)
        if m:
            out["方向"] = upper_ascii(m.group(1))

    return out


# -----------------------------------------------------------------------------
# 4) 建立正規化描述與顯示名稱（移植概念）
# -----------------------------------------------------------------------------
def build_normalized_desc(cat: str, t: Dict[str, str]) -> str:
    """
    建構人類可讀的正規化描述。
    請保持穩定，因為此描述將用於資料庫中的分組/去重。

    這是筆記本邏輯的簡化移植：
    前綴 + 關鍵欄位依固定順序排列。
    """
    parts: List[str] = [cat]

    # 常用順序（可依需求擴充）
    for k in ["阻值", "容量", "電壓", "容差", "功率", "介質", "尺寸", "封裝", "方向"]:
        v = t.get(k, "")
        if v:
            parts.append(v)

    # 其餘欄位可附加於末尾（可選）
    return " ".join(parts).strip()


def display20(cat: str, norm: str, others: Optional[Dict[str, str]] = None, min_len: int = 8, max_len: int = 20) -> str:
    """
    建立簡潔的顯示名稱（<=20 字元）供 UI 欄位或快速瀏覽使用。
    策略：
      - 以類別作為前綴（RS/CP/IC/CN/ID/OT）
      - 移除尾部標記的空格
      - 若太短，則使用其他可用欄位補充
    """
    prefix = PREFIX.get(cat, "OT")
    tail = norm.split(" ", 1)[1] if " " in norm else ""
    tokens = tail.split()
    base = prefix + "".join(tokens)

    if len(base) >= min_len:
        return base[:max_len]

    # 補充填充
    if others:
        extra = "".join([str(v) for v in others.values() if v])
        base2 = (base + extra)[:max_len]
        return base2

    return base[:max_len]


def pipe_view(cat: str, t: Dict[str, str]) -> str:
    """
    除錯友善的檢視字串，顯示已提取的欄位。
    工程師可用此快速調整規則。
    """
    keys = ["阻值", "容量", "電壓", "容差", "功率", "介質", "尺寸", "封裝", "方向"]
    parts = [f"{k}={t.get(k,'')}" for k in keys if t.get(k, "")]
    return f"{cat}|" + "|".join(parts)


# -----------------------------------------------------------------------------
# 5) 可選的 NER 推論（移植自 apply_model_to_bom.py 核心概念）
# -----------------------------------------------------------------------------
@dataclass
class NerInferenceConfig:
    model_dir: Path
    max_len: int = 64


def _simple_tokenize_for_ner(text: str) -> List[str]:
    """
    符合 NER 訓練設定的分詞方式。
    這是精簡版本；如需要可替換為您的訓練分詞器。
    """
    if text is None:
        return []
    # 先依空白分割
    parts = re.findall(r"\S+", str(text))
    tokens: List[str] = []
    for p in parts:
        # 分割為字母數字+符號區塊
        tokens.extend(re.findall(r"[A-Za-z0-9\.]+|[^A-Za-z0-9\.]", p))
    return [t for t in tokens if t.strip()]


def ner_infer_dataframe(df: pd.DataFrame, desc_col: str, cfg: NerInferenceConfig) -> pd.DataFrame:
    """
    對每列描述執行 DistilBERT Token Classification。

    輸出欄位：
      - NER_Result：字串化的 (token, label) 列表
      - Vendor_Name_Model：提取欄位的簡單串接（盡力處理）

    備註：此處保持推論為可選；若 transformers/torch 不可用，會明確失敗。
    """
    try:
        import torch
        from transformers import DistilBertTokenizerFast, DistilBertForTokenClassification
    except Exception as e:
        raise RuntimeError("transformers/torch 不可用於 NER 推論") from e

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

        # 聚合為欄位（輕量處理；如需要可重用您的 tokens_to_fields）
        fields: Dict[str, List[str]] = {}
        for tok, lab in pairs:
            if lab == "O" or lab == "IGNORE":
                continue
            fields.setdefault(lab, []).append(tok)
        fields_str = {k: " ".join(v) for k, v in fields.items()}

        # 簡易供應商名稱：若存在則依穩定順序串接
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
# 6) 驗證與 AUTO/REVIEW 路由
# -----------------------------------------------------------------------------
def decide_status(row: pd.Series) -> Tuple[str, str]:
    """
    決定資料列為 AUTO 或 NEED_REVIEW。

    目前規則（簡單、安全）：
      - 若類別為 OT -> REVIEW（通常需人工處理）
      - 若正規化描述缺少 RES/CAP 的核心欄位 -> REVIEW
      - 否則 AUTO

    可透過新增模型信心閾值或約束檢查來強化此規則。
    """
    cat = str(row.get("類別", "")).strip()
    norm = str(row.get("正規化Description", "")).strip()

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
# 7) 主要流水線
# -----------------------------------------------------------------------------
def run_pipeline(input_path: Path, out_dir: Path, sheet: Optional[str], model_dir: Optional[Path], verbose: bool) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1) 讀取 Excel 並偵測標頭列
    df, chosen_sheet = read_excel_auto_detect_sheet_and_header(input_path, sheet=sheet)
    if verbose:
        print(f"[資訊] 使用工作表：{chosen_sheet}")

    # 2) 僅將 Description 欄位名稱標準化為 description_raw
    #    保持原始欄位不變（我們只新增欄位）。
    col_desc = find_col(df, [r"^description(_修)?$", r"描述", r"品名", r"名稱", r"name", r"title"]) or "Description"
    if col_desc not in df.columns:
        # 最後手段：嘗試原始 'Description'
        raise ValueError("無法在提供的 Excel 中找到 Description 欄位。")

    df["description_raw"] = df[col_desc].astype(str).fillna("")

    # 保留使用者友善的原始描述欄位供輸出使用
    df["原始Description"] = df["description_raw"]

    # 3) 規則式正規化
    df["_BASE_"] = df["description_raw"].map(normalize_base_text)
    df["類別"] = df.apply(pick_category, axis=1)

    rows = []
    for _, row in df.iterrows():
        base = str(row.get("_BASE_", ""))
        cat = str(row.get("類別", "OT"))
        tks = extract_tokens(base, cat)

        norm = build_normalized_desc(cat, tks)
        disp = display20(cat, norm, tks)

        out_row = dict(row)  # 複製所有原始欄位 + 新增的工作欄位
        # 提取的欄位
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
            "正規化Description": norm,
            "顯示名20": disp,
            "Pipe檢視": pipe_view(cat, tks),
        })
        rows.append(out_row)

    out_main = pd.DataFrame(rows)

    # 4) 可選的 NER 模型推論（新增額外欄位）
    if model_dir:
        if verbose:
            print(f"[資訊] 使用 model_dir={model_dir} 執行 NER 推論")
        out_main = ner_infer_dataframe(out_main, desc_col="description_raw", cfg=NerInferenceConfig(model_dir=model_dir))

    # 5) 群組彙總工作表（同料號不同正規化描述）
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
        out_group = pd.DataFrame({"NOTE": ["未找到 PN 欄位；已略過群組彙總。"]})

    # 6) AUTO/REVIEW 路由
    statuses = out_main.apply(lambda r: decide_status(r), axis=1, result_type="expand")
    out_main["status"] = statuses[0]
    out_main["review_reason"] = statuses[1]

    out_auto = out_main[out_main["status"] == "AUTO"].copy()
    out_review = out_main[out_main["status"] != "AUTO"].copy()

    # 7) 輸出檔案
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
        print(f"[完成] Final:  {final_path}")
        print(f"[完成] AUTO:   {auto_path}  (rows={len(out_auto)})")
        print(f"[完成] REVIEW: {review_path} (rows={len(out_review)})")


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="一鍵式 BOM 清理 + 正規化流水線（規則式 + 可選 NER）。")
    # `--input` 為可選，以減少非技術使用者的阻力。
    # 優先順序：
    #   1) 位置參數 INPUT
    #   2) --input
    #   3) 自動選取當前資料夾中最近修改的 .xlsx/.xlsm
    p.add_argument("input_pos", nargs="?", help="（可選）以位置參數方式傳入 BOM Excel 路徑。")
    p.add_argument("--input", required=False, type=str, help="輸入 BOM Excel 檔案路徑。")
    p.add_argument("--out_dir", default="outputs", type=str, help="輸出目錄。")
    p.add_argument(
        "--sheet",
        default=None,
        type=str,
        help="Excel 工作表名稱。預設：自動偵測第一個包含 'Description' 標頭的工作表。",
    )
    p.add_argument("--model_dir", default=None, type=str, help="可選的 HuggingFace 模型目錄供 NER 推論使用。")
    p.add_argument("--verbose", action="store_true", help="印出進度日誌。")
    p.add_argument("--gui", action="store_true", help="開啟檔案選擇對話框以選擇輸入 Excel（Windows/Mac/Linux）。")
    return p


def main() -> None:
    args = build_arg_parser().parse_args()

    # 解析輸入路徑
    # - 若提供 --gui，一律開啟檔案選擇器。
    # - 若未提供輸入，先開啟檔案選擇器（對商業使用者最友善）。
    # - 若 tkinter 不可用（罕見），退回至自動選取當前資料夾中最新的 Excel。
    input_arg = args.input_pos or args.input
    input_path: Optional[Path] = None

    if args.gui or not input_arg:
        picked = pick_excel_file_via_dialog(initial_dir=Path.cwd())
        if picked is not None:
            input_path = picked.resolve()
            if args.verbose:
                print(f"[資訊] 透過對話框選擇輸入：{input_path}")
        else:
            # 使用者取消了對話框。
            if not input_arg:
                raise SystemExit("未選擇輸入檔案。使用者已取消。")
            input_path = Path(input_arg).expanduser().resolve()
    else:
        input_path = Path(input_arg).expanduser().resolve()

    if input_path is None:
        raise SystemExit("無法解析輸入路徑。")

    if not input_path.exists():
        raise FileNotFoundError(f"找不到輸入檔案：{input_path}")

    out_dir = Path(args.out_dir).expanduser().resolve()
    sheet = args.sheet
    model_dir = Path(args.model_dir).expanduser().resolve() if args.model_dir else None

    if not input_path.exists():
        raise FileNotFoundError(f"找不到輸入檔案：{input_path}")
    if model_dir and not model_dir.exists():
        raise FileNotFoundError(f"找不到模型目錄：{model_dir}")

    run_pipeline(input_path=input_path, out_dir=out_dir, sheet=sheet, model_dir=model_dir, verbose=args.verbose)


if __name__ == "__main__":
    main()
