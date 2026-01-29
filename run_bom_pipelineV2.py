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


def pick_excel_file_via_dialog(initial_dir: Path) -> "Path | None":
    """開啟原生檔案選擇對話框以選擇 Excel 檔案。

    回傳值
    ------
    Path | None
        - 選擇檔案的路徑，或
        - 若使用者取消 / GUI 不可用則回傳 None。

    維護者備註
    ---------
    - 此函式刻意保持精簡且無額外依賴（tkinter 是大多數 Python 安裝的標準函式庫）。
    - 在某些精簡版 Python 發行版上，tkinter 可能未安裝。
      此情況下會退回至要求使用 --input 參數。
    """
    try:
        import tkinter as tk
        from tkinter import filedialog
    except Exception as e:
        print("[警告] tkinter 不可用；無法開啟檔案選擇器。"
              "請安裝 tkinter 或使用 --input 指定檔案路徑。")
        return None

    root = tk.Tk()
    root.withdraw()  # 隱藏根視窗
    root.attributes("-topmost", True)

    file_path = filedialog.askopenfilename(
        initialdir=str(initial_dir),
        title="選擇 BOM Excel 檔案",
        filetypes=[
            ("Excel 檔案", "*.xlsx *.xlsm *.xls"),
            ("所有檔案", "*.*"),
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


def normalize_ascii(s: str) -> str:
    """
    正規化常見 BOM 符號但保留原始大小寫。

    範例：
    - µ -> u
    - Ω -> O
    - ％ -> %
    - 1uF -> 1uF (保持原始大小寫)
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
    return t.strip()  # 不轉換大小寫


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
    s = normalize_ascii(desc)  # 保留原始大小寫

    # 移除 (...) 內容，保留括號外文字
    s = re.sub(r"\([^)]*\)", " ", s)
    s = re.sub(r"（[^）]*）", " ", s)

    # OHM/OHMS -> O (不區分大小寫)
    s = re.sub(r"\bOHMS?\b", "O", s, flags=re.I)

    # 分隔符處理
    s = s.replace(",", " ").replace("|", " ").replace("/", " ")

    # 移除雜訊詞彙 (不區分大小寫)
    for w in DROP_WORDS:
        s = re.sub(rf"\b{re.escape(w)}\b", " ", s, flags=re.I)

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
# 重要說明（交接備註）
# ----------------------
# *類別字串* 是客戶端輸出，用於正規化後的 BOM。
# 請保持穩定、一致且易讀。
#
# 在早期版本中，電感被錯誤標記為 "ID"（易與二極體混淆）。
# 使用者要求將電感明確顯示為 "IND"。
#
# 若貴公司日後需要更細緻的分類（如 DIODE vs IND），
# 請擴充 `CAT_RULES`，下游邏輯無需額外修改即可運作。

# `顯示名20` 使用的顯示前綴。為避免縮寫混淆，我們使用類別本身作為前綴。
PREFIX = {"RES": "RES", "CAP": "CAP", "IND": "IND", "IC": "IC", "CN": "CN", "OT": "OT"}

# 類別關鍵字規則（移植版；可依公司需求擴充/調整）
CAT_RULES = [
    ("RES", [r"\bRES\b", r"\bRESIST", r"\bOHM\b", r"\b[0-9]+R[0-9]*\b", r"\b[0-9.]+K\b", r"\b[0-9.]+M\b"]),
    ("CAP", [r"\bCAP\b", r"\bCAPAC", r"\bUF\b", r"\bNF\b", r"\bPF\b"]),
    # 電感 / 鐵氧體磁珠
    ("IND", [r"\bIND\b", r"\bINDUCT", r"\bFERRITE\b", r"\bBEAD\b", r"\bUH\b", r"\bNH\b", r"\bMH\b"]),
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
# V17 修正：容量正規表達式允許數字和單位之間有空格
CAP_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*([UuNnPp])[Ff]\b", re.I)
CAP_CODE_RE = re.compile(r"\b(\d{3})\b")  # 如 104 代碼（盡力匹配）

# ===== V17 新增：電流 (mA/uA/A) =====
CURRENT_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*([uUmM]?[Aa])\b", re.I)

# ===== V17 新增：電感值 (nH/uH/mH) =====
IND_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*([nNuUmM][Hh])\b", re.I)

# ===== 新增：直流電阻 DCR (mΩ/uΩ) =====
# 處理 71.9mΩ, 100uΩ 這類電感的直流電阻規格
DCR_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*([mMuU]?[ΩO])\b", re.I)

# 電壓模式
#  - 範圍：1.65~3.6V, 1.65-3.6V, 1.65 to 3.6V
#  - 單一：3.6V, 5VDC
#  - V17 新增：裸電壓 (275/400/630) 安規電壓
VOLT_RANGE_RE = re.compile(
    r"\b(\d+(?:\.\d+)?)\s*(?:~|\-|TO)\s*(\d+(?:\.\d+)?)\s*k?V(?:AC|DC)?\b",
    re.I,
)
VOLT_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*k?V(?:AC|DC)?\b", re.I)
VOLT_BARE_RE = re.compile(r"\b(275|400|630)\b(?![\w%])")  # 安規裸電壓

# V17 修正：容差正規表達式支援 +/- 和全形符號
TOL_RE = re.compile(r"[±\+\-/]?\s*(\d+(?:\.\d+)?)\s*[%％]", re.I)
PWR_RE = re.compile(r"\b(\d+/\d+|\d+(?:\.\d+)?)\s?([mM]?[Ww])\b", re.I)

# ===== V17 新增：溫度係數 (PPM) =====
TEMP_COEF_RE = re.compile(r"[<>≦≤±\+\-/]?\s*(\d+(?:\.\d+)?)\s*PPM\b", re.I)

# ===== V17 新增：波長 (nm) =====
WAVELENGTH_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*nm\b", re.I)

# ===== V17 新增：針腳數 (8P, 2x8P, 16PIN) =====
PIN_COUNT_RE = re.compile(r"\b(\d+(?:\s*[xX]\s*\d+)?)\s*[Pp](?:IN)?[Ss]?\b", re.I)

# ===== V17 新增：間距 (P=2.54mm, L=5mm) =====
PITCH_RE = re.compile(r"\b[PLWHplwh][:=]?\s*(\d+(?:\.\d+)?)\s*mm\b", re.I)
PITCH_DIM_RE = re.compile(r"\b(\d+(?:\.\d+)?)(?:\s*[xX]\s*(\d+(?:\.\d+)?)){1,2}\s*mm\b", re.I)

# ===== V17 新增：顏色 =====
COLOR_RE = re.compile(r"\b(RED|GREEN|BLUE|WHITE|YELLOW|AMBER|RGB|BLACK|NATURAL)\b", re.I)

# ===== V17 新增：頻率 (MHz/kHz/GHz) =====
FREQ_RE = re.compile(r"\b(\d+(?:\.\d+)?)\s*([kKmMgG]?[Hh][Zz])\b", re.I)

# ===== V17 新增：類型 (介質類型、電晶體極性等) =====
TYPE_RE = re.compile(
    r"\b(CERAMIC|CER|TANTALUM|TANT|ELEC|ELECTROLYTIC|FILM|PP|THIN|THICK|"
    r"NPN|PNP|N-CH|P-CH|N-TYPE|P-TYPE|BI[-\s]?DIRECTIONAL|UNI[-\s]?DIRECTIONAL|"
    r"SCHOTTKY|ZERO-DRIFT|COMMON|POWER|ARRAY)\b",
    re.I,
)

# ===== V17 新增：法規 =====
COMPLIANCE_RE = re.compile(
    r"\b(RoHS|HF|Halogen\s*Free|Pb\s*Free|Lead[-\s]*Free|REACH|UL94V0|AEC-Q200|Green|Eco)\b",
    re.I,
)

# 封裝 / 尺寸
PKG_RE = re.compile(r"\b(01005|0201|0402|0603|0805|1206|1210|2010|2512|1608|2012|3216|2835)\b", re.I)
PKG_WORD_RE = re.compile(
    r"\b(SOT-?\d+[-\w]*|SOD-?\d+[-\w]*|SOP-?\d+|SOIC-?\d+|TSSOP-?\d*|LQFP-?\d*|MSOP-?\d*|"
    r"QFN-?\d+[-\w]*|DFN-?\d+[-\w]*|TO[-_]?\d+[-\w]*|DPAK|D2PAK|DO-?\d+[-\w]*|"
    r"POWERDI[-\w]*|WSON[-\w]*|TSON[-\w]*|BGA|SMB|SMA|SIP|ZIP|COB)\b",
    re.I,
)


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


def resistance_to_iec(value: str) -> str:
    """
    將電阻值轉換為 IEC 60063 表示法。
    
    IEC 表示法用 R、K、M 等字母代替小數點，例如：
    - 4.7K -> 4K7
    - 10R -> 10R
    - 1.5M -> 1M5
    - 0.1R -> R10 或 0R1
    - 47KO -> 47K (移除 O 後綴)
    - 71.9mO -> 71m9 (毫歐姆)
    """
    if not value:
        return ""
    
    # 移除 O/Ω 後綴（如果有的話）
    v = re.sub(r'[OΩ]$', '', value.strip(), flags=re.I)
    
    # 嘗試匹配數值+單位的格式 (例如 4.7K, 10R, 71.9m)
    m = re.fullmatch(r'(\d+(?:\.\d+)?)([RKMmµu])?', v, re.I)
    if not m:
        return value  # 無法解析，返回原值
    
    num_str = m.group(1)
    unit = m.group(2) or 'R'  # 預設單位為 R（歐姆）
    unit = unit.upper() if unit.upper() in ['R', 'K', 'M'] else unit.lower()
    
    # 如果是整數（沒有小數點），直接返回
    if '.' not in num_str:
        return f"{num_str}{unit}"
    
    # 有小數點的情況，將小數點替換為單位字母
    parts = num_str.split('.')
    integer_part = parts[0]
    decimal_part = parts[1].rstrip('0')  # 移除尾部的 0
    
    if integer_part == '0':
        # 例如 0.1R -> R10
        return f"{unit}{decimal_part}"
    elif decimal_part:
        # 例如 4.7K -> 4K7
        return f"{integer_part}{unit}{decimal_part}"
    else:
        # 小數部分為空（例如 10.0K -> 10K）
        return f"{integer_part}{unit}"


def capacitance_to_eia(value: str) -> str:
    """
    將電容值轉換為 EIA 代碼表示法。
    
    EIA 代碼是 3 位數字，前兩位是有效數字，第三位是 10 的次方（單位為 pF）：
    - 10pF -> 100 (10 × 10^0 = 10pF)
    - 100pF -> 101 (10 × 10^1 = 100pF)
    - 1nF -> 102 (10 × 10^2 = 1000pF = 1nF)
    - 10nF -> 103 (10 × 10^3 = 10000pF = 10nF)
    - 100nF -> 104 (10 × 10^4 = 100000pF = 100nF)
    - 1uF -> 105 (10 × 10^5 = 1000000pF = 1uF)
    - 10uF -> 106
    - 100uF -> 107
    - 0.1uF -> 104 (100nF = 0.1uF)
    """
    if not value:
        return ""
    
    v = value.strip().upper()
    
    # 解析電容值和單位
    m = re.fullmatch(r'(\d+(?:\.\d+)?)\s*([PNU])?F?', v, re.I)
    if not m:
        return value  # 無法解析，返回原值
    
    num = float(m.group(1))
    unit = (m.group(2) or 'P').upper()  # 預設 pF
    
    # 轉換為 pF
    if unit == 'U':  # uF
        pf = num * 1_000_000
    elif unit == 'N':  # nF
        pf = num * 1_000
    else:  # pF
        pf = num
    
    # 計算 EIA 代碼
    if pf < 1:
        return value  # 太小，無法表示
    
    # 找到有效數字和次方
    import math
    exponent = int(math.floor(math.log10(pf)))
    mantissa = pf / (10 ** exponent)
    
    # 取前兩位有效數字
    if mantissa >= 10:
        mantissa /= 10
        exponent += 1
    
    # 轉換為兩位數
    two_digits = int(round(mantissa * 10))
    if two_digits >= 100:
        two_digits = int(two_digits / 10)
        exponent += 1
    
    # 組合 EIA 代碼
    eia_code = f"{two_digits:02d}{exponent}"
    return eia_code


def extract_generic_meas(base: str) -> Dict[str, str]:
    """
    從正規化基底文字中提取常見量測值。
    回傳提取欄位的字典（字串形式）。
    V17 修正：新增電流、電感值、溫度係數、波長、針腳數、間距、顏色、頻率、類型、法規等欄位。
    """
    s = base
    out: Dict[str, str] = {}

    # 電阻值（尋找以 R/K/M 模式結尾的標記）
    res_tokens = []
    for tok in s.split():
        if re.fullmatch(r"\d+(?:\.\d+)?[RKM]\d*", tok, re.I):
            res_tokens.append(_format_resistance(tok))
    
    # 同時檢查 DCR (mΩ/uΩ)，合併到阻值欄位
    dcr_match = DCR_RE.search(s)
    if dcr_match:
        dcr_value = f"{dcr_match.group(1)}{dcr_match.group(2)}"
        res_tokens.append(dcr_value)
    
    if res_tokens:
        out["阻值"] = shortest_res_string(res_tokens)
        # 新增 IEC 轉換欄位
        out["阻值_IEC"] = resistance_to_iec(out["阻值"])

    # 電容值（如 10UF/100NF/1PF，V17 修正：允許空格）
    cap_tokens = []
    for m in CAP_RE.finditer(s):
        cap_tokens.append(f"{m.group(1)}{m.group(2)}F")
    if cap_tokens:
        out["容量"] = shortest_cap_string(cap_tokens)
        # 新增 EIA 轉換欄位
        out["容量_EIA"] = capacitance_to_eia(out["容量"])

    # ===== V17 新增：電感值 (nH/uH/mH) =====
    m = IND_RE.search(s)
    if m:
        out["電感值"] = f"{m.group(1)}{m.group(2)}"

    # ===== V17 新增：電流 (mA/uA/A) =====
    m = CURRENT_RE.search(s)
    if m:
        out["電流"] = f"{m.group(1)}{m.group(2)}"

    # 電壓
    # 優先使用範圍電壓（如 1.65~3.6V）。
    m = VOLT_RANGE_RE.search(s)
    if m:
        out["電壓"] = f"{m.group(1)}~{m.group(2)}V"
    else:
        m = VOLT_RE.search(s)
        if m:
            out["電壓"] = f"{m.group(1)}V"
        else:
            # V17 新增：檢查安規裸電壓 (275/400/630)
            m = VOLT_BARE_RE.search(s)
            if m:
                out["電壓"] = f"{m.group(1)}V"

    # 容差 (V17 修正：支援更多符號格式)
    m = TOL_RE.search(s)
    if m:
        out["容差"] = f"{m.group(1)}%"

    # 功率
    m = PWR_RE.search(s)
    if m:
        out["功率"] = normalize_power_token(m.group(0))

    # ===== V17 新增：溫度係數 (PPM) =====
    m = TEMP_COEF_RE.search(s)
    if m:
        out["溫度係數"] = f"{m.group(1)}PPM"

    # ===== V17 新增：波長 (nm) =====
    m = WAVELENGTH_RE.search(s)
    if m:
        out["波長"] = f"{m.group(1)}nm"

    # ===== V17 新增：針腳數 (8P, 2x8P, 16PIN) =====
    m = PIN_COUNT_RE.search(s)
    if m:
        pin_str = m.group(1).replace(" ", "")
        out["針腳數"] = pin_str

    # ===== V17 新增：間距 (P=2.54mm, L=5mm) =====
    m = PITCH_RE.search(s)
    if m:
        out["間距"] = f"{m.group(1)}mm"
    else:
        m = PITCH_DIM_RE.search(s)
        if m:
            out["間距"] = m.group(0)

    # ===== V17 新增：顏色 =====
    m = COLOR_RE.search(s)
    if m:
        out["顏色"] = m.group(1).upper()

    # ===== V17 新增：頻率 (MHz/kHz/GHz) =====
    m = FREQ_RE.search(s)
    if m:
        out["頻率"] = f"{m.group(1)}{m.group(2)}"

    # ===== V17 新增：類型 (介質類型、電晶體極性等) =====
    m = TYPE_RE.search(s)
    if m:
        out["類型"] = m.group(1).upper()

    # ===== V17 新增：法規 =====
    m = COMPLIANCE_RE.search(s)
    if m:
        out["法規"] = m.group(1).upper()

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

    # CAP 的簡易介電質提取（V17 擴充溫度代碼列表）
    if cat == "CAP":
        m = re.search(r"\b(C0G|NP0|NPO|X7R|X5R|Y5V|P100|N150|N750|U2J|X6S|Z5U|X7S|X8R|Y5U)\b", base, re.I)
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
    # 使用簡單的去重機制以避免重複標記
    # （如電壓透過不同提取路徑被附加兩次）。
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

    # 常用順序（V17 擴充：新增電感值、電流、溫度係數、波長、針腳數、間距、顏色、頻率、類型、法規，新增 DCR）
    for k in ["阻值", "容量", "電感值", "DCR", "電壓", "電流", "容差", "功率", "溫度係數", "介質",
              "顏色", "頻率", "波長", "間距", "尺寸", "封裝", "針腳數", "方向", "類型", "法規"]:
        _add(t.get(k, ""))

    # 其餘欄位可附加於末尾（可選）
    return " ".join(parts).strip()


def display20(cat: str, norm: str, others: Optional[Dict[str, str]] = None, min_len: int = 8, max_len: int = 20) -> str:
    """
    建立簡潔的顯示名稱（<=20 字元）供 UI 欄位或快速瀏覽使用。
    策略：
      - 以類別作為前綴（RES/CAP/IND/IC/CN/OT）
      - 移除尾部標記的空格
      - 若太短，則使用其他可用欄位補充
    """
    prefix = PREFIX.get(cat, "OT")
    tail = norm.split(" ", 1)[1] if " " in norm else ""
    tokens = tail.split()

    # 去重尾部標記同時保持順序（防止 IC3.6V3.6V 這類輸出）。
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

    # 補充填充
    if others:
        # 僅使用 base 中尚未存在的值進行補充。
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
    除錯友善的檢視字串，顯示已提取的欄位。
    工程師可用此快速調整規則。
    """
    # V17 擴充欄位列表（新增 DCR）
    keys = ["阻值", "容量", "電感值", "DCR", "電壓", "電流", "容差", "功率", "溫度係數",
            "介質", "顏色", "頻率", "波長", "間距", "尺寸", "封裝", "針腳數", "方向", "類型", "法規"]
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
    coverage = row.get("判別比例", None)

    # 使用者要求的規則：若可識別/可判斷字元 < 15% -> 人工審核。
    # 「判別比例」是盡力估算描述中有多少內容由提取的正規化欄位所解釋。
    # 工程師日後可調整此指標。
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
# 7) 主要流水線
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

    # 1) 讀取 Excel 並偵測標頭列
    df, chosen_sheet = read_excel_auto_detect_sheet_and_header(input_path, sheet=sheet)
    original_cols = list(df.columns)  # 為了輸出整潔：保留客戶提供的原始欄位
    if verbose:
        print(f"[資訊] 使用工作表：{chosen_sheet}")

    # 2) 僅將 Description 欄位名稱標準化為 description_raw
    #    保持原始欄位不變（我們只新增欄位）。
    col_desc = find_col(df, [r"^description(_修)?$", r"描述", r"品名", r"名稱", r"name", r"title"]) or "Description"
    if col_desc not in df.columns:
        # 最後手段：嘗試原始 'Description'
        raise ValueError("無法在提供的 Excel 中找到 Description 欄位。")

    df["description_raw"] = df[col_desc].astype(str).fillna("")

    # 保留使用者友善的原始描述欄位供輸出使用（僅除錯模式）
    if debug:
        df["原始Description"] = df["description_raw"]

    # 3) 規則式正規化
    df["_BASE_"] = df["description_raw"].map(normalize_base_text)
    df["類別"] = df.apply(pick_category, axis=1)

    rows = []
    for _, row in df.iterrows():
        base = str(row.get("_BASE_", ""))
        cat = str(row.get("類別", "OT"))
        tks = extract_tokens(base, cat)

        # 覆蓋率：基底描述中有多少被提取欄位所解釋。
        # 用於將模糊資料列路由至人工審核（預設閾值 15%）。
        base_compact = re.sub(r"\s+", "", base)
        total_chars = max(len(base_compact), 1)
        explained = "".join([str(v) for v in tks.values() if v])
        explained_chars = len(re.sub(r"\s+", "", explained))
        coverage_ratio = explained_chars / total_chars

        norm = build_normalized_desc(cat, tks)
        disp = display20(cat, norm, tks)

        out_row = dict(row)  # 複製所有原始欄位 + 新增的工作欄位
        # 提取的欄位 (V17 擴充)
        out_row.update({
            "阻值": tks.get("阻值",""),
            "阻值_IEC": tks.get("阻值_IEC",""),
            "容量": tks.get("容量",""),
            "容量_EIA": tks.get("容量_EIA",""),
            "電感值": tks.get("電感值",""),
            "電壓": tks.get("電壓",""),
            "電流": tks.get("電流",""),
            "容差": tks.get("容差",""),
            "功率": tks.get("功率",""),
            "溫度係數": tks.get("溫度係數",""),
            "介質": tks.get("介質",""),
            "顏色": tks.get("顏色",""),
            "頻率": tks.get("頻率",""),
            "波長": tks.get("波長",""),
            "間距": tks.get("間距",""),
            "尺寸": tks.get("尺寸",""),
            "封裝": tks.get("封裝",""),
            "針腳數": tks.get("針腳數",""),
            "方向": tks.get("方向",""),
            "類型": tks.get("類型",""),
            "法規": tks.get("法規",""),
            "其餘規格": tks.get("其餘規格",""),
            "判別比例": round(coverage_ratio, 4),
            "正規化Description": norm,
            "顯示名20": disp,
            # 僅除錯用：協助工程師快速調整規則
            "Pipe檢視": pipe_view(cat, tks) if debug else "",
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

    # 6.5) 輸出欄位策略（交接時的重要說明）
    # 公司使用者通常需要：
    #   - 所有原始輸入欄位
    #   - 一小組穩定的正規化欄位
    # 且不需要中間除錯欄位。
    # V17 擴充欄位列表
    normalized_cols = [
        "類別",
        "阻值",
        "阻值_IEC",
        "容量",
        "容量_EIA",
        "電感值",
        "電壓",
        "電流",
        "容差",
        "功率",
        "溫度係數",
        "介質",
        "顏色",
        "頻率",
        "波長",
        "間距",
        "尺寸",
        "封裝",
        "針腳數",
        "方向",
        "類型",
        "法規",
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
        # 除非除錯模式，否則移除內部工程欄位。
        drop_internal = [c for c in ["description_raw", "原始Description", "_BASE_", "Pipe檢視"] if c in out_main.columns]
        out_main = out_main.drop(columns=drop_internal, errors="ignore")
        out_auto = out_auto.drop(columns=drop_internal, errors="ignore")
        out_review = out_review.drop(columns=drop_internal, errors="ignore")

    # 重新排序為期望的欄位順序（若某些欄位不存在則略過）。
    out_main = out_main[keep_cols]
    out_auto = out_auto[keep_cols]
    # REVIEW 工作表即使其他地方未使用，也需顯示判別比例。
    out_review = out_review[keep_cols]

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
    p.add_argument("--gui", action="store_true", help="開啟檔案選擇器以選擇 Excel 輸入。")
    p.add_argument(
        "--debug",
        action="store_true",
        help="在輸出中包含中間/除錯欄位（供工程師使用）。預設：關閉（輸出整潔）。",
    )
    p.add_argument("--verbose", action="store_true", help="印出進度日誌。")
    return p


def main() -> None:
    args = build_arg_parser().parse_args()

    # 解析輸入路徑（含退回機制）
    input_arg = args.input_pos or args.input

    # 1) GUI 檔案選擇器優先順序最高（應使用者要求的公司用途）。
    if args.gui:
        picked = pick_excel_file_via_dialog(initial_dir=Path.cwd())
        if picked is None:
            raise SystemExit("未選擇檔案（已取消）。")
        input_path = picked.resolve()
    # 2) CLI 輸入
    elif input_arg:
        input_path = Path(input_arg).expanduser().resolve()
    # 3) 未提供輸入：先嘗試 GUI（若可用），否則自動選取最新的 Excel
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
                    "未提供輸入且當前資料夾未找到 Excel 檔案。\n"
                    "請執行：python run_bom_pipeline.py --gui  (建議)\n"
                    "或：python run_bom_pipeline.py --input <BOM.xlsx>"
                )
            input_path = candidates[0].resolve()
            if args.verbose:
                print(f"[資訊] 自動選取輸入：{input_path}")

    out_dir = Path(args.out_dir).expanduser().resolve()
    sheet = args.sheet
    model_dir = Path(args.model_dir).expanduser().resolve() if args.model_dir else None

    if not input_path.exists():
        raise FileNotFoundError(f"找不到輸入檔案：{input_path}")
    if model_dir and not model_dir.exists():
        # 公司友善行為：不要崩潰；繼續以純規則模式執行。
        print(f"[警告] 找不到模型目錄；以純規則模式執行：{model_dir}")
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
