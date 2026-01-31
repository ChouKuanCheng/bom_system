#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
Pipeline 輸出轉訓練資料工具
================================================================================

【用途】
將 run_bom_pipelineV2.py 輸出的 REVIEW 檔案，
經廠商人工審核修正後，轉換為 NER 模型訓練格式。

【工作流程】
1. Pipeline 處理 → 產出 _REVIEW.xlsx
2. 廠商人工審核 → 修正欄位值
3. 本工具轉換 → 產出訓練格式的 Excel
4. 訓練腳本重新訓練 → 模型效能提升

【使用方式】

    python convert_to_training_data.py --input "審核完成.xlsx" --output "新訓練資料.xlsx"

================================================================================
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import List, Tuple

import pandas as pd


# =============================================================================
# 欄位名稱對照（中文 → NER 標籤）
# =============================================================================
FIELD_TO_LABEL = {
    "類別": "Category",
    "阻值": "Resistance",
    "阻值_IEC": None,  # 衍生欄位，不納入訓練
    "容量": "Capacitance",
    "容量_EIA": None,  # 衍生欄位，不納入訓練
    "電感值": "Inductance",
    "電壓": "Voltage",
    "電流": "Current",
    "容差": "Tolerance",
    "功率": "Power",
    "溫度係數": "Temp_Coefficient",
    "介質": "Temp_Code",
    "顏色": "Color",
    "頻率": "Frequency",
    "波長": "Wavelength",
    "間距": "Size",  # 間距通常與尺寸合併
    "尺寸": "Size",
    "封裝": "Package",
    "針腳數": "Pin_Count",
    "方向": "Type",
    "類型": "Type",
    "法規": "Compliance",
    "製程": "Process_Type",
}


def simple_tokenize(text: str) -> List[str]:
    """
    簡易分詞器（與訓練腳本一致）
    """
    if not isinstance(text, str):
        return []
    tokens = []
    for m in re.finditer(r'\S+', text):
        segment = m.group()
        for sub in re.finditer(r'[A-Za-z0-9.+/%Ωµ]+|[^A-Za-z0-9.+/%Ωµ]', segment):
            tokens.append(sub.group())
    return tokens


def build_labels_from_fields(tokens: List[str], row: pd.Series) -> List[str]:
    """
    根據 tokens 和欄位值建立 labels
    
    策略：
    1. 對每個 token，檢查它是否出現在任何已知欄位值中
    2. 若找到匹配，賦予對應的 NER 標籤
    3. 若未找到，標記為 "O"（其他）或 "IGNORE"（符號）
    """
    labels = []
    
    # 建立欄位值 → 標籤的映射
    value_to_label = {}
    for field_name, ner_label in FIELD_TO_LABEL.items():
        if ner_label is None:
            continue
        value = str(row.get(field_name, "")).strip()
        if value:
            # 將欄位值分詞後，每個 token 都對應到該標籤
            field_tokens = simple_tokenize(value)
            for ft in field_tokens:
                if ft not in value_to_label:
                    value_to_label[ft.upper()] = ner_label
    
    # 對每個 token 尋找匹配
    for tok in tokens:
        tok_upper = tok.upper()
        
        # 檢查是否為符號
        if re.match(r'^[,;:\(\)\[\]\/\-\+\*\&\|\!\?\.\s]+$', tok):
            labels.append("IGNORE")
        # 檢查是否在已知欄位值中
        elif tok_upper in value_to_label:
            labels.append(value_to_label[tok_upper])
        # 檢查部分匹配（例如 "10K" 包含在 "10KΩ"）
        else:
            found = False
            for val, lbl in value_to_label.items():
                if tok_upper in val or val in tok_upper:
                    labels.append(lbl)
                    found = True
                    break
            if not found:
                labels.append("O")
    
    return labels


def convert_row(row: pd.Series, desc_col: str = "正規化Description") -> Tuple[str, str]:
    """
    將一筆資料轉換為訓練格式
    
    Returns:
        (description_raw, labels_str)
    """
    # 取得原始描述
    desc = str(row.get(desc_col, "")).strip()
    if not desc:
        desc = str(row.get("description_raw", "")).strip()
    
    # 分詞
    tokens = simple_tokenize(desc)
    
    # 建立標籤
    labels = build_labels_from_fields(tokens, row)
    
    # 確保長度一致
    if len(tokens) != len(labels):
        # 修正長度不一致
        labels = labels[:len(tokens)] + ["O"] * (len(tokens) - len(labels))
    
    return desc, str(labels)


def convert_excel(input_path: Path, output_path: Path, desc_col: str = "正規化Description"):
    """
    轉換整個 Excel 檔案
    """
    print(f"📥 讀取檔案：{input_path}")
    df = pd.read_excel(input_path)
    print(f"   共 {len(df)} 筆資料")
    
    results = []
    skipped = 0
    
    for idx, row in df.iterrows():
        try:
            desc, labels_str = convert_row(row, desc_col)
            if desc and labels_str != "[]":
                results.append({
                    "Description": desc,
                    "Labels": labels_str,
                })
            else:
                skipped += 1
        except Exception as e:
            print(f"   ⚠️ 第 {idx+1} 筆跳過：{e}")
            skipped += 1
    
    out_df = pd.DataFrame(results)
    out_df.to_excel(output_path, index=False)
    
    print(f"✅ 完成！")
    print(f"   輸出：{output_path}")
    print(f"   成功：{len(results)} 筆")
    print(f"   跳過：{skipped} 筆")


def main():
    parser = argparse.ArgumentParser(
        description="將 Pipeline 輸出轉換為 NER 訓練資料格式"
    )
    parser.add_argument(
        "--input", "-i",
        required=True,
        help="輸入的 Excel 檔案（廠商審核完成的 _REVIEW.xlsx）"
    )
    parser.add_argument(
        "--output", "-o",
        default=None,
        help="輸出的 Excel 檔案（訓練格式）。預設：<輸入檔名>_training.xlsx"
    )
    parser.add_argument(
        "--desc_col",
        default="正規化Description",
        help="描述欄位名稱。預設：正規化Description"
    )
    
    args = parser.parse_args()
    
    input_path = Path(args.input).expanduser().resolve()
    if not input_path.exists():
        raise FileNotFoundError(f"找不到輸入檔案：{input_path}")
    
    if args.output:
        output_path = Path(args.output).expanduser().resolve()
    else:
        output_path = input_path.parent / f"{input_path.stem}_training.xlsx"
    
    convert_excel(input_path, output_path, args.desc_col)


if __name__ == "__main__":
    main()
