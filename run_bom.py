import re
import argparse
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional

import yaml
import pandas as pd

import torch
from transformers import AutoTokenizer, AutoModelForTokenClassification


# -----------------------
# YAML 載入工具
# -----------------------
def load_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# -----------------------
# 文字正規化
# -----------------------
def apply_text_replacements(text: str, normalize_cfg: Dict[str, Any]) -> str:
    """
    在 NER 處理前套用 normalize.yaml 的文字層級替換規則。
    預期結構範例：
      text_replacements:
        - {pattern: "C0G", replace: "NP0"}
        - {pattern: "SMD", replace: "SMT"}
    """
    if not text:
        return ""
    rules = normalize_cfg.get("text_replacements", [])
    out = text
    for r in rules:
        pattern = r.get("pattern")
        repl = r.get("replace", "")
        flags = r.get("flags", "")
        re_flags = re.IGNORECASE if "i" in flags else 0
        if pattern:
            out = re.sub(pattern, repl, out, flags=re_flags)
    return out


def to_ascii(s: str) -> str:
    """將 Code20 轉為 ASCII 安全格式。根據需求將 Ω 對應為 R 等。"""
    if s is None:
        return ""
    # 常見對應規則
    s = s.replace("Ω", "R")
    s = s.replace("ohm", "R").replace("Ohm", "R").replace("OHM", "R")
    # 防禦性移除剩餘的非 ASCII 字元
    s = s.encode("ascii", errors="ignore").decode("ascii")
    return s


# -----------------------
# NER 推論
# -----------------------
def ner_predict(
    text: str,
    tokenizer: AutoTokenizer,
    model: AutoModelForTokenClassification,
    device: torch.device,
) -> Tuple[List[str], List[str], List[float]]:
    """
    回傳值：tokens（標記）、labels（標籤）、每個 token 的信心度（簡單 softmax 最大機率）。
    使用 tokenizer 的 wordpiece tokens。
    """
    if not text:
        return [], [], []

    enc = tokenizer(
        text,
        return_tensors="pt",
        truncation=True,
        max_length=512,
        return_offsets_mapping=True,
    )
    offset_mapping = enc.pop("offset_mapping")[0].tolist()  # 每個 token 的 (start, end)
    enc = {k: v.to(device) for k, v in enc.items()}

    with torch.no_grad():
        out = model(**enc)
        logits = out.logits[0]  # [seq_len, num_labels]
        probs = torch.softmax(logits, dim=-1)
        confs, pred_ids = torch.max(probs, dim=-1)

    tokens = tokenizer.convert_ids_to_tokens(enc["input_ids"][0].tolist())
    id2label = model.config.id2label
    labels = [id2label[int(i)] for i in pred_ids.cpu().tolist()]
    conf_list = confs.cpu().tolist()

    # 移除特殊標記（[CLS]、[SEP]、[PAD]）
    cleaned_tokens, cleaned_labels, cleaned_confs = [], [], []
    for tok, lab, cf, (st, en) in zip(tokens, labels, conf_list, offset_mapping):
        if tok in (tokenizer.cls_token, tokenizer.sep_token, tokenizer.pad_token):
            continue
        # 忽略偏移量為空的 token（可能發生）
        if st == en == 0 and tok.startswith("["):
            continue
        cleaned_tokens.append(tok)
        cleaned_labels.append(lab)
        cleaned_confs.append(float(cf))

    return cleaned_tokens, cleaned_labels, cleaned_confs


# -----------------------
# 聚合：token 標籤 -> 欄位字典
# -----------------------
def _merge_wordpieces(tokens: List[str], labels: List[str], confs: List[float]) -> List[Tuple[str, str, float]]:
    """
    合併 wordpiece（如 '##' 開頭的 token）為完整字串。
    回傳 (text, label, avg_conf) 的列表。
    """
    merged: List[Tuple[str, str, float]] = []
    buff_txt, buff_lab, buff_confs = "", None, []

    def flush():
        nonlocal buff_txt, buff_lab, buff_confs
        if buff_txt:
            merged.append((buff_txt, buff_lab or "O", sum(buff_confs) / max(len(buff_confs), 1)))
        buff_txt, buff_lab, buff_confs = "", None, []

    for tok, lab, cf in zip(tokens, labels, confs):
        if tok.startswith("##"):
            # 延續前一個 token
            buff_txt += tok[2:]
            buff_confs.append(cf)
            continue
        # 新 token 開始
        flush()
        buff_txt = tok
        buff_lab = lab
        buff_confs = [cf]
    flush()
    # 清理 token 中的人工符號
    merged2 = []
    for t, l, c in merged:
        t = t.replace("▁", "")  # 以防萬一
        merged2.append((t, l, c))
    return merged2


def aggregate_fields(merged_tokens: List[Tuple[str, str, float]]) -> Tuple[Dict[str, str], float]:
    """
    簡易聚合邏輯：
      - 將相同標籤的 token 串接
      - 保留最佳信心度的標籤區段
    回傳欄位字典 + 整體信心度估計值。
    """
    fields: Dict[str, List[Tuple[str, float]]] = {}
    overall_confs = []

    for txt, lab, cf in merged_tokens:
        overall_confs.append(cf)
        if lab in ("O", "IGNORE"):
            continue
        fields.setdefault(lab, []).append((txt, cf))

    # 合併每個標籤的 tokens
    out: Dict[str, str] = {}
    for lab, items in fields.items():
        # 對於單位連接的 token 用 '' 合併，否則用空格——這裡使用簡單啟發式
        parts = [t for t, _ in items]
        val = " ".join(parts)
        val = val.replace(" ,", ",").replace(" .", ".")
        out[lab] = val.strip()

    overall_conf = float(sum(overall_confs) / max(len(overall_confs), 1)) if overall_confs else 0.0
    return out, overall_conf


# -----------------------
# 後處理規則與欄位正規化
# -----------------------
def normalize_fields(fields: Dict[str, str], normalize_cfg: Dict[str, Any], temp_cfg: Dict[str, Any]) -> Dict[str, str]:
    """
    套用 normalize.yaml 的欄位層級正規化。
    同時套用溫度係數標準化（C0G->NP0 等）。
    """
    out = dict(fields)

    # 欄位層級正規表達式替換
    field_rules = normalize_cfg.get("field_normalizations", [])
    for r in field_rules:
        fld = r.get("field")
        pattern = r.get("pattern")
        repl = r.get("replace", "")
        flags = r.get("flags", "")
        re_flags = re.IGNORECASE if "i" in flags else 0
        if fld and pattern and fld in out and out[fld]:
            out[fld] = re.sub(pattern, repl, out[fld], flags=re_flags).strip()

    # 使用 temp_coefficient.yaml 進行溫度代碼標準化
    # 預期 temp_cfg 格式如：
    #  codes:
    #    NP0: {aliases: ["NP0","C0G"], ...}
    temp_codes = temp_cfg.get("codes", {})
    if "Temp_Coefficient" in out and out["Temp_Coefficient"]:
        raw = out["Temp_Coefficient"].strip().upper()
        canonical = None
        for canon, info in temp_codes.items():
            aliases = [canon] + [a.upper() for a in info.get("aliases", [])]
            if raw in aliases:
                canonical = canon.upper()
                break
        if canonical:
            out["Temp_Coefficient"] = canonical

    # 製程原始值 + 正規化值
    proc_raw = out.get("Process_Type", "")
    if proc_raw:
        out["Process_Type_raw"] = proc_raw
        proc_map = normalize_cfg.get("process_map", {})
        out["Process_Type"] = proc_map.get(proc_raw.upper(), proc_raw.upper())
    else:
        out["Process_Type_raw"] = ""

    return out


def apply_postrules(fields: Dict[str, str], patterns_cfg: Dict[str, Any]) -> Dict[str, str]:
    """
    patterns.yaml 邏輯的掛鉤點。
    MVP 版本：保持簡單安全。可迭代新增更多規則。
    """
    out = dict(fields)

    # 範例：若 Package 包含 SOD-323HE -> SOD-323（備用規則）
    pkg = out.get("Package", "")
    if pkg:
        m = re.search(r"(SOD-\d+)", pkg, flags=re.IGNORECASE)
        if m:
            out["Package"] = m.group(1).upper()
        m2 = re.search(r"(SOT-?\d+|SOT\d+)", pkg, flags=re.IGNORECASE)
        if m2 and "SOD" not in out["Package"]:
            out["Package"] = m2.group(1).upper().replace("-", "")

    # 為 RES/CAP + Size 推斷 CHIP 封裝（規則 1）
    cat = out.get("Category", "").upper()
    size = out.get("Size", "").upper() or out.get("Package_Size", "").upper()
    if cat in ("RES", "CAP") and size in ("01005","0201","0402","0603","0805","1608","3216"):
        out["Package"] = "CHIP"
        out["Package_Size"] = size

    # 若 Size 存在但 Package_Size 為空，則填入
    if out.get("Size") and not out.get("Package_Size"):
        out["Package_Size"] = out["Size"].upper()

    return out


# -----------------------
# Name20 / Code20 生成
# -----------------------
def build_name20(fields: Dict[str, str], templates_cfg: Dict[str, Any]) -> str:
    cat = fields.get("Category", "").upper()
    tpls = templates_cfg.get("name20", {})
    tpl = tpls.get(cat) or tpls.get("DEFAULT", "")
    if not tpl:
        return ""

    # 填入變數
    ctx = {
        "res": fields.get("Resistance", ""),
        "cap": fields.get("Capacitance", ""),
        "ind": fields.get("Inductance", ""),
        "pwr": fields.get("Power", ""),
        "tol": fields.get("Tolerance", ""),
        "pkg": fields.get("Package", ""),
        "size": fields.get("Package_Size", "") or fields.get("Size", ""),
        "proc": fields.get("Process_Type_raw", "") or fields.get("Process_Type", ""),
        "temp": fields.get("Temp_Coefficient", ""),
        "volt": fields.get("Voltage", ""),
        "pin": fields.get("Pin_Count", ""),
        "freq": fields.get("Frequency", ""),
        "type": fields.get("Type", ""),
    }

    s = tpl.format(**ctx)
    s = re.sub(r"\s+", "", s)  # 移除空格
    # 強制限制 20 字元。後續可實作更智慧的壓縮方式。
    return s[:20]


def build_code20(fields: Dict[str, str], templates_cfg: Dict[str, Any], category_schema: Dict[str, Any]) -> str:
    cat = fields.get("Category", "").upper()
    cat_spec = (category_schema.get("categories") or {}).get(cat, {})
    # IC 不產生 Code20
    if cat == "IC" or not cat_spec.get("enable_code20", False):
        return ""

    tpl = (templates_cfg.get("code20", {}) or {}).get(cat) or (templates_cfg.get("code20", {}) or {}).get("DEFAULT", "")
    if not tpl:
        return ""

    # 對應表（容差代碼、功率代碼、製程代碼）
    maps = templates_cfg.get("mappings", {})

    tol = fields.get("Tolerance", "")
    tol_code = maps.get("tolerance_code", {}).get(tol, "")
    pwr = fields.get("Power", "")
    pwr_code = maps.get("power_code", {}).get(pwr, "")
    proc_norm = fields.get("Process_Type", "")
    proc_code = maps.get("process_code", {}).get(proc_norm, "")

    # 值正規化為 ASCII
    res_ascii = to_ascii(fields.get("Resistance", ""))
    cap_ascii = to_ascii(fields.get("Capacitance", ""))
    ind_ascii = to_ascii(fields.get("Inductance", ""))
    temp_ascii = to_ascii(fields.get("Temp_Coefficient", ""))

    ctx = {
        "cat": cat,
        "res": res_ascii,
        "cap": cap_ascii,
        "ind": ind_ascii,
        "pwr": pwr_code,
        "tol": tol_code,
        "size": to_ascii(fields.get("Package_Size", "") or fields.get("Size", "")),
        "proc": proc_code,
        "temp": temp_ascii,  # Q3=C：temp 一律包含在 Code20 中
        # Compliance 刻意不包含
    }

    code = tpl.format(**ctx)
    code = to_ascii(code)
    code = re.sub(r"[^A-Za-z0-9]+", "", code)  # 僅保留英數字
    return code[:20]


# -----------------------
# 主程式
# -----------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-i", "--input", required=True, help="輸入 BOM Excel 路徑")
    ap.add_argument("-o", "--output", required=True, help="輸出 Excel 路徑")
    ap.add_argument("--sheet", default=0, help="工作表索引或名稱（預設 0）")
    ap.add_argument("--desc-col", default="Description", help="描述欄位名稱")
    ap.add_argument("--model-dir", required=True, help="distilbert_ner_final 資料夾路徑")
    ap.add_argument("--rules-dir", required=True, help="包含 YAML 設定檔的 rules 資料夾路徑")
    args = ap.parse_args()

    rules_dir = Path(args.rules_dir)
    category_schema = load_yaml(rules_dir / "category_schema.yaml")
    normalize_cfg = load_yaml(rules_dir / "normalize.yaml")
    patterns_cfg = load_yaml(rules_dir / "patterns.yaml")
    temp_cfg = load_yaml(rules_dir / "temp_coefficient.yaml")
    templates_cfg = load_yaml(rules_dir / "templates.yaml")

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    tokenizer = AutoTokenizer.from_pretrained(args.model_dir)
    model = AutoModelForTokenClassification.from_pretrained(args.model_dir).to(device)
    model.eval()

    df = pd.read_excel(args.input, sheet_name=args.sheet)
    if args.desc_col not in df.columns:
        raise ValueError(f"找不到描述欄位 '{args.desc_col}'。現有欄位：{list(df.columns)}")

    # 準備輸出欄位
    out_rows = []
    for _, row in df.iterrows():
        raw_desc = str(row.get(args.desc_col, "") or "")
        norm_desc = apply_text_replacements(raw_desc, normalize_cfg)

        tokens, labels, confs = ner_predict(norm_desc, tokenizer, model, device)
        merged = _merge_wordpieces(tokens, labels, confs)
        fields, overall_conf = aggregate_fields(merged)

        # 正規化 + 後處理規則
        fields = normalize_fields(fields, normalize_cfg, temp_cfg)
        fields = apply_postrules(fields, patterns_cfg)

        # 衍生輸出
        name20 = build_name20(fields, templates_cfg)
        code20 = build_code20(fields, templates_cfg, category_schema)

        # 展平為資料列輸出
        out = dict(row)
        out["__desc_norm__"] = norm_desc
        out["__overall_conf__"] = overall_conf
        # 核心欄位（可依需求新增更多）
        for k in [
            "Category","Type","Resistance","Capacitance","Inductance","Power","Tolerance",
            "Package","Package_Size","Pin_Count","Process_Type_raw","Process_Type",
            "Voltage","Current","Frequency","Temp_Coefficient","Compliance",
        ]:
            out[k] = fields.get(k, "")
        out["Name20"] = name20
        out["Code20"] = code20

        out_rows.append(out)

    out_df = pd.DataFrame(out_rows)
    out_df.to_excel(args.output, index=False)
    print(f"已儲存：{args.output}")


if __name__ == "__main__":
    main()
