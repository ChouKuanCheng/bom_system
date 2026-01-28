# ===== run_bom_v2.py =====
# 改版重點：
# 1. Category_raw -> Category 主分類正規化
# 2. 結構性空白清理（不改單位大小寫）
# 3. Resistance_IEC（IEC A）與 Capacitance_EIA（EIA）
# 4. 溫度代碼去空白 + canonical + Code20 專用欄
# ========================

import re
import argparse
from pathlib import Path
from typing import Dict, Any

import yaml
import pandas as pd
import torch
from transformers import AutoTokenizer, AutoModelForTokenClassification


# ---------- YAML 載入 ----------
def load_yaml(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------- NER 前處理 ----------
def apply_text_replacements(text: str, normalize_cfg: Dict[str, Any]) -> str:
    out = text or ""
    for r in normalize_cfg.get("text_replacements", []):
        if isinstance(r, dict) and r.get("pattern"):
            flags = re.IGNORECASE if "i" in r.get("flags", "") else 0
            out = re.sub(r["pattern"], r.get("replace", ""), out, flags=flags)
    return out


# ---------- NER 推論 ----------
def ner_predict(text, tokenizer, model, device):
    enc = tokenizer(
        text,
        return_tensors="pt",
        truncation=True,
        max_length=512,
        return_offsets_mapping=True,
    )
    offsets = enc.pop("offset_mapping")[0].tolist()
    enc = {k: v.to(device) for k, v in enc.items()}

    with torch.no_grad():
        out = model(**enc)
        probs = torch.softmax(out.logits[0], dim=-1)
        confs, preds = torch.max(probs, dim=-1)

    tokens = tokenizer.convert_ids_to_tokens(enc["input_ids"][0].tolist())
    id2label = model.config.id2label

    results = []
    for tok, pid, cf, (s, e) in zip(tokens, preds.tolist(), confs.tolist(), offsets):
        if tok in (tokenizer.cls_token, tokenizer.sep_token, tokenizer.pad_token):
            continue
        if s == e == 0 and tok.startswith("["):
            continue
        results.append((tok, id2label[int(pid)], float(cf)))
    return results


def merge_wordpieces(items):
    """合併 wordpiece（如 '##' 開頭的 token）為完整字串。"""
    merged = []
    buf_t, buf_l, buf_c = "", None, []
    def flush():
        nonlocal buf_t, buf_l, buf_c
        if buf_t:
            merged.append((buf_t, buf_l or "O", sum(buf_c)/len(buf_c)))
        buf_t, buf_l, buf_c = "", None, []
    for t,l,c in items:
        if t.startswith("##"):
            buf_t += t[2:]
            buf_c.append(c)
        else:
            flush()
            buf_t, buf_l, buf_c = t, l, [c]
    flush()
    return merged


def aggregate_fields(merged):
    """聚合欄位：將相同標籤的 token 串接。"""
    fields = {}
    confs = []
    for t,l,c in merged:
        confs.append(c)
        if l in ("O","IGNORE"):
            continue
        fields.setdefault(l, []).append(t)
    out = {k: " ".join(v) for k,v in fields.items()}
    return out, (sum(confs)/len(confs) if confs else 0.0)


# ---------- 後處理 ----------
def cleanup_spacing(val: str) -> str:
    """清理結構性空白（不改變單位大小寫）。"""
    if not val:
        return val
    val = re.sub(r"\s*/\s*", "/", val)
    val = re.sub(r"\s*\.\s*", ".", val)
    val = re.sub(r"\s*%\s*", "%", val)
    val = re.sub(r"(\d)\s+([A-Za-zΩµ])", r"\1\2", val)
    val = re.sub(r"([A-Za-zΩµ])\s+(\d)", r"\1\2", val)
    return val.strip()


def normalize_category(fields: Dict[str,str]) -> Dict[str,str]:
    """Category_raw -> Category 主分類正規化。"""
    out = dict(fields)
    raw = out.get("Category","")
    out["Category_raw"] = raw
    s = raw.lower()
    if "res" in s:
        out["Category"] = "RES"
    elif "cap" in s:
        out["Category"] = "CAP"
    elif "ind" in s:
        out["Category"] = "IND"
    elif any(k in s for k in ["conn","header","cn"]):
        out["Category"] = "CONNECTOR"
    elif "led" in s:
        out["Category"] = "LED"
    elif any(k in s for k in ["xtal","crystal","osc"]):
        out["Category"] = "CRYSTAL"
    elif any(k in s for k in ["ic","mcu","fpga"]):
        out["Category"] = "IC"
    else:
        out["Category"] = "UNKNOWN"
    return out


def resistance_to_iec(val: str) -> str:
    """將電阻值轉換為 IEC 標準格式（如 4K7）。"""
    if not val:
        return ""
    v = val.replace("Ω","").replace("ohm","").strip()
    try:
        if "m" in v:
            num = float(v.replace("m",""))/1000
        elif "k" in v.lower():
            num = float(v.lower().replace("k",""))*1e3
        elif "M" in v:
            num = float(v.replace("M",""))*1e6
        else:
            num = float(v)
    except:
        return ""
    if num < 1:
        return f"{num:.3f}".rstrip("0").rstrip(".").replace(".","R")
    if num < 1000:
        return f"{num:g}".replace(".","R")
    if num < 1e6:
        return f"{int(num/1e3)}K"
    return f"{int(num/1e6)}M"


def capacitance_to_eia(val: str) -> str:
    """將電容值轉換為 EIA 標準代碼（如 104）。"""
    if not val:
        return ""
    v = val.lower().replace("f","")
    try:
        if "uf" in v:
            pf = float(v.replace("uf",""))*1e6
        elif "nf" in v:
            pf = float(v.replace("nf",""))*1e3
        elif "pf" in v:
            pf = float(v.replace("pf",""))
        else:
            return ""
    except:
        return ""
    s = str(int(pf))
    return s if len(s)<=2 else s[:2]+str(len(s)-2)


# ---------- 主程式 ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-i","--input",required=True, help="輸入 BOM Excel 路徑")
    ap.add_argument("-o","--output",required=True, help="輸出 Excel 路徑")
    ap.add_argument("--desc-col",default="Description", help="描述欄位名稱")
    ap.add_argument("--model-dir",required=True, help="NER 模型資料夾路徑")
    ap.add_argument("--rules-dir",required=True, help="規則設定檔資料夾路徑")
    args = ap.parse_args()

    rules = Path(args.rules_dir)
    normalize_cfg = load_yaml(rules/"normalize.yaml")
    temp_cfg = load_yaml(rules/"temp_coefficient.yaml")

    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    tokenizer = AutoTokenizer.from_pretrained(args.model_dir)
    model = AutoModelForTokenClassification.from_pretrained(args.model_dir).to(device)
    model.eval()

    df = pd.read_excel(args.input)
    rows = []

    for _,r in df.iterrows():
        text = apply_text_replacements(str(r.get(args.desc_col,"")), normalize_cfg)
        ner = ner_predict(text, tokenizer, model, device)
        merged = merge_wordpieces(ner)
        fields, conf = aggregate_fields(merged)

        # 結構性空白清理
        for k in list(fields.keys()):
            fields[k] = cleanup_spacing(fields[k])

        # 溫度係數處理
        if "Temp_Coefficient" in fields:
            fields["Temp_Coefficient_raw"] = fields["Temp_Coefficient"]
            fields["Temp_Coefficient"] = re.sub(r"\s+","",fields["Temp_Coefficient"])
            if fields["Temp_Coefficient"].upper() == "C0G":
                fields["Temp_Coefficient"] = "NP0"
            fields["Temp_Code20"] = fields["Temp_Coefficient"]

        # 類別正規化
        fields = normalize_category(fields)

        # IEC / EIA 轉換
        fields["Resistance_IEC"] = resistance_to_iec(fields.get("Resistance",""))
        fields["Capacitance_EIA"] = capacitance_to_eia(fields.get("Capacitance",""))

        out = dict(r)
        out.update(fields)
        out["__overall_conf__"] = conf
        rows.append(out)

    pd.DataFrame(rows).to_excel(args.output,index=False)
    print(f"已儲存：{args.output}")


if __name__ == "__main__":
    main()
