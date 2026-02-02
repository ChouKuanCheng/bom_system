# -----------------------------------------------------------
# DistilBERT (NER) 10-fold + Test + Shuffled Test 單一腳本 (進階指標版)
#
# 新增功能：
# 1. Per-label TP / FP / FN / TN / Specificity / Support
# 2. Macro / Micro / Weighted F1
# 3. Cohen’s Kappa Score
# -----------------------------------------------------------

import os
import re
import datetime
import random
import warnings

import numpy as np
import math
import pandas as pd
import torch
from torch.utils.data import DataLoader, Dataset
from torch.optim import AdamW

from transformers import (
    DistilBertTokenizerFast,
    DistilBertConfig,
    DistilBertForTokenClassification
)
from sklearn.model_selection import KFold
from sklearn.metrics import precision_recall_fscore_support, cohen_kappa_score, accuracy_score, roc_auc_score
from sklearn.preprocessing import label_binarize
from tqdm import tqdm
import matplotlib.pyplot as plt

# 忽略一些 sklearn 的除以零警告 (在某些類別沒預測到時會發生)
warnings.filterwarnings('ignore') 

# ========= 0. 隨機種子（讓結果可重現） =========

def set_seed(seed: int = 42):
    random.seed(seed)
    np.random.seed(seed)
    torch.manual_seed(seed)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(seed)


# ========= 1. 簡易 tokenizer =========

def simple_tokenize(text: str):
    if not isinstance(text, str):
        return []
    tokens = []
    for m in re.finditer(r'\S+', text):
        segment = m.group()
        for sub in re.finditer(r'[A-Za-z0-9.+/%Ωµ]+|[^A-Za-z0-9.+/%Ωµ]', segment):
            tokens.append(sub.group())
    return tokens


# ========= 2. 資料增強 =========

synonym_dict = {
    "Ω": ["Ohm", "ohm"],
    "%": ["percent", "Percent"],
    "V": ["Volt", "volt"],
    "A": ["Ampere", "ampere"],
    "F": ["Farad", "farad"],
}

def augment_token(token):
    for key, synonyms in synonym_dict.items():
        if key in token and random.random() < 0.3:
            token = token.replace(key, random.choice(synonyms))
            break
    if token.isalpha() and random.random() < 0.2:
        token = token.lower() if random.random() < 0.5 else token.upper()
    if len(token) > 2 and random.random() < 0.1:
        pos = random.randint(1, len(token) - 1)
        token = token[:pos] + " " + token[pos:]
    return token

def augment_sentence(tokens, tags, augment_rate=0.5):
    if random.random() > augment_rate:
        return tokens, tags
    augmented_tokens = [augment_token(tok) for tok in tokens]
    if random.random() < 0.2:
        combined = list(zip(augmented_tokens, tags))
        random.shuffle(combined)
        augmented_tokens, tags = zip(*combined)
        augmented_tokens = list(augmented_tokens)
        tags = list(tags)
    return augmented_tokens, tags


# ========= 3. Dataset 類別 =========

class NERDataset(Dataset):
    def __init__(self, texts, tags, tokenizer, tag2id, max_len=64, augment=False, augment_rate=0.5):
        self.texts = texts
        self.tags = tags
        self.tokenizer = tokenizer
        self.tag2id = tag2id
        self.max_len = max_len
        self.augment = augment
        self.augment_rate = augment_rate

    def __len__(self):
        return len(self.texts)

    def __getitem__(self, idx):
        try:
            text = eval(str(self.texts[idx]))
            tags = eval(str(self.tags[idx]))
        except Exception:
            return {
                "input_ids": torch.zeros(self.max_len, dtype=torch.long),
                "attention_mask": torch.zeros(self.max_len, dtype=torch.long),
                "labels": torch.full((self.max_len,), -100, dtype=torch.long),
            }

        if self.augment:
            text, tags = augment_sentence(text, tags, augment_rate=self.augment_rate)

        encoding = self.tokenizer(
            text,
            is_split_into_words=True,
            return_offsets_mapping=True,
            padding="max_length",
            truncation=True,
            max_length=self.max_len,
            return_tensors="pt",
        )

        labels = [-100] * self.max_len
        word_ids = encoding.word_ids(batch_index=0)
        current_word_idx = -1
        for i, word_idx in enumerate(word_ids):
            if word_idx is not None:
                if word_idx != current_word_idx:
                    current_word_idx = word_idx
                    if word_idx < len(tags):
                        labels[i] = self.tag2id.get(tags[word_idx], -100)
                else:
                    if word_idx < len(tags):
                        labels[i] = self.tag2id.get(tags[word_idx], -100)

        return {
            "input_ids": encoding["input_ids"].squeeze(),
            "attention_mask": encoding["attention_mask"].squeeze(),
            "labels": torch.tensor(labels, dtype=torch.long),
        }


# ========= 4. 輔助函式 =========

def extract_labels(label_str: str):
    if not isinstance(label_str, str):
        return []
    return re.findall(r"'([^']*)'", label_str)

def build_ner_df(raw_df):
    processed_rows = []
    skipped = 0
    for _, row in raw_df.iterrows():
        desc = row["Description"]
        labels_str = row["Labels"]
        tok_list = simple_tokenize(desc)
        tag_list = extract_labels(labels_str)
        if len(tok_list) != len(tag_list):
            skipped += 1
            continue
        processed_rows.append({
            "tokens": str(tok_list),
            "tags": str(tag_list),
        })
    ner_df = pd.DataFrame(processed_rows).reset_index(drop=True)
    return ner_df, skipped


# ========= 5. [新增] 詳細指標計算函式 =========

def compute_token_level_metrics(
    y_true,
    y_pred,
    y_prob,  # shape: (N, C) softmax probs aligned to y_true/y_pred
    tag2id,
    exclude_tags=("IGNORE",),
):
    """Compute token-level metrics for multi-class NER with explicit exclusion of IGNORE/padding.

    Returns:
        per_label_df: columns [label, tp, fp, fn, tn, specificity, precision, recall, f1, support]
        overall: dict with accuracy, micro_f1, weighted_f1, kappa, auc_micro, auc_weighted, specificity_weighted_ovr
    """
    id2tag = {v: k for k, v in tag2id.items()}

    y_true = np.asarray(y_true, dtype=int)
    y_pred = np.asarray(y_pred, dtype=int)
    y_prob = np.asarray(y_prob) if y_prob is not None else None

    # labels to include in evaluation
    eval_label_ids = [lid for lid, name in id2tag.items() if name not in set(exclude_tags)]
    eval_label_ids = sorted(eval_label_ids)

    # Filter arrays to only tokens whose true label is in eval_label_ids
    # (This ensures IGNORE tokens are excluded even if they appear in y_true.)
    eval_mask = np.isin(y_true, eval_label_ids)
    y_true_f = y_true[eval_mask]
    y_pred_f = y_pred[eval_mask]
    y_prob_f = y_prob[eval_mask] if y_prob is not None else None

    # Overall metrics
    accuracy = accuracy_score(y_true_f, y_pred_f) if len(y_true_f) else 0.0
    # Micro F1 for multi-class = accuracy when computed on hard labels, but keep explicit for reporting
    _, _, micro_f1, _ = precision_recall_fscore_support(
        y_true_f, y_pred_f, labels=eval_label_ids, average="micro", zero_division=0
    )
    _, _, weighted_f1, _ = precision_recall_fscore_support(
        y_true_f, y_pred_f, labels=eval_label_ids, average="weighted", zero_division=0
    )
    kappa = cohen_kappa_score(y_true_f, y_pred_f, labels=eval_label_ids) if len(y_true_f) else 0.0

    # Per-label confusion + derived metrics
    rows = []
    specificity_weight_num = 0.0
    specificity_weight_den = 0.0

    for lid in eval_label_ids:
        label_name = id2tag[lid]
        true_mask = (y_true_f == lid)
        pred_mask = (y_pred_f == lid)

        tp = int(np.sum(true_mask & pred_mask))
        fp = int(np.sum(~true_mask & pred_mask))
        fn = int(np.sum(true_mask & ~pred_mask))
        tn = int(np.sum(~true_mask & ~pred_mask))

        specificity = tn / (tn + fp) if (tn + fp) > 0 else np.nan
        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        f1 = (2 * precision * recall / (precision + recall)) if (precision + recall) > 0 else 0.0
        support = int(np.sum(true_mask))

        rows.append(
            dict(
                label=label_name,
                tp=tp,
                fp=fp,
                fn=fn,
                tn=tn,
                specificity=float(specificity) if not np.isnan(specificity) else np.nan,
                precision=float(precision),
                recall=float(recall),
                f1=float(f1),
                support=support,
            )
        )

        if not np.isnan(specificity) and support > 0:
            specificity_weight_num += specificity * support
            specificity_weight_den += support

    specificity_weighted_ovr = (
        float(specificity_weight_num / specificity_weight_den) if specificity_weight_den > 0 else np.nan
    )

    per_label_df = pd.DataFrame(rows)

    # AUC (OvR): micro & weighted
    auc_micro = np.nan
    auc_weighted = np.nan
    if y_prob_f is not None and len(y_true_f) > 0:
        # Keep only prob columns for eval labels (and align class order)
        col_index = {lid: idx for idx, lid in enumerate(sorted(tag2id.values()))}
        cols = [col_index[lid] for lid in eval_label_ids]
        y_score = y_prob_f[:, cols]

        y_true_bin = label_binarize(y_true_f, classes=eval_label_ids)
        # If some class missing in this slice, corresponding column in y_true_bin may be all zeros.
        # roc_auc_score will error in that case; handle safely.
        try:
            auc_micro = float(roc_auc_score(y_true_bin, y_score, average="micro"))
        except Exception:
            auc_micro = np.nan
        try:
            auc_weighted = float(roc_auc_score(y_true_bin, y_score, average="weighted"))
        except Exception:
            auc_weighted = np.nan

    overall = dict(
        accuracy=float(accuracy),
        micro_f1=float(micro_f1),
        weighted_f1=float(weighted_f1),
        kappa=float(kappa),
        auc_micro=float(auc_micro) if not np.isnan(auc_micro) else np.nan,
        auc_weighted=float(auc_weighted) if not np.isnan(auc_weighted) else np.nan,
        specificity_weighted_ovr=float(specificity_weighted_ovr) if not np.isnan(specificity_weighted_ovr) else np.nan,
        n_tokens=int(len(y_true_f)),
    )
    return per_label_df, overall


def format_detailed_report(per_label_df: pd.DataFrame, overall: dict, title: str) -> str:
    lines = []
    lines.append("=" * 50)
    lines.append(f"【{title}】")
    lines.append("Label           | TP    FP    FN    TN    | Spec.   Prec.   Rec.    F1      | Count")
    lines.append("-" * 95)
    for _, r in per_label_df.iterrows():
        lines.append(
            f"{str(r['label'])[:15]:<15} | "
            f"{int(r['tp']):<5} {int(r['fp']):<5} {int(r['fn']):<5} {int(r['tn']):<5} | "
            f"{(r['specificity'] if pd.notna(r['specificity']) else float('nan')):.4f}  "
            f"{r['precision']:.4f}  {r['recall']:.4f}  {r['f1']:.4f}  | "
            f"{int(r['support']):<5}"
        )
    lines.append("")
    lines.append("[Overall Metrics]")
    lines.append(f"Accuracy    : {overall['accuracy']:.4f}")
    lines.append(f"Micro F1    : {overall['micro_f1']:.4f}")
    lines.append(f"Weighted F1 : {overall['weighted_f1']:.4f}")
    lines.append(f"Cohen's Kappa: {overall['kappa']:.4f}")
    lines.append(f"OvR ROC-AUC (Micro)   : {overall['auc_micro'] if not np.isnan(overall['auc_micro']) else 'nan'}")
    lines.append(f"OvR ROC-AUC (Weighted): {overall['auc_weighted'] if not np.isnan(overall['auc_weighted']) else 'nan'}")
    lines.append(f"OvR Specificity (Weighted): {overall['specificity_weighted_ovr'] if not np.isnan(overall['specificity_weighted_ovr']) else 'nan'}")
    lines.append("=" * 50)
    return "\n".join(lines)


def t_based_mean_ci(values, alpha=0.05):
    """t-based CI for k-fold summary."""
    vals = np.asarray(values, dtype=float)
    vals = vals[~np.isnan(vals)]
    n = len(vals)
    if n == 0:
        return (np.nan, np.nan, np.nan)
    mean = float(np.mean(vals))
    if n == 1:
        return (mean, mean, mean)
    sd = float(np.std(vals, ddof=1))
    # Approx t critical via scipy if available; otherwise use normal approx for n>=30; for 10-fold, use hard-coded t(0.975,9)=2.262
    df = n - 1
    t_crit = 2.262 if df == 9 else 1.96
    half = t_crit * sd / math.sqrt(n)
    return (mean, mean - half, mean + half)


def bootstrap_ci_from_sequences(seq_items, tag2id, n_boot=1000, seed=42, exclude_tags=("IGNORE",)):
    """Bootstrap 95% CI on test/shuffled by resampling *sequences* (BOM descriptions) with replacement.

    seq_items: list of tuples (y_true_seq, y_pred_seq, y_prob_seq) where each element is already filtered to exclude padding/IGNORE.
    Returns DataFrame with mean/low/high for each metric.
    """
    rng = np.random.default_rng(seed)
    if len(seq_items) == 0:
        return pd.DataFrame()

    metrics_list = []
    n = len(seq_items)
    for _ in range(n_boot):
        idx = rng.integers(0, n, size=n)
        y_true = np.concatenate([seq_items[i][0] for i in idx])
        y_pred = np.concatenate([seq_items[i][1] for i in idx])
        y_prob = np.concatenate([seq_items[i][2] for i in idx]) if seq_items[0][2] is not None else None
        _, overall = compute_token_level_metrics(y_true, y_pred, y_prob, tag2id, exclude_tags=exclude_tags)
        metrics_list.append(overall)

    df = pd.DataFrame(metrics_list)
    out_rows = []
    for col in ["accuracy", "micro_f1", "weighted_f1", "kappa", "auc_micro", "auc_weighted", "specificity_weighted_ovr"]:
        vals = df[col].astype(float).to_numpy()
        vals = vals[~np.isnan(vals)]
        if len(vals) == 0:
            out_rows.append(dict(metric=col, mean=np.nan, ci_low=np.nan, ci_high=np.nan))
        else:
            out_rows.append(
                dict(
                    metric=col,
                    mean=float(np.mean(vals)),
                    ci_low=float(np.percentile(vals, 2.5)),
                    ci_high=float(np.percentile(vals, 97.5)),
                )
            )
    return pd.DataFrame(out_rows)


# ========= 6. 評估函式 (修改版) =========

def evaluate_model(model, data_loader, device, ignore_label_id=None, num_labels=None):
    """Evaluate model and return flattened token-level arrays plus per-sequence items for bootstrap.

    Excludes:
      - padding positions (labels == -100)
      - IGNORE label positions (labels == ignore_label_id), if provided
    Returns:
      avg_loss, precision_w, recall_w, f1_w, acc,
      all_preds, all_labels, all_probs, seq_items
    where:
      all_probs is (N, C) softmax probs for all kept tokens
      seq_items is list of (y_true_seq, y_pred_seq, y_prob_seq) per BOM description (sequence)
    """
    model.eval()
    total_loss = 0.0

    all_preds, all_labels = [], []
    all_probs = []
    seq_items = []

    with torch.no_grad():
        for batch in tqdm(data_loader, desc="Evaluating", leave=False):
            input_ids = batch["input_ids"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            labels = batch["labels"].to(device)

            outputs = model(input_ids=input_ids, attention_mask=attention_mask, labels=labels)
            loss = outputs.loss
            logits = outputs.logits  # (B, L, C)
            total_loss += float(loss.item())

            probs = torch.softmax(logits, dim=-1)  # (B, L, C)
            preds = torch.argmax(logits, dim=-1)   # (B, L)

            B, L = preds.shape
            C = probs.shape[-1] if num_labels is None else num_labels

            for i in range(B):
                # valid token mask: attention==1, label!=-100, label!=IGNORE
                mask = (attention_mask[i] == 1) & (labels[i] != -100)
                if ignore_label_id is not None:
                    mask = mask & (labels[i] != ignore_label_id)

                if torch.sum(mask).item() == 0:
                    continue

                y_true_seq = labels[i][mask].cpu().numpy().astype(int)
                y_pred_seq = preds[i][mask].cpu().numpy().astype(int)
                y_prob_seq = probs[i][mask].cpu().numpy()  # (n_i, C)

                seq_items.append((y_true_seq, y_pred_seq, y_prob_seq))

                all_labels.extend(y_true_seq.tolist())
                all_preds.extend(y_pred_seq.tolist())
                all_probs.append(y_prob_seq)

    avg_loss = total_loss / max(1, len(data_loader))

    if len(all_labels) > 0:
        precision_w, recall_w, f1_w, _ = precision_recall_fscore_support(
            all_labels, all_preds, average="weighted", zero_division=0
        )
        acc = accuracy_score(all_labels, all_preds)
        all_probs_arr = np.vstack(all_probs) if len(all_probs) else None
    else:
        precision_w = recall_w = f1_w = acc = 0.0
        all_probs_arr = None

    return avg_loss, float(precision_w), float(recall_w), float(f1_w), float(acc), all_preds, all_labels, all_probs_arr, seq_items


# ========= 7. Shuffled Test Helper =========

def shuffle_tokens_and_tags(df, seed=42):
    rng = random.Random(seed)
    df_shuf = df.copy()
    new_tokens = []
    new_tags = []

    for tok_str, tag_str in zip(df["tokens"], df["tags"]):
        try:
            toks = list(eval(str(tok_str)))
            tags = list(eval(str(tag_str)))
        except Exception:
            new_tokens.append(tok_str)
            new_tags.append(tag_str)
            continue

        if len(toks) != len(tags) or len(toks) == 0:
            new_tokens.append(tok_str)
            new_tags.append(tag_str)
            continue

        pairs = list(zip(toks, tags))
        rng.shuffle(pairs)
        shuffled_toks, shuffled_tags = zip(*pairs)

        new_tokens.append(str(list(shuffled_toks)))
        new_tags.append(str(list(shuffled_tags)))

    df_shuf["tokens"] = new_tokens
    df_shuf["tags"] = new_tags
    return df_shuf


# ========= 8. 主程式 =========

def run_10fold_plus_test():
    set_seed(42)
    print(" authorizing local file system...")

    # 請修改為您的實際路徑
    BASE_PATH = r"c:\Users\ASUS\Desktop\中山資管\研究\石曜銘產學\10fold"

    TRAIN_FILE = os.path.join(BASE_PATH, "所有資料集_訓練集.xlsx")
    VAL_FILE   = os.path.join(BASE_PATH, "所有資料集_驗證集.xlsx")
    TEST_FILE  = os.path.join(BASE_PATH, "所有資料集_測試集.xlsx")
    OUTPUT_LOG_FILE = os.path.join(BASE_PATH, "10fold_plus_test_detailed_results_1218.txt")

    def read_file(path):
        if path.lower().endswith(".xlsx"):
            return pd.read_excel(path)
        else:
            return pd.read_csv(path)

    print("📥 1. 正在讀取 Train / Val / Test 檔案...")
    try:
        raw_train = read_file(TRAIN_FILE)
        raw_val   = read_file(VAL_FILE)
        raw_test  = read_file(TEST_FILE)
    except FileNotFoundError as e:
        print(f"❌ 找不到檔案: {e}")
        return

    # 轉成 NER 格式
    print("🔄 2. 正在將三個資料集轉成 NER 格式...")
    train_df, skipped_train = build_ner_df(raw_train)
    val_df,   skipped_val   = build_ner_df(raw_val)
    test_df,  skipped_test  = build_ner_df(raw_test)

    # 建立標籤編碼器
    print("🔖 3. 正在建立標籤編碼器...")
    all_tags_list = []
    for df_tmp in [train_df, val_df, test_df]:
        for tags_str in df_tmp["tags"]:
            try:
                all_tags_list.extend(eval(str(tags_str)))
            except: pass

    all_tags = sorted(list(set(all_tags_list)))
    tag2id = {tag: idx for idx, tag in enumerate(all_tags)}
    id2tag = {idx: tag for tag, idx in tag2id.items()}
    print(f"✅ 標籤編碼完成，共 {len(tag2id)} 個類別。")

    ignore_label_id = tag2id.get("IGNORE", None)
    num_labels = len(tag2id)
    cv_overall_rows = []  # per fold overall metrics
    cv_per_label_rows = []  # per fold per-label metrics
    # 超參數
    N_SPLITS = 10
    EPOCHS = 30
    PATIENCE = 3
    LEARNING_RATE = 5e-5
    BATCH_SIZE = 16
    MAX_LEN = 64
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    tokenizer = DistilBertTokenizerFast.from_pretrained("distilbert-base-uncased")

    with open(OUTPUT_LOG_FILE, "w", encoding="utf-8") as f:
        f.write(f"--- 10-Fold CV + Detailed Metrics Report ---\n")
        f.write(f"時間: {datetime.datetime.now()}\n")
        f.write(f"標籤: {list(tag2id.keys())}\n\n")

    # ========= (一) 10-fold CV =========
    print(f"\n🚀 4. 在 Train+Val 上進行 {N_SPLITS}-fold CV...")
    cv_df = pd.concat([train_df, val_df], axis=0).reset_index(drop=True)
    kfold = KFold(n_splits=N_SPLITS, shuffle=True, random_state=42)
    fold_best_f1_scores = []

    for fold, (train_ids, val_ids) in enumerate(kfold.split(cv_df)):
        print(f"\n===== CV FOLD {fold + 1}/{N_SPLITS} =====")
        train_fold_df = cv_df.iloc[train_ids].reset_index(drop=True)
        val_fold_df   = cv_df.iloc[val_ids].reset_index(drop=True)

        # Dataset & Loader
        train_dataset = NERDataset(train_fold_df["tokens"].tolist(), train_fold_df["tags"].tolist(), tokenizer, tag2id, max_len=MAX_LEN, augment=True)
        val_dataset_fold = NERDataset(val_fold_df["tokens"].tolist(), val_fold_df["tags"].tolist(), tokenizer, tag2id, max_len=MAX_LEN, augment=False)
        train_loader = DataLoader(train_dataset, batch_size=BATCH_SIZE, shuffle=True)
        val_loader   = DataLoader(val_dataset_fold, batch_size=BATCH_SIZE)

        # Model & Optimizer
        config = DistilBertConfig.from_pretrained("distilbert-base-uncased", num_labels=len(tag2id), dropout=0.3, id2label=id2tag, label2id=tag2id)
        model = DistilBertForTokenClassification.from_pretrained("distilbert-base-uncased", config=config).to(device)
        optimizer = AdamW(model.parameters(), lr=LEARNING_RATE)

        best_val_loss_fold = float("inf")
        best_val_f1_fold = 0.0
        patience_counter = 0
        best_state_dict_fold = None

        # 用來畫圖
        train_losses, val_losses, val_f1_scores, train_acc_scores, val_acc_scores = [], [], [], [], []

        for epoch in range(EPOCHS):
            model.train()
            total_train_loss = 0.0
            train_preds, train_labels = [], []
            
            loop_train = tqdm(train_loader, desc=f"Fold {fold+1} Epoch {epoch+1}", leave=False)
            for batch in loop_train:
                input_ids = batch["input_ids"].to(device)
                attention_mask = batch["attention_mask"].to(device)
                labels = batch["labels"].to(device)

                optimizer.zero_grad()
                outputs = model(input_ids=input_ids, attention_mask=attention_mask, labels=labels)
                loss = outputs.loss
                loss.backward()
                optimizer.step()

                total_train_loss += loss.item()
                
                # 計算 Train Acc
                logits = outputs.logits.detach()
                preds = torch.argmax(logits, dim=-1)
                mask = labels != -100
                train_preds.extend(preds[mask].cpu().numpy())
                train_labels.extend(labels[mask].cpu().numpy())
                loop_train.set_postfix(loss=loss.item())

            avg_val_loss, val_p, val_r, val_f1, val_acc, _, _, _, _ = evaluate_model(model, val_loader, device, ignore_label_id=ignore_label_id, num_labels=num_labels)
            train_acc = accuracy_score(train_labels, train_preds) if len(train_labels) > 0 else 0.0

            # Val
            avg_val_loss, val_p, val_r, val_f1, val_acc, _, _, _, _ = evaluate_model(model, val_loader, device, ignore_label_id=ignore_label_id, num_labels=num_labels)

            # 紀錄
            train_losses.append(avg_train_loss)
            val_losses.append(avg_val_loss)
            val_f1_scores.append(val_f1)
            train_acc_scores.append(train_acc)
            val_acc_scores.append(val_acc)

            print(f"Fold {fold+1} Ep {epoch+1}: Train Loss {avg_train_loss:.4f} | Val Loss {avg_val_loss:.4f} | Val F1 {val_f1:.4f}")

            # Early Stopping
            if avg_val_loss < best_val_loss_fold:
                best_val_loss_fold = avg_val_loss
                best_val_f1_fold = val_f1
                patience_counter = 0
                best_state_dict_fold = {k: v.detach().cpu().clone() for k, v in model.state_dict().items()}
            else:
                patience_counter += 1
                if patience_counter >= PATIENCE:
                    print("🛑 Early Stopping")
                    break
        
        # 畫圖 (略，與原本相同，為節省篇幅)
        epochs_range = range(1, len(train_losses) + 1)
        fig, axes = plt.subplots(1, 3, figsize=(18, 4))
        axes[0].plot(epochs_range, train_losses, label="Train"); axes[0].plot(epochs_range, val_losses, label="Val"); axes[0].set_title("Loss")
        axes[1].plot(epochs_range, train_acc_scores, label="Train"); axes[1].plot(epochs_range, val_acc_scores, label="Val"); axes[1].set_title("Acc")
        axes[2].plot(epochs_range, val_f1_scores, label="Val F1"); axes[2].set_title("F1")
        plt.tight_layout()
        plt.savefig(os.path.join(BASE_PATH, f"fold_{fold+1}_curves.png"))
        plt.close(fig)

        # ---- Fold-level evaluation (token-level, exclude IGNORE & padding) ----
        if best_state_dict_fold is not None:
            model.load_state_dict({k: v.to(device) for k, v in best_state_dict_fold.items()})
        _, _, _, _, _, fold_preds, fold_labels, fold_probs, _ = evaluate_model(
            model, val_loader, device, ignore_label_id=ignore_label_id, num_labels=num_labels
        )
        fold_per_label_df, fold_overall = compute_token_level_metrics(
            fold_labels, fold_preds, fold_probs, tag2id, exclude_tags=("IGNORE",)
        )
        fold_overall["fold"] = fold + 1
        cv_overall_rows.append(fold_overall)
        fold_per_label_df.insert(0, "fold", fold + 1)
        cv_per_label_rows.append(fold_per_label_df)
        fold_best_f1_scores.append(best_val_f1_fold)
        with open(OUTPUT_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"Fold {fold+1}: Best F1={best_val_f1_fold:.4f}, Best Loss={best_val_loss_fold:.4f}\n")

    # ========= (二) Final Train & Test =========
    print("\n🚀 6. Final Training & Detailed Testing...")

    # Dataset & Loader (Final)
    train_dataset_final = NERDataset(train_df["tokens"].tolist(), train_df["tags"].tolist(), tokenizer, tag2id, max_len=MAX_LEN, augment=True)
    val_dataset_final   = NERDataset(val_df["tokens"].tolist(), val_df["tags"].tolist(), tokenizer, tag2id, max_len=MAX_LEN, augment=False)
    test_dataset        = NERDataset(test_df["tokens"].tolist(), test_df["tags"].tolist(), tokenizer, tag2id, max_len=MAX_LEN, augment=False)
    
    train_loader_final = DataLoader(train_dataset_final, batch_size=BATCH_SIZE, shuffle=True)
    val_loader_final   = DataLoader(val_dataset_final, batch_size=BATCH_SIZE)
    test_loader        = DataLoader(test_dataset, batch_size=BATCH_SIZE)

    model_final = DistilBertForTokenClassification.from_pretrained("distilbert-base-uncased", config=config).to(device)
    optimizer_final = AdamW(model_final.parameters(), lr=LEARNING_RATE)

    best_val_loss_final = float("inf")
    best_state_dict = None
    patience_counter = 0

    # ... (Final 訓練迴圈省略，邏輯與 Fold 相同，重點在最後的測試) ...
    # 這裡簡單模擬訓練過程，實際執行請保留原本的完整訓練迴圈
    for epoch in range(EPOCHS):
        model_final.train()
        loop_train = tqdm(train_loader_final, desc=f"Final Ep {epoch+1}", leave=False)
        for batch in loop_train:
            input_ids = batch["input_ids"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            labels = batch["labels"].to(device)
            optimizer_final.zero_grad()
            # NEW (Fixed) - Use keyword arguments
            outputs = model_final(input_ids=input_ids, attention_mask=attention_mask, labels=labels)
            loss = outputs.loss
            loss.backward()
            optimizer_final.step()
        
        # Validation
        avg_val_loss, _, _, val_f1, _, _, _, _, _ = evaluate_model(model_final, val_loader_final, device, ignore_label_id=ignore_label_id, num_labels=num_labels)
        print(f"[Final] Ep {epoch+1} Val Loss: {avg_val_loss:.4f}, F1: {val_f1:.4f}")

        if avg_val_loss < best_val_loss_final:
            best_val_loss_final = avg_val_loss
            best_state_dict = {k: v.cpu().clone() for k, v in model_final.state_dict().items()}
            patience_counter = 0
        else:
            patience_counter += 1
            if patience_counter >= PATIENCE:
                break
    
    if best_state_dict:
        model_final.load_state_dict(best_state_dict)
    
    # ----------------------------------------------------
    #  重點修改：詳細 Test 報告 (含 Specificity, Kappa 等)
    # ----------------------------------------------------
    print("🧪 7. 正在生成詳細 Test 報表...")
    
    # 取得原始預測與標籤列表
    test_loss, _, _, _, _, test_preds, test_labels, test_probs, test_seq_items = evaluate_model(model_final, test_loader, device, ignore_label_id=ignore_label_id, num_labels=num_labels)
    
    # 計算詳細指標（明確排除 IGNORE 與 padding tokens）
    test_per_label_df, test_overall = compute_token_level_metrics(
        test_labels, test_preds, test_probs, tag2id, exclude_tags=("IGNORE",)
    )
    detailed_report = format_detailed_report(test_per_label_df, test_overall, title="Final Test Detailed Report")

    print(detailed_report)

    # Bootstrap 95% CI（以 BOM description 為抽樣單位）
    test_boot_ci_df = bootstrap_ci_from_sequences(
        test_seq_items, tag2id, n_boot=1000, seed=42, exclude_tags=("IGNORE",)
    )
    if len(test_boot_ci_df) > 0:
        print("\n[Final Test Bootstrap 95% CI]")
        print(test_boot_ci_df.to_string(index=False))
    
    with open(OUTPUT_LOG_FILE, "a", encoding="utf-8") as f:
        f.write("\n" + "="*50 + "\n")
        f.write("Final Test Set 詳細評估結果\n")
        f.write(detailed_report + "\n")

    # ----------------------------------------------------
    #  Shuffled Test 詳細報告
    # ----------------------------------------------------
    print("🧪 8. Shuffled Test 詳細報表...")
    shuffled_test_df = shuffle_tokens_and_tags(test_df, seed=123)
    sh_dataset = NERDataset(shuffled_test_df["tokens"].tolist(), shuffled_test_df["tags"].tolist(), tokenizer, tag2id, max_len=MAX_LEN)
    sh_loader = DataLoader(sh_dataset, batch_size=BATCH_SIZE)
    
    _, _, _, _, _, sh_preds, sh_labels, sh_probs, sh_seq_items = evaluate_model(model_final, sh_loader, device, ignore_label_id=ignore_label_id, num_labels=num_labels)
    
    sh_per_label_df, sh_overall = compute_token_level_metrics(
        sh_labels, sh_preds, sh_probs, tag2id, exclude_tags=("IGNORE",)
    )
    sh_report = format_detailed_report(sh_per_label_df, sh_overall, title="Shuffled Test Detailed Report")
    print(sh_report)

    sh_boot_ci_df = bootstrap_ci_from_sequences(
        sh_seq_items, tag2id, n_boot=1000, seed=123, exclude_tags=("IGNORE",)
    )
    if len(sh_boot_ci_df) > 0:
        print("\n[Shuffled Test Bootstrap 95% CI]")
        print(sh_boot_ci_df.to_string(index=False))
    
    with open(OUTPUT_LOG_FILE, "a", encoding="utf-8") as f:
        f.write("\n" + "="*50 + "\n")
        f.write("Shuffled Test Set 詳細評估結果\n")
        f.write(sh_report + "\n")

    # Save Model
    SAVE_DIR = os.path.join(BASE_PATH, "distilbert_ner_final")
    model_final.save_pretrained(SAVE_DIR)
    tokenizer.save_pretrained(SAVE_DIR)
    print(f"✅ 完成。模型已存至 {SAVE_DIR}")


    # ==================================================
    #  Export to 3 Excel workbooks (CV / Final Test / Shuffled Test)
    #  Each workbook uses multiple sheets.
    # ==================================================
    cv_overall_df = pd.DataFrame(cv_overall_rows).sort_values("fold") if len(cv_overall_rows) else pd.DataFrame()
    cv_per_label_df = pd.concat(cv_per_label_rows, ignore_index=True) if len(cv_per_label_rows) else pd.DataFrame()

    # 10-fold summary 95% CI (t-based) for required metrics
    cv_ci_rows = []
    for metric in ["accuracy", "micro_f1", "weighted_f1", "kappa", "auc_micro", "auc_weighted", "specificity_weighted_ovr"]:
        if len(cv_overall_df) == 0 or metric not in cv_overall_df.columns:
            cv_ci_rows.append(dict(metric=metric, mean=np.nan, ci_low=np.nan, ci_high=np.nan))
            continue
        mean, low, high = t_based_mean_ci(cv_overall_df[metric].values)
        cv_ci_rows.append(dict(metric=metric, mean=mean, ci_low=low, ci_high=high))
    cv_ci_df = pd.DataFrame(cv_ci_rows)

    cv_xlsx = os.path.join(BASE_PATH, "CV_results.xlsx")
    with pd.ExcelWriter(cv_xlsx, engine="openpyxl") as writer:
        cv_overall_df.to_excel(writer, sheet_name="fold_overall", index=False)
        cv_per_label_df.to_excel(writer, sheet_name="fold_per_label", index=False)
        cv_ci_df.to_excel(writer, sheet_name="overall_95CI", index=False)

    # Final Test workbook
    final_xlsx = os.path.join(BASE_PATH, "FinalTest_results.xlsx")
    final_overall_df = pd.DataFrame([test_overall])
    with pd.ExcelWriter(final_xlsx, engine="openpyxl") as writer:
        final_overall_df.to_excel(writer, sheet_name="overall", index=False)
        test_per_label_df.to_excel(writer, sheet_name="per_label", index=False)
        test_boot_ci_df.to_excel(writer, sheet_name="bootstrap_95CI", index=False)

    # Shuffled Test workbook
    sh_xlsx = os.path.join(BASE_PATH, "ShuffledTest_results.xlsx")
    sh_overall_df = pd.DataFrame([sh_overall])
    with pd.ExcelWriter(sh_xlsx, engine="openpyxl") as writer:
        sh_overall_df.to_excel(writer, sheet_name="overall", index=False)
        sh_per_label_df.to_excel(writer, sheet_name="per_label", index=False)
        sh_boot_ci_df.to_excel(writer, sheet_name="bootstrap_95CI", index=False)

    print(f"\n✅ 已輸出三個 Excel 檔案：\n- {cv_xlsx}\n- {final_xlsx}\n- {sh_xlsx}")
if __name__ == "__main__":
    run_10fold_plus_test()