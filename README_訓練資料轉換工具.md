# 訓練資料轉換工具 - 使用手冊

## 📋 概述

本工具用於將 Pipeline 輸出的審核後資料轉換為 NER 模型訓練格式。

---

## 🔄 完整工作流程

```
1. run_bom_pipelineV2.py 處理 BOM
              ↓
2. 產出 _REVIEW.xlsx (需人工審核)
              ↓
3. 廠商修正欄位值 ⭐ 重要步驟
              ↓
4. convert_to_training_data.py 轉換
              ↓
5. 產出訓練格式 Excel
              ↓
6. 模型重新訓練（需技術人員）
```

---

## 🚀 使用方式

```bash
python convert_to_training_data.py --input "審核完成的REVIEW.xlsx" --output "新訓練資料.xlsx"
```

### 參數說明

| 參數 | 說明 | 預設值 |
|-----|------|-------|
| `--input`, `-i` | 輸入的審核完成 Excel | （必填） |
| `--output`, `-o` | 輸出的訓練資料 Excel | `<輸入檔名>_training.xlsx` |
| `--desc_col` | 描述欄位名稱 | `正規化Description` |

---

## ⚠️ 廠商需要修正確認的欄位

以下欄位會被轉換工具讀取，**請確保正確**：

| 欄位名稱 | 對應標籤 | 範例 |
|---------|---------|------|
| 類別 | Category | RES, CAP, IND |
| 阻值 | Resistance | 10K, 4.7R |
| 容量 | Capacitance | 100nF, 1uF |
| 電感值 | Inductance | 10uH |
| 電壓 | Voltage | 25V |
| 電流 | Current | 1A, 500mA |
| 容差 | Tolerance | 1%, 5% |
| 功率 | Power | 1W, 1/4W |
| 溫度係數 | Temp_Coefficient | 100PPM |
| 介質 | Temp_Code | X7R, C0G |
| 顏色 | Color | RED, GREEN |
| 頻率 | Frequency | 100MHz |
| 波長 | Wavelength | 520nm |
| 尺寸 | Size | 0402, 0805 |
| 封裝 | Package | TSSOP, QFN |
| 針腳數 | Pin_Count | 8P, 16PIN |
| 方向/類型 | Type | NPN, RIGHT ANGLE |
| 法規 | Compliance | RoHS |
| 製程 | Process_Type | SMD, THT |

> **注意**：`阻值_IEC` 和 `容量_EIA` 是衍生欄位，不需要修正。

---

## 📊 輸出格式

轉換後的 Excel 包含兩個欄位：

| 欄位名稱 | 說明 | 範例 |
|---------|------|------|
| Description | 原始描述文字 | `RES 10K 1% 0402 SMD RoHS` |
| Labels | NER 標籤列表 | `['Category', 'Resistance', 'Tolerance', 'Package', 'Process_Type', 'Compliance']` |

---

## 📝 審核指南

### ✅ 正確的審核方式

1. **確認欄位值與描述對應**
   - 描述：`RES 10K 1% 0402`
   - 阻值欄位應為：`10K`（不是 `10KΩ` 或 `10000`）

2. **刪除錯誤識別的值**
   - 若 `容量` 欄位錯誤地填入電阻值，請清空

3. **補上遺漏的值**
   - 若 `封裝` 欄位為空但描述中有 `0402`，請填入

### ❌ 避免的操作

- 不要修改 `正規化Description` 欄位
- 不要新增自定義欄位
- 不要使用縮寫（如 `K` 代替 `10K`）

---

## ❓ 常見問題

### Q: 轉換後的標籤數量與 token 數量不符？
A: 這是正常的，工具會自動補齊為 "O" 標籤。

### Q: 某些欄位沒有被轉換？
A: 只有上表列出的欄位會被轉換，其他欄位會被忽略。

### Q: 如何驗證轉換結果？
A: 開啟輸出的 Excel，檢查 `Labels` 欄位是否合理。
