# BOM 自動正規化系統 - 廠商使用手冊

## 📋 系統簡介

本程式用於自動處理 BOM (Bill of Materials) Excel 檔案，將零件描述欄位正規化，提取關鍵規格（阻值、容量、封裝等），並標準化顯示名稱。

---

## ⚠️ 首次使用前必須確認的事項

### 1. Python 環境
請確認已安裝 Python 3.8 或以上版本：
```bash
python --version
```

### 2. 安裝相依套件
在程式資料夾中執行：
```bash
pip install -r requirements.txt
```

### 3. NER 模型資料夾
程式會自動尋找 `distilbert_ner_final` 資料夾作為 AI 模型。
- 若找不到，程式會使用純規則式處理（仍可正常運作）
- 若需要 AI 功能，請確保模型資料夾在程式同一目錄下

---

## 🚀 快速開始

### 方法一：使用圖形介面選檔（最簡單）
```bash
python run_bom_pipelineV2.py --gui
```

### 方法二：指定輸入檔案
```bash
python run_bom_pipelineV2.py --input "您的BOM檔案.xlsx"
```

### 方法三：指定輸入輸出路徑
```bash
python run_bom_pipelineV2.py --input "C:/BOM/客戶BOM.xlsx" --out_dir "C:/BOM/results"
```

---

## 📁 輸出檔案說明

| 檔案名稱 | 說明 |
|---------|------|
| `<原檔名>_final.xlsx` | 完整處理結果，含「主分頁」和「群組彙總」兩個工作表 |
| `<原檔名>_AUTO.xlsx` | 自動通過驗證的資料，可直接使用 |
| `<原檔名>_REVIEW.xlsx` | 需要人工審核的資料 |

---

## 📊 輸出欄位說明

### 正規化欄位
| 欄位名稱 | 說明 |
|---------|------|
| 類別 | RES(電阻)/CAP(電容)/IND(電感)/IC/CN(連接器)/OT(其他) |
| 阻值 | 電阻值（如 10K, 4.7K） |
| 阻值_IEC | IEC 標準格式（如 10K → 10K） |
| 容量 | 電容值（如 100nF, 1uF） |
| 容量_EIA | EIA 代碼格式（如 100nF → 104） |
| 正規化Description | 標準化的完整描述 |
| 顯示名20 | 20字元以內的簡潔顯示名稱 |

### 狀態欄位
| 欄位名稱 | 說明 |
|---------|------|
| status | AUTO（自動通過）或 NEED_REVIEW（需審核） |
| review_reason | 需審核的原因代碼 |
| NER_Used | 是否使用 AI 模型處理 |
| NER_Result | AI 模型識別結果（原始格式） |

### 審核原因代碼
| 代碼 | 說明 |
|-----|------|
| ner_low_valid_X/Y | NER 有效標籤太少（X/Y 表示有效數/總數） |
| short_display20_len=N | 顯示名太短（N < 8 字元） |
| missing_resistance | 電阻類但缺少阻值 |
| missing_capacitance | 電容類但缺少容量 |
| duplicate_display_diff_pn | 相同顯示名但不同料號（需確認） |

---

## 🔧 完整參數說明

```bash
python run_bom_pipelineV2.py [選項]
```

| 參數 | 說明 | 預設值 |
|-----|------|-------|
| `--input` | 輸入 BOM Excel 檔案路徑 | （必填或使用 --gui） |
| `--out_dir` | 輸出目錄路徑 | outputs |
| `--sheet` | 指定工作表名稱 | 自動偵測 |
| `--model_dir` | NER 模型目錄 | distilbert_ner_final |
| `--gui` | 開啟檔案選擇對話框 | - |
| `--verbose` | 顯示詳細處理訊息 | - |
| `--debug` | 除錯模式（保留中間欄位） | - |

---

## ❓ 常見問題

### Q: 程式顯示「找不到模型目錄」
A: 這是正常的，程式會改用純規則式處理。若需要 AI 功能，請確保 `distilbert_ner_final` 資料夾存在。

### Q: Excel 檔案無法讀取
A: 請確認：
1. 檔案沒有被其他程式開啟
2. 檔案格式是 .xlsx 或 .xlsm
3. 檔案中有名為「Description」的欄位

### Q: 為什麼某些資料被標記為 NEED_REVIEW？
A: 請查看 `review_reason` 欄位的代碼說明（見上方表格）。

---

## 📞 技術支援

如有問題，請聯繫系統開發團隊。
