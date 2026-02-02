# Rules 資料夾說明

## 📋 概述

此資料夾包含 BOM 正規化系統的設定規則檔案（YAML 格式）。
這些檔案定義了零件分類、正規表達式模式、和命名模板。

---

## 📁 檔案說明

| 檔案名稱 | 用途 |
|---------|------|
| `category_schema.yaml` | 零件類別定義（RES/CAP/IND/IC 等） |
| `patterns.yaml` | 正規表達式模式（用於識別阻值、容量等） |
| `normalize.yaml` | 文字正規化規則（統一格式、符號替換） |
| `temp_coefficient.yaml` | 溫度係數代碼對照表 |
| `templates.yaml` | 命名模板（顯示名20的組合規則） |

---

## 🔧 修改指南

### 1. 新增零件類別

編輯 `category_schema.yaml`，在 `categories:` 區塊新增：

```yaml
  NEW_CATEGORY:
    description: 新類別描述
    enable_name20: true
    enable_code20: false
    required_fields_for_name20: [欄位1, 欄位2]
```

### 2. 新增識別規則

編輯 `patterns.yaml`，新增正規表達式模式。

### 3. 新增溫度係數代碼

編輯 `temp_coefficient.yaml`，新增對照項目。

---

## ⚠️ 注意事項

1. **YAML 格式敏感**：縮排必須使用空格，不可使用 Tab
2. **修改後需重新執行程式**：設定變更不需重新訓練模型
3. **備份原檔**：建議修改前先備份原始檔案
