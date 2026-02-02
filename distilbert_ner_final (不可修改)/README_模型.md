# DistilBERT NER 模型說明

## 📋 概述

此資料夾包含預訓練的 DistilBERT NER (Named Entity Recognition) 模型，
用於自動識別 BOM 描述中的零件規格標籤。

---

## 📁 檔案說明

| 檔案名稱 | 用途 | 可否修改 |
|---------|------|---------|
| `model.safetensors` | 模型權重檔（約 253MB） | ❌ 不可修改 |
| `config.json` | 模型設定和標籤定義 | ⚠️ 僅查看 |
| `tokenizer.json` | 分詞器設定 | ❌ 不可修改 |
| `tokenizer_config.json` | 分詞器參數 | ❌ 不可修改 |
| `vocab.txt` | 詞彙表 | ❌ 不可修改 |
| `special_tokens_map.json` | 特殊標記對照 | ❌ 不可修改 |

---

## 🏷️ 可識別的標籤

模型可識別以下 21 種標籤：

| 標籤 | 中文說明 | 範例 |
|-----|---------|------|
| Category | 類別 | RES, CAP, IC |
| Resistance | 阻值 | 10K, 4.7K |
| Capacitance | 容量 | 100nF, 1uF |
| Inductance | 電感值 | 10uH |
| Voltage | 電壓 | 25V, 50V |
| Current | 電流 | 1A, 500mA |
| Power | 功率 | 1W, 1/4W |
| Tolerance | 容差 | 1%, 5% |
| Package | 封裝 | TSSOP, QFN |
| Size | 尺寸 | 0402, 0805 |
| Pin_Count | 針腳數 | 8P, 16PIN |
| Frequency | 頻率 | 100MHz |
| Wavelength | 波長 | 520nm |
| Color | 顏色 | RED, GREEN |
| Temp_Code | 溫度代碼 | X7R, C0G |
| Temp_Coefficient | 溫度係數 | 100PPM |
| Type | 類型 | NPN, PNP |
| Process_Type | 製程 | SMD, THT |
| Compliance | 法規 | RoHS |
| IGNORE | 忽略項 | 逗號、括號等符號 |
| O | 其他 | 無法識別的內容 |

---

## ⚠️ 注意事項

1. **不可修改模型檔案**：任何修改都會導致模型無法載入
2. **模型版本**：基於 DistilBERT (transformers 4.57.1)
3. **GPU 加速**：若有 NVIDIA GPU，模型會自動使用 CUDA 加速
4. **純規則模式**：若此資料夾不存在，程式會自動改用純規則式處理

---

## 🔄 重新訓練模型

如需重新訓練模型（例如新增標籤），請聯繫系統開發團隊。
訓練需要：
1. 標註好的訓練資料
2. GPU 運算資源
3. Python 深度學習環境
