"""
export_quantize.py - Lò nén siêu tốc V17.3 (Dual-Loop & Atomic Write)
Nhiệm vụ: Chuyển đổi mô hình XGBoost (Tập trung & Cụ thể) sang ONNX và Lượng tử hóa INT8.
"""
import xgboost as xgb
import onnxmltools
from onnxmltools.convert.common.data_types import FloatTensorType
from onnxruntime.quantization import quantize_dynamic, QuantType
import os
import glob
import warnings

warnings.filterwarnings('ignore')

# Cấu hình chuẩn
FEATURES_COUNT = 13 
ONNX_OPSET = 13 # 🚀 BẮT BUỘC: Opset 13 để kích hoạt "Khiên NaN" (Sparsity routing)
TEMP_DIR = "temp_quantize"

def process_model(xgb_path, final_onnx_path):
    print(f"\n🔄 Đang xử lý: {xgb_path}")
    raw_onnx_path = os.path.join(TEMP_DIR, f"raw_{os.path.basename(final_onnx_path)}")
    temp_final_path = final_onnx_path + ".tmp" # Dùng cho Atomic Write

    try:
        # 1. 🟢 VÁ LỖI 1: Nạp trực tiếp bằng Booster (tương thích với hàm xgb.train)
        booster = xgb.Booster()
        booster.load_model(xgb_path)

        # 2. Định nghĩa Tensor Đầu vào
        initial_type = [('float_input', FloatTensorType([None, FEATURES_COUNT]))]

        # 3. 🟢 VÁ LỖI 2: Dịch ngược sang ONNX với target_opset = 13
        onnx_model = onnxmltools.convert_xgboost(
            booster, 
            initial_types=initial_type,
            target_opset=ONNX_OPSET
        )

        with open(raw_onnx_path, "wb") as f:
            f.write(onnx_model.SerializeToString())

        # 4. Lượng tử hóa INT8 (Xuất ra file .tmp trước)
        quantize_dynamic(
            model_input=raw_onnx_path,
            model_output=temp_final_path,
            weight_type=QuantType.QUInt8
        )

        # 5. 🟢 VÁ LỖI 3: Kỹ thuật ATOMIC RENAME (An toàn cho Node.js Hot-Reload)
        # Lệnh rename trong hệ điều hành diễn ra tức thời, Node.js sẽ không bị đọc nhầm file dở dang
        os.replace(temp_final_path, final_onnx_path)

        # Dọn dẹp rác
        os.remove(raw_onnx_path)
        print(f"✅ Hoàn tất Lượng tử hóa & Đẩy vào Node.js: {final_onnx_path}")

    except Exception as e:
        print(f"❌ Lỗi xử lý mô hình {xgb_path}: {e}")

def export_and_quantize():
    if not os.path.exists(TEMP_DIR):
        os.makedirs(TEMP_DIR)

    print("📦 [Export & Quantize] BẮT ĐẦU DÂY CHUYỀN ÉP KIỂU NÃO BỘ...")

    # 🟢 VÁ LỖI 4: XỬ LÝ TOÀN BỘ MẠNG LƯỚI DUAL-LOOP
    
    # LƯỚI 1: Não Tập Trung (Universal Model)
    universal_xgb = "model_universal_pro.xgb"
    if os.path.exists(universal_xgb):
        process_model(universal_xgb, "Universal_Scout.onnx")
    else:
        print(f"⚠️ Không tìm thấy {universal_xgb}")

    # LƯỚI 2: Não Cụ Thể (Specific Models Tầng 1, Tầng 2)
    specific_models = glob.glob("model_v16_*.xgb")
    if specific_models:
        print(f"🔍 Phát hiện {len(specific_models)} mô hình coin cụ thể. Đang tiến hành nén hàng loạt...")
        for xgb_file in specific_models:
            onnx_name = os.path.basename(xgb_file).replace(".xgb", ".onnx")
            process_model(xgb_file, onnx_name)

    print("\n🎉 TOÀN BỘ HỆ THỐNG NÃO BỘ ĐÃ SẴN SÀNG CHẠY LIVE!")

if __name__ == "__main__":
    export_and_quantize()