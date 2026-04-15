import onnxruntime as ort

# Thay đúng tên file model của bạn
session = ort.InferenceSession("model_NEARUSDT.onnx")

input_info = session.get_inputs()[0]
print("Tên input:", input_info.name)
print("Shape input:", input_info.shape)