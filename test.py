import onnx
# Tải mô hình
model = onnx.load("model_SOLUSDT.onnx")
# Chuyển toàn bộ cấu trúc kiến trúc sang dạng Text
readable_graph = onnx.helper.printable_graph(model.graph)

# Lưu ra file txt hoặc in ra màn hình để copy cho AI
with open("model_structure.txt", "w") as f:
    f.write(readable_graph)
    
print("Đã xuất cấu trúc model ra file model_structure.txt. Hãy copy nội dung này cho DeepSeek!")