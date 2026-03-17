import struct
import zlib
import os

# ==================== 你的真实数据（硬编码，无需读取文件）====================
METRICS_DATA = [
    {"n_workers": 1, "runtime": 266.43, "throughput": 37451.49},
    {"n_workers": 4, "runtime": 264.91, "throughput": 37666.38},
    {"n_workers": 8, "runtime": 275.56, "throughput": 36210.63}
]

# ==================== 纯Python生成标准PNG（无任何外部依赖）====================
def generate_standard_png(filename="performance_chart.png"):
    # 图片尺寸
    WIDTH = 800
    HEIGHT = 600
    PADDING = 80
    
    # 颜色定义 (R, G, B)
    WHITE = (255, 255, 255)
    BLACK = (0, 0, 0)
    RED = (255, 0, 0)
    BLUE = (0, 0, 255)
    
    # 初始化像素数组（全白背景）
    pixels = [WHITE for _ in range(WIDTH * HEIGHT)]
    
    # 提取数据
    workers = [d["n_workers"] for d in METRICS_DATA]
    runtimes = [d["runtime"] for d in METRICS_DATA]
    throughputs = [d["throughput"] for d in METRICS_DATA]
    
    # 数据归一化
    max_rt = max(runtimes)
    min_rt = min(runtimes)
    max_tp = max(throughputs)
    min_tp = min(throughputs)
    x_step = (WIDTH - 2*PADDING) // (len(workers)-1) if len(workers) > 1 else 100

    # ==================== 基础绘图函数 ====================
    def set_pixel(x, y, color):
        """设置单个像素颜色"""
        if 0 <= x < WIDTH and 0 <= y < HEIGHT:
            idx = y * WIDTH + x
            pixels[idx] = color

    def draw_line(x1, y1, x2, y2, color, width=3):
        """绘制线条（Bresenham算法）"""
        x1, y1, x2, y2 = int(x1), int(y1), int(x2), int(y2)
        dx = abs(x2 - x1)
        dy = abs(y2 - y1)
        sx = 1 if x1 < x2 else -1
        sy = 1 if y1 < y2 else -1
        err = dx - dy

        while True:
            # 绘制宽度
            for w in range(-width//2, width//2 + 1):
                for h in range(-width//2, width//2 + 1):
                    set_pixel(x1 + w, y1 + h, color)
            if x1 == x2 and y1 == y2:
                break
            e2 = 2 * err
            if e2 > -dy:
                err -= dy
                x1 += sx
            if e2 < dx:
                err += dx
                y1 += sy

    def draw_circle(x, y, radius, color):
        """绘制圆形数据点"""
        x, y = int(x), int(y)
        for dx in range(-radius, radius + 1):
            for dy in range(-radius, radius + 1):
                if dx*dx + dy*dy <= radius*radius:
                    set_pixel(x + dx, y + dy, color)

    # ==================== 绘制图表 ====================
    # 绘制坐标轴
    draw_line(PADDING, HEIGHT-PADDING, WIDTH-PADDING, HEIGHT-PADDING, BLACK, 4)  # X轴
    draw_line(PADDING, PADDING, PADDING, HEIGHT-PADDING, RED, 4)                  # 左Y轴（运行时间）
    draw_line(WIDTH-PADDING, PADDING, WIDTH-PADDING, HEIGHT-PADDING, BLUE, 4)    # 右Y轴（吞吐量）

    # 绘制数据点和线条
    prev_x_rt, prev_y_rt = None, None
    prev_x_tp, prev_y_tp = None, None

    for i in range(len(workers)):
        x = PADDING + i * x_step
        
        # 运行时间（左Y轴）
        rt_norm = (runtimes[i] - min_rt) / (max_rt - min_rt) if max_rt != min_rt else 0.5
        y_rt = PADDING + int(rt_norm * (HEIGHT - 2*PADDING))
        draw_circle(x, y_rt, 8, RED)
        if prev_x_rt is not None:
            draw_line(prev_x_rt, prev_y_rt, x, y_rt, RED, 3)
        prev_x_rt, prev_y_rt = x, y_rt

        # 吞吐量（右Y轴）
        tp_norm = (throughputs[i] - min_tp) / (max_tp - min_tp) if max_tp != min_tp else 0.5
        y_tp = HEIGHT - PADDING - int(tp_norm * (HEIGHT - 2*PADDING))
        draw_circle(x, y_tp, 8, BLUE)
        if prev_x_tp is not None:
            draw_line(prev_x_tp, prev_y_tp, x, y_tp, BLUE, 3)
        prev_x_tp, prev_y_tp = x, y_tp

        # 标注Worker数（X轴）
        draw_circle(x, HEIGHT-PADDING+10, 4, BLACK)

    # ==================== 生成标准PNG文件（关键：修复格式问题）====================
    with open(filename, 'wb') as f:
        # 1. PNG签名
        f.write(b'\x89PNG\r\n\x1a\n')

        # 2. IHDR chunk（图像头）
        ihdr_data = struct.pack('!IIBBBBB', WIDTH, HEIGHT, 8, 2, 0, 0, 0)
        f.write(struct.pack('!I', len(ihdr_data)))
        f.write(b'IHDR')
        f.write(ihdr_data)
        f.write(struct.pack('!I', zlib.crc32(b'IHDR' + ihdr_data) & 0xFFFFFFFF))

        # 3. IDAT chunk（像素数据）
        # 转换像素为字节流（RGB格式）
        pixel_data = b''
        for y in range(HEIGHT):
            pixel_data += b'\x00'  # 过滤字节（无过滤）
            for x in range(WIDTH):
                r, g, b = pixels[y * WIDTH + x]
                pixel_data += struct.pack('!BBB', r, g, b)
        
        # 压缩像素数据
        compressed_data = zlib.compress(pixel_data, 9)
        f.write(struct.pack('!I', len(compressed_data)))
        f.write(b'IDAT')
        f.write(compressed_data)
        f.write(struct.pack('!I', zlib.crc32(b'IDAT' + compressed_data) & 0xFFFFFFFF))

        # 4. IEND chunk（文件尾）
        f.write(struct.pack('!I', 0))
        f.write(b'IEND')
        f.write(struct.pack('!I', zlib.crc32(b'IEND') & 0xFFFFFFFF))

    print(f"✅ 标准PNG图片生成成功！路径：{os.path.abspath(filename)}")
    print("📌 打开方式：")
    print("   - Mac：右键 → 打开方式 → 预览")
    print("   - Windows：双击直接打开")

if __name__ == "__main__":
    generate_standard_png()
