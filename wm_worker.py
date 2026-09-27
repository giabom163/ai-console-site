#!/usr/bin/env python
# -*- coding: utf-8 -*-
r"""单文件去水印 worker（在独立子进程里跑）。
用途：把「一个文件的完整去水印」放进独立进程，天然隔离 PyMuPDF 的 C 层崩溃
      （个别异常 PDF 会让进程直接段错误、无 Python 回溯，try/except 抓不到）；
      同时让 pipeline.step_watermark 能用多进程并发吃满多核。

用法：
  python wm_worker.py <pdf> <out_root> <src_root> <dry 0|1>

输出：stdout 末尾打印 "RESULT" + 一行 JSON（pipeline 父进程解析）。
崩溃/异常时进程退出码非 0 或没有 RESULT，父进程按 fail 处理。
"""
import os
import sys
import json
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pipeline as P  # noqa: E402


def main():
    if len(sys.argv) < 5:
        sys.stderr.write("usage: wm_worker.py <pdf> <out_root> <src_root> <dry 0|1>\n")
        sys.exit(2)
    pdf, out_root, src_root, dry = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4] == "1"
    try:
        res = P._wm_process_one(pdf, out_root, src_root, dry=dry)
    except Exception:
        res = {"file": pdf, "status": "fail", "route": "exc",
               "err": traceback.format_exc()[-800:]}
    # ensure_ascii=True（默认）-> 纯 ASCII 输出，中文以 \uXXXX 转义，父进程 json.loads 还原，
    # 彻底规避子进程 stdout 编码（GBK/UTF-8）差异导致的乱码。
    sys.stdout.write("RESULT" + json.dumps(res))
    sys.stdout.flush()


if __name__ == "__main__":
    main()
