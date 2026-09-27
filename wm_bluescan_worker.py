# -*- coding: utf-8 -*-
"""Per-file blue-watermark scanner worker (subprocess-isolated).
Prints RESULT + json: {"file":..., "max_page_frac":..., "mean_frac":..., "pages":N}"""
import sys, json

def main():
    pdf = sys.argv[1]
    try:
        import pymupdf as fitz
        import numpy as np
        import cv2
        doc = fitz.open(pdf)
        fr, vis = [], []
        for i in range(min(len(doc), 10)):
            pix = doc[i].get_pixmap(matrix=fitz.Matrix(1.0, 1.0))
            arr = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.height, pix.width, pix.n)[..., :3]
            ai = arr.astype(np.int16)
            R, G, B = ai[..., 0], ai[..., 1], ai[..., 2]
            vals = arr.mean(axis=2)
            m = (B - R > 12) & (B > 110) & (R > 70) & (B - G > 2) & (vals > 185)
            fr.append(round(float(m.mean()), 5))
            # visible watermark = bluish far from any dark text anchor (on white bg)
            dark = vals < 170
            dist = cv2.distanceTransform((~dark).astype(np.uint8), cv2.DIST_L2, 5)
            vis.append(round(float((m & (dist > 5)).mean()), 5))
        doc.close()
        res = {"file": pdf, "pages": len(fr),
               "max_page_frac": max(fr) if fr else 0.0,
               "mean_frac": round(sum(fr) / len(fr), 5) if fr else 0.0,
               "max_visible_frac": max(vis) if vis else 0.0}
    except Exception as e:
        res = {"file": pdf, "status": "fail", "err": repr(e)[:200]}
    sys.stdout.write("RESULT" + json.dumps(res))
    sys.stdout.flush()

if __name__ == "__main__":
    main()
