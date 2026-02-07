#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
process_dna_plink2_super.py – Super Honest Power + PLINK2 + Threads + Safe Merge
"""

import subprocess
import sys
import csv
import os
from pathlib import Path

REFERENCE_PANEL_PREFIX = "ref_panel"
QPADM_PATH = "qpAdm"
CONVERTF_PATH = "convertf"

def log(msg):
    print(f"[*] {msg}")

def run_cmd(cmd, fallback_cmd=None):
    try:
        subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        log(f"خطأ في: {' '.join(cmd)}")
        if fallback_cmd:
            log("جاري تجربة الإصدار القديم (plink 1.9)...")
            subprocess.run(fallback_cmd, check=True)
        else:
            raise e

def clean_and_sort_for_plink(input_path, output_path):
    log(f"تنظيف وترتيب: {input_path}")
    try:
        data_rows = []
        skipped = 0
        with open(input_path, 'r', encoding='utf-8', errors='ignore') as f:
            sample = f.read(2048)
            f.seek(0)
            delimiter = '\t' if '\t' in sample else ','
            reader = csv.reader(f, delimiter=delimiter)
            header_skipped = False
            for row in reader:
                if len(row) < 4 or not row[0].strip():
                    continue
                if not header_skipped and ('rsid' in row[0].lower() or row[0].startswith('#')):
                    header_skipped = True
                    continue

                rsid = row[0].strip()
                chrom_raw = row[1].strip().upper().replace('CHR', '')
                pos_str = row[2].strip()
                if not pos_str.isdigit():
                    skipped += 1
                    continue

                geno_raw = row[3].strip().upper()
                geno_clean = geno_raw.replace(' ', '').replace('/', '').replace('|', '')
                geno = geno_clean[:2] if len(geno_clean) >= 2 else '--'
                if geno in {'II', 'DD', '00', '..', 'XX'}:
                    geno = '--'

                chr_map = {'X': 23, 'Y': 24, 'MT': 26, 'M': 26}
                chr_num = chr_map.get(chrom_raw, int(chrom_raw) if chrom_raw.isdigit() else None)
                if chr_num is None or not (1 <= chr_num <= 26 and chr_num != 25):
                    skipped += 1
                    continue

                chrom_out = 'X' if chr_num == 23 else 'Y' if chr_num == 24 else 'MT' if chr_num == 26 else str(chr_num)

                data_rows.append((chr_num, int(pos_str), rsid, chrom_out, pos_str, geno))

        data_rows.sort(key=lambda x: (x[0], x[1]))

        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            for r in data_rows:
                f.write(f"{r[2]}\t{r[3]}\t{r[4]}\t{r[5]}\n")

        log(f"تم التنظيف: {len(data_rows)} SNP صالح (تم تخطي {skipped})")
        return True
    except Exception as e:
        log(f"خطأ في التنظيف: {e}")
        return False

def run_full_pipeline(filepath, kit_id):
    filepath = Path(filepath).resolve()
    if not filepath.exists():
        print("الملف غير موجود!")
        sys.exit(1)

    kit_dir = filepath.parent
    temp_clean = kit_dir / f"sorted_temp_{kit_id}.txt"
    out_prefix = str(kit_dir / kit_id)
    merged_prefix = f"{out_prefix}_merged"
    pruned_prefix = f"{out_prefix}_pruned"
    results_dir = kit_dir / f"{kit_id}_results"
    results_dir.mkdir(exist_ok=True)

    threads = str(os.cpu_count() or 4)

    try:
        if clean_and_sort_for_plink(filepath, temp_clean):
            log("تحويل إلى BED بـ plink2")
            run_cmd([
                "plink2", "--23file", str(temp_clean),
                "--out", out_prefix, "--make-bed", "--allow-no-sex", "--allow-extra-chr"
            ])

            ref_bed = str(Path(__file__).parent / "reference" / REFERENCE_PANEL_PREFIX)
            if Path(f"{ref_bed}.bed").exists():
                log("دمج حقيقي مع ref_panel (17k شخص)")
                merge_cmd = [
                    "plink2", "--bfile", out_prefix,
                    "--bmerge", ref_bed, "--make-bed",
                    "--out", merged_prefix, "--allow-no-sex", "--merge-mode", "6"
                ]
                merge_fallback = [
                    "plink", "--bfile", out_prefix,
                    "--bmerge", ref_bed, "--make-bed",
                    "--out", merged_prefix, "--allow-no-sex", "--merge-mode", "6"
                ]
                run_cmd(merge_cmd, fallback_cmd=merge_fallback)
                base_prefix = merged_prefix
            else:
                log("تحذير: ref_panel مش موجود")
                base_prefix = out_prefix

            log("Pruning سريع")
            run_cmd([
                "plink2", "--bfile", base_prefix,
                "--indep-pairwise", "50", "5", "0.2",
                "--maf", "0.05", "--geno", "0.1",
                "--threads", threads,
                "--out", pruned_prefix
            ])

            log("PCA على المواقع المختارة")
            run_cmd([
                "plink2", "--bfile", base_prefix,
                "--extract", f"{pruned_prefix}.prune.in",
                "--pca", "30",
                "--threads", threads,
                "--out", str(results_dir / f"{kit_id}_PCA_30")
            ])

            merged_bed = f"{base_prefix}.bed"
            log("Admixture")
            for k in [5, 8, 10, 13]:
                subprocess.run([
                    "admixture", "--cv", "--acceleration", merged_bed, str(k)
                ], check=True)
                for ext in [".Q", ".P"]:
                    src = f"{merged_bed}.{k}{ext}"
                    if Path(src).exists():
                        os.rename(src, str(results_dir / f"{kit_id}_K{k}{ext}"))

            if os.path.exists(CONVERTF_PATH) and os.path.exists(QPADM_PATH):
                log("تحضير qpAdm")
                eigen_prefix = f"{kit_id}_eigen"
                with open("par_convert.txt", "w") as par:
                    par.write(f"genotypename: {base_prefix}.bed\n")
                    par.write(f"snpname: {base_prefix}.bim\n")
                    par.write(f"indivname: {base_prefix}.fam\n")
                    par.write("outputformat: EIGENSTRAT\n")
                    par.write(f"genooutfilename: {eigen_prefix}.geno\n")
                    par.write(f"snpoutfilename: {eigen_prefix}.snp\n")
                    par.write(f"indoutfilename: {eigen_prefix}.ind\n")

                subprocess.run([CONVERTF_PATH, "-p", "par_convert.txt"], check=True)

                log("تشغيل qpAdm")
                par_qpadm = f"{kit_id}_qpadm.par"
                with open(par_qpadm, "w") as p:
                    p.write(f"leftpops:\n  {kit_id}\n  Natufian\n  Anatolian_N\n  Iran_N\n  Levant_BA\n")
                    p.write("rightpops:\n  Mbuti\n  Ami\n  Onge\n  Papuan\n")
                    p.write("details: YES\nallsnps: NO\n")

                with open(str(results_dir / f"{kit_id}_qpAdm.txt"), "w") as outf:
                    subprocess.run([QPADM_PATH, "-p", par_qpadm], stdout=outf, check=True)

            log(f"★ انتهى كل شيء! النتائج في: {results_dir}")

    except Exception as e:
        log(f"خطأ: {e}")
    finally:
        if temp_clean.exists():
            temp_clean.unlink()
        for tmp in ["par_convert.txt", f"{kit_id}_qpadm.par"]:
            if Path(tmp).exists():
                Path(tmp).unlink()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("الاستخدام: python3 process_dna_plink2_super.py <مسار_الملف> <kit_id>")
        sys.exit(1)
    run_full_pipeline(sys.argv[1], sys.argv[2])