"""包装节拍对照实验：异方差 Welch t 与均值差区间。"""
import math
import random
import numpy as np
from scipy import stats


def metadata():
    return {"name": "包装线节拍对照实验", "short": "pack-welch", "tagline": "把工艺调整的节拍差，放进可复查的实验区间。", "client": "榆川包装 · 52 人食品外包装车间，工艺组比较两种封装动作", "workflow": "工艺员用秒表登记每段节拍，把两列平均值截图交给生产经理。", "pains": ["忽略两组方差差异", "只报改善秒数没有不确定区间", "班次混杂未单独观察"], "upstream": "随机分配试运行片段 YC_pack_trial.csv", "downstream": "工艺经理决定是否继续扩大试验",
            "entities": ["实验片段", "节拍比较", "工艺评审简报"], "dataset_name": "A 原动作 / B 改动作 · 160 段观测",
            "columns": [{"key": "run", "label": "片段", "type": "string"}, {"key": "arm", "label": "实验组", "type": "string"}, {"key": "shift", "label": "班次", "type": "string"}, {"key": "seconds", "label": "单件节拍/秒", "type": "number"}],
            "parameters": [{"key": "confidence", "label": "区间置信度", "type": "select", "default": "0.95", "options": [{"value": str(v), "label": f"{v*100:g}%"} for v in [.9, .95, .99]]}, {"key": "shift", "label": "实验班次", "type": "select", "default": "全部", "options": [{"value": n, "label": n} for n in ["全部", "早班", "晚班"]]}],
            "workbench_title": "节拍效果核验", "action_label": "执行 Welch 比较", "unit": "秒", "accent": "oklch(58% 0.16 69)", "style": "industrial",
            "seed_story": ["08:20 工艺员导入 YC_pack_trial.csv，A/B 各 80 段。", "08:30 核查片段按班次交错随机分配。", "08:40 分析员选择 95% 置信度和全部班次。", "08:45 执行 Welch 检验，查看 A−B 改善秒数。", "09:00 检查置信区间是否跨 0。", "09:20 改为晚班重算，查看异方差影响。", "09:45 导出工艺评审简报，交经理安排扩大试验。"],
            "method": "改善量=均值(A)−均值(B)；SE=√(sA²/nA+sB²/nB)；Welch–Satterthwaite 自由度；双侧 t 检验和 t 分布置信区间；Cohen d 用合并标准差。", "limitations": ["假定观测片段近似独立", "未校正多个班次的探索性比较", "显著节拍差不能证明成品质检合格"]}


def seed_records():
    rng = random.Random(1004)
    rows = []
    for arm, mu, sigma in [("A", 19.5, 1.4), ("B", 18.2, 2.1)]:
        for i in range(80):
            shift = "早班" if i % 2 == 0 else "晚班"
            rows.append({"run": f"YC-{arm}{i+1:03}", "arm": arm, "shift": shift, "seconds": round(max(1, rng.gauss(mu + (.8 if shift == "晚班" else 0), sigma)), 3)})
    return rows


def _validate_params(params):
    try:
        confidence = float(params.get("confidence", .95))
    except (TypeError, ValueError):
        raise ValueError("置信度只接受 0.90/0.95/0.99")
    if confidence not in (.9, .95, .99):
        raise ValueError("置信度只接受 0.90/0.95/0.99")
    shift = str(params.get("shift", "全部"))
    if shift not in ("全部", "早班", "晚班"):
        raise ValueError("未知班次")
    return confidence, shift


def analyze(records, params):
    confidence, shift = _validate_params(params)
    groups = {"A": [], "B": []}
    seen = set()
    for r in records:
        try:
            arm, run = str(r["arm"]), str(r["run"])
            seconds = float(r["seconds"])
            if arm not in groups or not math.isfinite(seconds) or seconds <= 0 or run in seen:
                raise ValueError()
            seen.add(run)
            if shift == "全部" or r["shift"] == shift:
                groups[arm].append(seconds)
        except (KeyError, TypeError, ValueError):
            raise ValueError("片段号须唯一，组别为 A/B，节拍必须为正有限数")
    return _finish(groups, confidence, shift)


class Moments:
    """Welford 在线均值/方差，不保留原始观测。"""
    __slots__ = ("n", "mean", "m2")

    def __init__(self):
        self.n = 0
        self.mean = 0.0
        self.m2 = 0.0

    def add(self, x):
        self.n += 1
        delta = x - self.mean
        self.mean += delta / self.n
        self.m2 += delta * (x - self.mean)

    @property
    def var(self):
        return self.m2 / (self.n - 1) if self.n > 1 else 0.0


def _finish(groups, confidence, shift):
    na, nb = len(groups["A"]), len(groups["B"])
    if min(na, nb) < 3:
        raise ValueError("每组至少需要 3 个独立实验片段")
    a, b = np.array(groups["A"]), np.array(groups["B"])
    ma, mb, va, vb = float(a.mean()), float(b.mean()), float(a.var(ddof=1)), float(b.var(ddof=1))
    return _report(ma, mb, va, vb, na, nb, confidence, shift)


def _report(ma, mb, va, vb, na, nb, confidence, shift):
    se2 = va / na + vb / nb
    if se2 <= 1e-15:
        raise ValueError("两组均无变异，Welch 检验无法估计误差")
    df = se2 * se2 / ((va / na) ** 2 / (na - 1) + (vb / nb) ** 2 / (nb - 1))
    diff, se = ma - mb, math.sqrt(se2)
    statistic = diff / se
    p = float(2 * stats.t.sf(abs(statistic), df))
    margin = float(stats.t.ppf((1 + confidence) / 2, df)) * se
    low, high = diff - margin, diff + margin
    pooled = math.sqrt(((na - 1) * va + (nb - 1) * vb) / (na + nb - 2))
    effect = diff / pooled
    rows = [{"arm": k, "n": n, "mean": round(m, 4), "sd": round(math.sqrt(v), 4)}
            for k, n, m, v in (("A", na, ma, va), ("B", nb, mb, vb))]
    return {"summary": f"B 相对 A 平均节拍缩短 {diff:.3f} 秒；{confidence*100:g}% 区间为 [{low:.3f}, {high:.3f}] 秒。",
            "metrics": [{"label": "改善量 A−B", "value": round(diff, 5), "unit": "秒"}, {"label": "双侧 p 值", "value": float(f"{p:.6g}")}, {"label": "Cohen d", "value": round(effect, 5)}, {"label": "Welch 自由度", "value": round(df, 3)}],
            "table": {"columns": [{"key": "arm", "label": "实验组"}, {"key": "n", "label": "片段数"}, {"key": "mean", "label": "均值/秒"}, {"key": "sd", "label": "样本标准差/秒"}], "rows": rows},
            "chart": {"type": "bar", "labels": ["A 原动作", "B 改动作"], "series": [{"name": "平均单件节拍/秒", "values": [round(ma, 4), round(mb, 4)]}]},
            "findings": [f"区间下界 {low:.4f} 秒，上界 {high:.4f} 秒；{'区间未跨 0' if low > 0 or high < 0 else '区间跨 0'}。", f"t={statistic:.4f}，自由度={df:.3f}；分析班次：{shift}。", "仅在分组随机、片段独立的前提下支持实验推断；扩大试验仍须核查质检指标。"], "method": metadata()["method"], "warnings": ["班次子组比较为探索性分析，未调整多重检验。"]}


def analyze_batches(batch_iter, params):
    """流式版本：batch_iter 逐批产出已入库的 (arm, shift, seconds) 规格化行。

    只保留 Welford 聚合量，内存不随总行数增长。片段号唯一性由解析期全量
    去重保证，与 analyze() 对整份记录的计算口径一致。
    """
    confidence, shift = _validate_params(params)
    moments = {"A": Moments(), "B": Moments()}
    for batch in batch_iter:
        for arm, row_shift, seconds in batch:
            if shift == "全部" or row_shift == shift:
                moments[arm].add(seconds)
    a, b = moments["A"], moments["B"]
    if min(a.n, b.n) < 3:
        raise ValueError("每组至少需要 3 个独立实验片段")
    return _report(a.mean, b.mean, a.var, b.var, a.n, b.n, confidence, shift)
