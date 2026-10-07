import csv, statistics, os

BASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'jmeter', 'results')
A = os.path.join(BASE, 'A.csv')
B = os.path.join(BASE, 'B.csv')


def load(p):
    with open(p, newline='') as f:
        return list(csv.DictReader(f))


def fill(rows):
    n = len(rows)
    h = sum(1 for r in rows if (r.get('hasAd') or '').strip() == '1')
    return (h / n * 100 if n else 0.0), n


def nums(rows, col):
    out = []
    for r in rows:
        v = (r.get(col) or '').strip()
        if v and v not in ('null', 'NF'):
            try:
                out.append(float(v))
            except ValueError:
                pass
    return out


def nonempty(rows, col):
    return sum(1 for r in rows if (r.get(col) or '').strip() not in ('', 'null', 'NF'))


a, b = load(A), load(B)
fa, na = fill(a)
fb, nb = fill(b)
wa = statistics.mean(nums(a, 'winBid')) if nums(a, 'winBid') else 0.0
wb = statistics.mean(nums(b, 'winBid')) if nums(b, 'winBid') else 0.0
pb_list = nums(b, 'payPrice')
pb = statistics.mean(pb_list) if pb_list else None

pa_ne = nonempty(a, 'payPrice')
pb_ne = nonempty(b, 'payPrice')

print("=" * 64)
print(f"A组 (M3 一价): 样本={na}  填充率={fa:.2f}%  winBid均价={wa:.4f}  [payPrice非空行={pa_ne}]")
if pa_ne > 0:
    print("  ⚠️ A组含 payPrice 非空行，疑似误测成 M4！请确认服务是 M3 一价。")
print(f"B组 (M4 二价): 样本={nb}  填充率={fb:.2f}%  winBid均价={wb:.4f}  "
      f"payPrice均价={pb if pb is not None else 'N/A'}  [payPrice非空行={pb_ne}]")
if pb_ne == 0:
    print("  ⚠️ B组 payPrice 全空，疑似误测成 M3！请确认服务是 M4 二价。")
print("-" * 64)
if pb is not None and wa:
    print(f"成交价变化% = (payPrice_B - winBid_A) / winBid_A")
    print(f"            = ({pb:.4f} - {wa:.4f}) / {wa:.4f} = {(pb - wa) / wa * 100:+.2f}%")
    print(f"填充率变化  = {fb - fa:+.2f} 个百分点 （二价理论上不影响填充率，应≈0）")
print("=" * 64)
