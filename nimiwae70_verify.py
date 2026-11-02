"""D70 verification. Independent of the training path; synthetic lineages, so no frame is needed. Usage: python -I nimiwae70_verify.py"""
import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import math
import numpy as np, pandas as pd, torch, torch.nn.functional as F
import nimiwae56 as M, nimiwae70 as A
RES = []
def check(name, ok, detail=''):
    RES.append((name, bool(ok))); print(f"{'PASS' if ok else 'FAIL'}  {name}  {detail}", flush=True)

def batch(n=6):
    T = M.T_MAX; B = {}
    v = torch.zeros(n, T); v[:, :5] = 1
    for k in ('lD', 'lH', 'mD', 'mH', 'cr', 'mcr', 'cc', 'mcc', 'hid', 'real', 'fmp', 'psamp', 'sampled', 'in_span', 'm_dmg', 'brk', 'm_brk', 'm_cl'): B[k] = torch.zeros(n, T)
    B['v'] = v; B['llim'] = torch.full((n, T), math.log(0.95)); B['llo'] = torch.full((n, T), -20.0); B['lex'] = torch.full((n, T), math.log(0.95))
    B['psamp'][:] = 1; B['sampled'][:] = 1; B['in_span'] = v.clone(); B['real'][:, 2:4] = 1; B['mD'][:, 2:4] = 1; B['lD'][:, 2] = math.log(2.0); B['lD'][:, 3] = math.log(3.0)
    B['ev'] = torch.zeros(n, T, dtype=torch.long); B['ev'][:, 2:4] = M.EV['true_hard']; B['mode'] = torch.zeros(n, T, dtype=torch.long); B['cl'] = torch.zeros(n, T, dtype=torch.long)
    cov = torch.zeros(n, T, M.NCOV); yrs = torch.tensor([1.3, 6.7, 11.1, 17.4, 21.9, 0, 0, 0])[None].expand(n, -1)   # irregular intervals
    cov[..., A.ITYR] = yrs / 20; B['cov'] = cov; B['dmg'] = torch.zeros(n, T, 9)
    for k in ('sp', 'l3', 'maj', 'fty', 'spc', 'moved', 'ndef', 'cause', 'scl', 'phy', 'rcd'): B[k] = torch.ones(n, dtype=torch.long)
    B['moved'][:] = 0; B['ndef'][:] = 0; B['f'] = torch.full((n,), 2, dtype=torch.long); B['l'] = torch.full((n,), 3, dtype=torch.long)
    B['snum'] = torch.zeros(n, 9); B['summ'] = torch.zeros(n, M.NSUMM)
    B['c_spc'] = torch.ones(n, dtype=torch.long); B['c_grp'] = torch.ones(n, dtype=torch.long); B['c_num'] = torch.randn(n, A.NCX)
    return B

def model(age=True):
    torch.manual_seed(0)
    return A.NIMIWAE70(3, 3, 3, 3, 3, 3, 3, age=age)

m = model(); B = batch(); z = torch.randn(4, 6, M.DZ)
# 1. your constraint: age advances exactly with elapsed time, for every sample and tree
a = m.ages(z, B).detach(); t = B['cov'][..., A.ITYR] * 20
d = (a - a[..., :1]) - (t - t[:, :1])[None]
check('age step = elapsed years at every slot', float(d[..., :5].abs().max()) < 1e-4, f'max |error| {float(d[..., :5].abs().max()):.1e} yr')
check('one birth date: t - a_t is constant over slots', float(((t[None] - a)[..., :5].std(-1)).max()) < 1e-4)
# 2. curve monotone in age for random inputs and weights
tok = m.curve.token(B) + torch.randn(6, 1 + A.NKNOT); grid = torch.exp(torch.linspace(0, math.log(400), 200))
ls = torch.stack([m.curve.log_s(tok, g.expand(6)) for g in grid], 1)
check('curve monotone in age', bool((ls[:, 1:] - ls[:, :-1] >= -1e-6).all()))
# 3. existence gate against the written formula
lp1 = F.logsigmoid(40 * (torch.randn(4, 6, M.T_MAX))); lp0 = torch.log(-torch.expm1(lp1.clamp(max=-1e-6)))
l1, l0, born = m.born_gate(lp1, lp0, z, B)
e = torch.exp(lp1) * torch.sigmoid(A.KAPPA_B * m.ages(z, B))
check('P(exists) = P(size step) x sigma(kappa a_t)', float((torch.exp(l1) - e).abs().max()) < 1e-5)
check('P(exists) + P(not) = 1', float((torch.exp(l1) + torch.exp(l0) - 1).abs().max()) < 1e-4)
late = (m.ages(z, B) < -2)
if late.any(): check('no existence 2 yr before birth', float(torch.exp(l1)[late].max()) < 0.02)
# 4. age off = the base model: same draws, same log weights
torch.manual_seed(1); b0 = model(age=False); B0 = batch()
class Base(M.NIMIWAE56): pass
torch.manual_seed(5); o = b0.joint(B0, 8)
o2 = None
b0.born_gate = M.NIMIWAE56.born_gate.__get__(b0); b0.extra_lw = M.NIMIWAE56.extra_lw.__get__(b0)
torch.manual_seed(5); o2 = b0.joint(B0, 8)
check('hooks are inert when age is off', float((o['lw'] - o2['lw']).abs().max()) == 0.0)
torch.manual_seed(5); oa = m.joint(B0, 8)
check('hooks act when age is on', float((oa['lw'] - o['lw']).abs().max()) > 0)
# 5. record likelihood against a hand computation
R = dict(c_spc=torch.ones(3, dtype=torch.long), c_grp=torch.ones(3, dtype=torch.long), c_num=torch.zeros(3, A.NCX), src=torch.tensor([A.TOT, A.BHA, A.SITE]), cc=torch.tensor([1, 5, A.CC_SITE]),
         a_obs=torch.tensor([20., 20., 20.]), lD=torch.log(torch.tensor([6., 6., 6.])))
ll = m.rec_ll(R).detach(); tk = m.curve.token(R); l = m.curve.years_bh(R['c_grp'])
hand = []
for i in range(3):
    ag = 20.0 + (0.0 if i == 0 else float(l[i])); mu = float(m.curve.log_s(tk[i:i + 1], torch.tensor([ag]))[0]) + float(m.curve.delta(R['cc'][i:i + 1])[0, 0])
    var = 0.02 ** 2 + (0.06 / 6) ** 2 + math.exp(2 * float(m.lsS)) + math.exp(2 * float(m.lstau))
    hand.append(-0.5 * (math.log(6) - mu) ** 2 / var - 0.5 * math.log(var) - 0.5 * math.log(2 * math.pi))
check('record likelihood = hand formula; TOTAGE gets no breast-height years', float((ll - torch.tensor(hand)).abs().max()) < 1e-4)
# 5b. years to breast height: the prior is centred on l0, by species group, with one term per group
mm = A.NIMIWAE70(3, 3, 3, 3, 3, 3, 3, l0=[5.0, 2.0, 5.0, 2.0])
g_ = torch.tensor([1, 2, 3]); check('years to breast height start at the softwood and hardwood values', torch.allclose(mm.curve.years_bh(g_), torch.tensor([2.0, 5.0, 2.0])))
check('the prior is 0 at those values', float(mm.curve.log_prior_bh()) == 0.0)
with torch.no_grad(): mm.curve.eps_l.weight[1] = 0.3
check('one prior sd on one group costs 0.5', abs(float(mm.curve.log_prior_bh()) + 0.5) < 1e-5)
check('a group moves its own value only', abs(float(mm.curve.years_bh(torch.tensor([1]))) - 2 * math.exp(0.3)) < 1e-5 and float(mm.curve.years_bh(torch.tensor([2]))) == 5.0)
# 6. gradient routing
m.zero_grad(); m.rec_ll(R).sum().backward()
enc = [p for p in m.enc_params() if p.grad is not None and p.grad.abs().sum() > 0]
dec = [p for mod in (m.dtr, m.dout, m.bhead) for p in mod.parameters() if p.grad is not None and p.grad.abs().sum() > 0]
cur = [p for p in m.curve.parameters() if p.grad is not None and p.grad.abs().sum() > 0]
check('records reach the curve, not the encoder or decoder', len(enc) == 0 and len(dec) == 0 and len(cur) > 0)
m.zero_grad(); torch.manual_seed(2); oo = m.joint(batch(), 4); (-oo['lw'].mean()).backward()
check('lineages reach the curve and the birth head', m.curve.out.weight.grad.abs().sum() > 0 and m.bhead.weight.grad.abs().sum() > 0)
# 7. no leak: the birth date does not depend on the tree's records or on where its last record falls
Bb = batch(); Bc = {k: v.clone() for k, v in Bb.items()}; Bc['lD'] = Bc['lD'] + 1.0; Bc['mD'] = torch.zeros_like(Bc['mD']); Bc['l'][:] = 2; Bc['f'][:] = 1; torch.manual_seed(3); zz = torch.randn(2, 6, M.DZ)
check('birth date depends on z and statics only', float((m.ages(zz, Bb) - m.ages(zz, Bc)).abs().max()) == 0.0)
# 8. birth can fall inside the lineage: slots before it have no existence, in probabilities and in sampled lineages
young = model(); B8 = batch(); B8['lD'][:] = 0; B8['ev'][:] = 0; B8['real'][:] = 0; B8['mD'][:] = 0
B8['real'][:, 4] = 1; B8['mD'][:, 4] = 1; B8['lD'][:, 4] = math.log(1.5); B8['ev'][:, 4] = M.EV['true_hard']; B8['f'][:] = 4; B8['l'][:] = 4
with torch.no_grad(): young.bhead.bias.fill_(math.log(3.0)); young.bhead.weight.zero_(); young.dout.bias[0] = 3.0
aY = young.ages(torch.zeros(1, 6, M.DZ), B8)[0, 0]
check('birth can fall after the first slots', float(aY[0]) < 0 and float(aY[3]) < 0 and float(aY[4]) > 0, f'ages {[round(float(x), 1) for x in aY[:5]]}')
L = M.sample_lineages(young, B8, n_draws=5, K=8)
check('sampled lineages have no tree before birth', float(np.nansum(L['exists'][:, :, :4])) == 0.0)
Pr = M.impute(young, B8, K=16, nsamp=20)
lim = torch.sigmoid(A.KAPPA_B * aY[:4].detach())
check('P(exists) never exceeds the birth step before birth', bool((Pr['pE'][:, :4] <= lim + 1e-5).all()), f'limit by slot {[round(float(x), 5) for x in lim]}')
# 9. a tree cannot carry tally size before birth
tall = model(); B9 = batch()
with torch.no_grad(): tall.bhead.bias.fill_(math.log(3.0)); tall.bhead.weight.zero_()
zz9 = torch.zeros(1, 6, M.DZ); aa = tall.ages(zz9, B9); neg = (aa < 0) & (B9['v'][None] > 0)
small = torch.full((1, 6, M.T_MAX), math.log(0.2)); big = torch.full((1, 6, M.T_MAX), math.log(3.0)); mp0 = torch.zeros(6, M.DZ); lp0 = torch.zeros(6, M.DZ)
check('unborn slots penalise tally size', float((tall.extra_lw(small, zz9, B9, mp0, lp0) - tall.extra_lw(big, zz9, B9, mp0, lp0)).min()) > 0 or not bool(neg.any()), f'{int(neg.sum())} unborn slots')
# 12. vigour has a real prior: under p(z) it is N(0, tau), whatever the prior's own location and scale
torch.manual_seed(9); mpv = torch.randn(2000, M.DZ) * 3; lpv = torch.randn(2000, M.DZ); zv = mpv + torch.exp(0.5 * lpv) * torch.randn(4, 2000, M.DZ)
vv = tall.vigour(zv, mpv, lpv).detach(); tau = float(torch.exp(tall.lstau))
check('vigour is N(0, tau) under the prior', abs(float(vv.mean())) < 0.03 and abs(float(vv.std()) / tau - 1) < 0.03, f'mean {float(vv.mean()):+.3f}, sd {float(vv.std()):.3f}, tau {tau:.3f}')
# 10. E1 mask: only slots before the move, only trees first recorded at or after it
s_ = pd.DataFrame({'statecd': 1, 'unitcd': 1, 'countycd': 1, 'plot': 1, 'subp': 1, 'tree': [1, 1, 1, 2, 2, 2, 3, 3], 'invyr': [2002, 2007, 2012, 2002, 2007, 2012, 2002, 2007],
                   'slot_kind': ['pre', 'pre', 'real', 'real', 'real', 'post', 'pre', 'real'], 'move_invyr': [2007, 2007, 2007, 2007, 2007, 2007, np.nan, np.nan]})
check('E1 mask: post-move debut only, slots before the move only', A.e1_mask(s_).tolist() == [True, False, False, False, False, False, False, False])
# 11. the joint model routes prediction through its own build
check('prediction uses the joint build', getattr(young, 'build', None) is not M.build and 'build' in A.NIMIWAE70.__dict__)
print('checks:', sum(o for _, o in RES), 'of', len(RES)); sys.exit(0 if all(o for _, o in RES) else 1)
