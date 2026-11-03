"""D70 verification. Independent of the training path; synthetic lineages, so no frame is needed. Usage: python -I nimiwae70_verify.py [DATA_DIR]; with a frame, checks 14 use real trees."""
import sys, os; sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import math
import numpy as np, pandas as pd, torch, torch.nn.functional as F
import nimiwae56 as M, nimiwae70 as A
_float = float
def float(x):                                                 # tensors are read as values here; gradients are tested separately
    return x.detach().item() if torch.is_tensor(x) else _float(x)

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
    B['soil'] = torch.randn(n, A.NSOIL); B['af_mu'] = torch.full((n,), math.log(30.0)); B['af_sd'] = torch.full((n,), 0.4); B['la'] = torch.full((n,), 3, dtype=torch.long); B['af_has'] = torch.ones(n); B['l0'] = torch.full((n,), 2.0)
    return B

def model(age=True):
    torch.manual_seed(0)
    return A.NIMIWAE70(3, 3, 3, 3, 3, age=age)

m = model(); B = batch(); z = torch.randn(4, 6, M.DZ)
# 1. your constraint: age advances exactly with elapsed time, for every sample and tree
a = m.ages(z, B).detach(); t = B['cov'][..., A.ITYR] * 20
d = (a - a[..., :1]) - (t - t[:, :1])[None]
check('age step = elapsed years at every slot', float(d[..., :5].abs().max()) < 1e-4, f'max |error| {float(d[..., :5].abs().max()):.1e} yr')
check('one birth date: t - a_t is constant over slots', float(((t[None] - a)[..., :5].std(-1)).max()) < 1e-4)
# 2. the age prior is exact: z0 ~ N(0, 1) under p(z) and log a_l = af_mu + af_sd z0
mp2, lp2 = m.p_z(B); check('z0 has a standard normal prior', float(mp2[:, 0].abs().max()) == 0.0 and float(lp2[:, 0].abs().max()) == 0.0)
check('other dimensions keep their learned prior', float(mp2[:, 1:].abs().max()) > 0)
zs_ = torch.randn(4000, 6, M.DZ); az = m.ages(zs_, B).detach()[:, :, 3]; la_ = torch.log(az)
check('age at the prior slot follows the direct model: mean and sd of log age', abs(float(la_.mean()) - math.log(30)) < 0.01 and abs(float(la_.std()) - 0.4) < 0.01, f'{float(la_.mean()):.3f} {float(la_.std()):.3f}')
check('no learned parameter moves the age prior', not any('bhead' in k or 'curve' in k for k in dict(m.named_parameters())))
# 3. existence gate against the written formula
lp1 = F.logsigmoid(40 * (torch.randn(4, 6, M.T_MAX))); lp0 = torch.log(-torch.expm1(lp1.clamp(max=-1e-6)))
with torch.no_grad():
    l1, l0, born = m.born_gate(lp1, lp0, z, B)
    e = torch.exp(lp1) * torch.sigmoid(A.KAPPA_B * (m.ages(z, B) - B['l0'][None, :, None]))
check('P(exists) = P(size step) x sigma(kappa (a_t - l0)): breast height first', float((torch.exp(l1) - e).abs().max()) < 1e-5)
check('P(exists) + P(not) = 1', float((torch.exp(l1) + torch.exp(l0) - 1).abs().max()) < 1e-4)
late = (m.ages(z, B).detach() < -2)
if late.any(): check('no existence 2 yr before birth', float(torch.exp(l1)[late].max()) < 0.02)
# 4. age off = the base model: same draws, same log weights
torch.manual_seed(1); b0 = model(age=False); B0 = batch()
class Base(M.NIMIWAE56): pass
torch.manual_seed(5); o = b0.joint(B0, 8)
o2 = None
b0.born_gate = M.NIMIWAE56.born_gate.__get__(b0); b0.extra_lw = M.NIMIWAE56.extra_lw.__get__(b0)
torch.manual_seed(5); o2 = b0.joint(B0, 8)
with torch.no_grad(): check('hooks are inert when age is off', float((o['lw'] - o2['lw']).abs().max()) == 0.0)
torch.manual_seed(5); oa = m.joint(B0, 8)
with torch.no_grad(): check('hooks act when age is on', float((oa['lw'] - o['lw']).abs().max()) > 0)
# 6. the decoder never reads z0
torch.manual_seed(2); zd = torch.randn(2, 6, M.DZ); zd2 = zd.clone(); zd2[..., 0] += 2.0; zd3 = zd.clone(); zd3[..., 1] += 2.0
with torch.no_grad():
    D1 = m.decode(zd, B)[0]; D2 = m.decode(zd2, B)[0]; D3 = m.decode(zd3, B)[0]
check('z0 does not reach the size path', float((D1 - D2).abs().max()) == 0.0); check('the other dimensions do', float((D1 - D3).abs().max()) > 0)
check('decode does not change z in place', float((zd[..., 0]).abs().max()) > 0)
b0d = model(age=False); check('age off keeps every dimension in the size path', float((b0d.decode(zd, B)[0] - b0d.decode(zd2, B)[0]).abs().max()) > 0)
# 6b. gradient routing
m.zero_grad(); torch.manual_seed(2); oo = m.joint(batch(), 4); (-oo['lw'].mean()).backward()
check('lineages train the encoder and decoder', sum(float(p.grad.abs().sum()) for p in m.enc_params() if p.grad is not None) > 0 and m.dout.weight.grad.abs().sum() > 0)
# 7. no leak: age depends on z and the prior's own slot, not on the tree's other records or where they end
Bb = batch(); Bc = {k: v.clone() for k, v in Bb.items()}; Bc['lD'] = Bc['lD'] + 1.0; Bc['mD'] = torch.zeros_like(Bc['mD']); Bc['l'][:] = 2; Bc['f'][:] = 1; torch.manual_seed(3); zz = torch.randn(2, 6, M.DZ)
check('birth date depends on z and the prior only', float((m.ages(zz, Bb) - m.ages(zz, Bc)).abs().max()) == 0.0)
Bq = batch(); Bq2 = {k: v.clone() for k, v in Bq.items()}; Bq2['af_mu'] = Bq2['af_mu'] + 1.0
with torch.no_grad():
    check('the decoder and prior do not read the age prior', float((m.p_z(Bq)[0] - m.p_z(Bq2)[0]).abs().max()) == 0.0 and float((m.decode(zd, Bq)[0] - m.decode(zd, Bq2)[0]).abs().max()) == 0.0)
    check('the encoder does', float((m.st_q(Bq) - m.st_q(Bq2)).abs().max()) > 0); check('age off: the encoder does not', float((b0d.st_q(Bq) - b0d.st_q(Bq2)).abs().max()) == 0.0)
# 8. birth can fall inside the lineage: slots before it have no existence, in probabilities and in sampled lineages
young = model(); B8 = batch(); B8['lD'][:] = 0; B8['ev'][:] = 0; B8['real'][:] = 0; B8['mD'][:] = 0
B8['real'][:, 4] = 1; B8['mD'][:, 4] = 1; B8['lD'][:, 4] = math.log(1.5); B8['ev'][:, 4] = M.EV['true_hard']; B8['f'][:] = 4; B8['l'][:] = 4
B8['la'][:] = 4; B8['af_mu'][:] = math.log(3.0); B8['af_sd'][:] = 1e-3
with torch.no_grad(): young.dout.bias[0] = 3.0
aY = young.ages(torch.zeros(1, 6, M.DZ), B8)[0, 0]
check('birth can fall after the first slots', float(aY[0]) < 0 and float(aY[3]) < 0 and float(aY[4]) > 0, f'ages {[round(float(x), 1) for x in aY[:5]]}')
L = M.sample_lineages(young, B8, n_draws=5, K=8)
check('sampled lineages have no tree before birth', float(np.nansum(L['exists'][:, :, :4])) == 0.0)
Pr = M.impute(young, B8, K=16, nsamp=20)
lim = torch.sigmoid(A.KAPPA_B * (aY[:4].detach() - B8['l0'][0]))
check('P(exists) never exceeds the birth step before birth', bool((Pr['pE'][:, :4] <= lim + 1e-5).all()), f'limit by slot {[round(float(x), 5) for x in lim]}')
# 9. a tree cannot carry tally size before birth
tall = model(); B9 = batch()
B9['af_mu'][:] = math.log(3.0); B9['af_sd'][:] = 1e-3
zz9 = torch.zeros(1, 6, M.DZ); aa = tall.ages(zz9, B9); neg = (aa < 0) & (B9['v'][None] > 0)
small = torch.full((1, 6, M.T_MAX), math.log(0.2)); big = torch.full((1, 6, M.T_MAX), math.log(3.0)); mp0 = torch.zeros(6, M.DZ); lp0 = torch.zeros(6, M.DZ)
check('unborn slots penalise tally size', float((tall.extra_lw(small, zz9, B9, mp0, lp0) - tall.extra_lw(big, zz9, B9, mp0, lp0)).min()) > 0 or not bool(neg.any()), f'{int(neg.sum())} unborn slots')
zg = torch.zeros(1, 6, M.DZ, requires_grad=True); (tall.extra_lw(big, zg, B9, mp0, lp0)).sum().backward(); check('the unborn term reaches z0', float(zg.grad[..., 0].abs().sum()) > 0)
# 15. the exact age posterior: no slot before the first record means the prior, whatever the draws; a limit moves it
mx = model(); Bx = batch(); Bx['la'][:] = 0; Bx['f'][:] = 0; Bx['af_mu'][:] = math.log(60.0); Bx['af_sd'][:] = 0.3; mx.eval()
torch.manual_seed(4); ox = mx.joint(Bx, 6); zx = ox['z'].detach()
torch.manual_seed(9); oy = mx.joint(Bx, 6, z=zx)
check('joint scores given draws: same draws, same weights', float((ox['lw'] - oy['lw']).abs().max()) < 1e-3, f"{float((ox['lw'] - oy['lw']).abs().max()):.1e}")
gx, px = A.age_posterior(mx, Bx, K=16); zq = A.z_quantiles(gx, px, [0.1, 0.5, 0.9])
check('no birth limit: the grid posterior is the prior for every tree', float((zq[1]).abs().max()) < 0.02 and float((zq[2] - 1.2816).abs().max()) < 0.03 and float((zq[0] + 1.2816).abs().max()) < 0.03, f'{[round(float(zq[i, 0]), 3) for i in range(3)]}')
check('the grid leaves no state behind', getattr(mx, '_cache', None) is None)
Bl = batch(); Bl['la'][:] = 4; Bl['f'][:] = 4; Bl['af_mu'][:] = math.log(4.0); Bl['af_sd'][:] = 0.5; Bl['ev'][:, 2:4] = M.EV['true_soft']; Bl['real'][:, 2:4] = 0; Bl['mD'][:, 2:4] = 0
Bl['real'][:, 4] = 1; Bl['mD'][:, 4] = 1; Bl['lD'][:, 4] = math.log(3.0); Bl['ev'][:, 4] = M.EV['true_hard']
gl, pl_ = A.age_posterior(mx, Bl, K=16); zl = A.z_quantiles(gl, pl_, [0.1, 0.5, 0.9])
check('a birth limit moves the posterior older', float(zl[1].min()) > 0.2, f'median z0 {float(zl[1].min()):.2f}')
tq = A.z_quantiles(torch.linspace(-1, 1, 3), torch.tensor([[0.25, 0.5, 0.25]]), [0.25, 0.5, 0.75]); check('grid quantiles against a hand case', float((tq[:, 0] - torch.tensor([-0.5, 0.0, 0.5])).abs().max()) < 1e-5, f'{tq[:, 0].tolist()}')
# 16. am. 16: the short-tree term is the mixture, and it is 0 when the tree is surely at breast height
Bm = batch(); um = model(); zm = torch.zeros(1, 6, M.DZ); smallm = torch.full((1, 6, M.T_MAX), math.log(0.2)); bigm = torch.full((1, 6, M.T_MAX), math.log(3.0))
with torch.no_grad():
    uu = um.bh_age(zm, Bm); want = (torch.log(torch.sigmoid(A.KAPPA_B * uu) + torch.sigmoid(-A.KAPPA_B * uu) * torch.sigmoid(-M.KAPPA * (bigm - Bm['lex'][None]))) * Bm['v'][None]).sum(-1)
    got = um.extra_lw(bigm, zm, Bm, None, None)
check('short-tree term = log[P(at BH) + P(not) P(below level)]', float((got - want).abs().max()) < 1e-4, f'{float((got - want).abs().max()):.1e}')
Bo = batch(); Bo['af_mu'][:] = math.log(80.0); Bo['af_sd'][:] = 1e-3; Bo['la'][:] = 0
with torch.no_grad(): check('surely past breast height: no short-tree penalty', float(um.extra_lw(bigm, zm, Bo, None, None).abs().max()) < 1e-3)
Bh = batch(); Bh['l0'][:] = 5.0
with torch.no_grad(): check('hardwoods need more years before tally size', float(um.extra_lw(bigm, zm, Bh, None, None).sum()) <= float(um.extra_lw(bigm, zm, Bm, None, None).sum()))
s10 = pd.DataFrame({'statecd': 1, 'unitcd': 1, 'countycd': 1, 'plot': 1, 'subp': 1, 'tree': [1, 1, 1], 'invyr': [2002, 2007, 2012], 'slot_kind': ['real', 'real', 'real'], 'hidden': [True, False, False], 'move_invyr': [2007, 2007, 2007]})
check('E1 mask reads visible records only', A.e1_mask(s10).tolist() == [True, False, False], str(A.e1_mask(s10).tolist()))
# 10. E1 mask: only slots before the move, only trees first recorded at or after it
s_ = pd.DataFrame({'statecd': 1, 'unitcd': 1, 'countycd': 1, 'plot': 1, 'subp': 1, 'tree': [1, 1, 1, 2, 2, 2, 3, 3], 'invyr': [2002, 2007, 2012, 2002, 2007, 2012, 2002, 2007],
                   'slot_kind': ['pre', 'pre', 'real', 'real', 'real', 'post', 'pre', 'real'], 'move_invyr': [2007, 2007, 2007, 2007, 2007, 2007, np.nan, np.nan]})
check('E1 mask: post-move debut only, slots before the move only', A.e1_mask(s_).tolist() == [True, False, False, False, False, False, False, False])
# 11. the joint model routes prediction through its own build
check('prediction uses the joint build', getattr(young, 'build', None) is not M.build and 'build' in A.NIMIWAE70.__dict__)
# 13. Schedule-Free AdamW
torch.manual_seed(1); p0 = torch.nn.Parameter(torch.tensor([3.0, -2.0])); sf = A.ScheduleFreeAdamW([p0], lr=0.1, beta=0.9, wd=0.0, warmup=1); zs = []; ws = []
for t_ in range(1, 6):
    sf.zero_grad(); (0.5 * (p0 ** 2).sum()).backward(); sf.step(); zs.append(sf.z[0].clone()); ws.append(0.1 ** 2)
xm = sum(w * z_ for w, z_ in zip(ws, zs)) / sum(ws)
check('x is the weighted average of the z iterates', float((sf.x[0] - xm).abs().max()) < 1e-5, f"{float((sf.x[0] - xm).abs().max()):.1e}")
check('parameters hold y = beta x + (1 - beta) z while training', float((p0.detach() - (0.9 * sf.x[0] + 0.1 * sf.z[0])).abs().max()) < 1e-6)
p1 = torch.nn.Parameter(torch.tensor([3.0, -2.0])); p2 = torch.nn.Parameter(torch.tensor([3.0, -2.0])); s1 = A.ScheduleFreeAdamW([p1], lr=0.1, beta=0.9, wd=0.0, warmup=1)
s2 = torch.optim.Adam([p2], lr=0.1, betas=(0.0, 0.999), eps=1e-8)
(0.5 * (p1 ** 2).sum()).backward(); s1.step(); (0.5 * (p2 ** 2).sum()).backward(); s2.step()
check('the first z step equals an Adam step without momentum', float((s1.z[0] - p2.detach()).abs().max()) < 1e-5)
torch.manual_seed(2); A_ = torch.randn(40, 8); w_ = torch.randn(8); y_ = A_ @ w_ + 0.1 * torch.randn(40); p3 = torch.nn.Parameter(torch.zeros(8)); s3 = A.ScheduleFreeAdamW([p3], lr=0.05, wd=0.0, warmup=20)
for t_ in range(1500):
    i_ = torch.randint(0, 40, (8,)); s3.zero_grad(); ((A_[i_] @ p3 - y_[i_]) ** 2).mean().backward(); s3.step()
s3.finish(); ls_ = torch.linalg.lstsq(A_, y_[:, None]).solution[:, 0]
check('it converges on noisy least squares; finish() leaves x', float((p3.detach() - ls_).norm()) < 0.1 and float((p3.detach() - s3.x[0]).abs().max()) == 0.0, f"{float((p3.detach() - ls_).norm()):.3f} from the solution")
# 14. on the real frame: the prior comes from the first visible live record, a hidden record never feeds it, and age steps match the dates
if len(sys.argv) > 1:
    sq0 = pd.read_parquet(f'{sys.argv[1]}/seq.parquet'); pk = sq0[sq0.split == 'val'].plot_key.drop_duplicates().sample(100, random_state=1); f_ = sq0[sq0.plot_key.isin(pk)].copy()
    Bf, vf, sf_ = A.build(f_); ti, po = sf_._tid.to_numpy(), sf_._pos.to_numpy()
    ok = (sf_.slot_kind == 'real') & sf_.af_mu.notna(); e_ = sf_[ok].assign(ti=ti[ok], po=po[ok]).sort_values(['ti', 'po']).groupby('ti').first()
    check('frame: la is the first slot with a prior', bool((Bf['la'][e_.index.to_numpy().copy()].numpy() == e_.po.to_numpy()).all()) and abs(float(Bf['af_mu'][e_.index.to_numpy().copy()].mean()) - e_.af_mu.mean()) < 1e-3, f'{len(e_):,} of {Bf["v"].shape[0]:,} trees')
    bs_ = M.bench_sizes(f_); Bh, _, sh = A.build(bs_, vf); ti, po = sh._tid.to_numpy(), sh._pos.to_numpy(); hi = sh.hidden.fillna(False).to_numpy(bool)
    check('frame: no hidden slot feeds the prior', int(((Bh['af_has'].numpy()[ti[hi]] > 0) & (Bh['la'].numpy()[ti[hi]] == po[hi])).sum()) == 0, f'{int(hi.sum()):,} hidden slots')
    check('frame: no visible record falls before birth under any draw', bool((Bf['la'] == Bf['f']).all()), f'{int((Bf["la"] == Bf["f"]).sum()):,} trees')
    fz = f_.copy(); fz.loc[fz.index[::3], 'af_mu'] = np.nan; Bz, _, _ = A.build(fz, vf); nz = Bz['af_has'] == 0
    check('frame: a tree with no prior anchors at its first visible record', bool((Bz['la'][nz] == Bz['f'][nz]).all()), f'{int(nz.sum()):,} trees without a prior')
    check('frame: l0 is 2 for softwoods and 5 for hardwoods', set(Bf['l0'].unique().tolist()) <= {2.0, 5.0} and bool((Bf['l0'] == 2.0).any()))
    zf = torch.zeros(1, Bf['v'].shape[0], M.DZ)
    mf = A.make_model(vf); af = mf.ages(zf, Bf).detach()[0].numpy(); ti, po = sf_._tid.to_numpy(), sf_._pos.to_numpy(); sa = sf_.assign(a=af[ti, po], yr=(pd.to_datetime(sf_.measdate) - pd.Timestamp('2000-01-01')) / pd.Timedelta(days=365.25))
    r_ = sa.groupby(M.K_).apply(lambda g: float(((g.a - g.a.iloc[0]) - (g.yr - g.yr.iloc[0])).abs().max()), include_groups=False).max()
    check('frame: age steps equal elapsed time from the dates', r_ < 0.01, f'max error {r_:.4f} yr')
p4 = torch.nn.Parameter(torch.tensor([3.0, -2.0])); s4 = A.ScheduleFreeAdamW([p4], lr=0.1, wd=0.0, warmup=1)
for t_ in range(5): s4.zero_grad(); (0.5 * (p4 ** 2).sum()).backward(); s4.step()
y4 = p4.detach().clone()
with s4.averaged(): inside = float((p4.detach() - s4.x[0]).abs().max())
check('scoring mid-run reads x and gives y back', inside == 0.0 and float((p4.detach() - y4).abs().max()) == 0.0)
print('checks:', sum(o for _, o in RES), 'of', len(RES)); sys.exit(0 if all(o for _, o in RES) else 1)
