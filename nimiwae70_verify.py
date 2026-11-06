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
# 9. D72: the cut-off term is log(1 - p_s g sigma(x)); age off it is the D55 term; no other age factor remains
tall = model(); B9 = batch(); B9['af_mu'][:] = math.log(3.0); B9['af_sd'][:] = 0.5
zz9 = torch.randn(2, 6, M.DZ); xt9 = torch.randn(2, 6, M.T_MAX) * 3; ps9 = torch.rand(6, M.T_MAX)[None].expand(2, -1, -1).clone(); ps9[..., 0] = 1.0
with torch.no_grad():
    g9 = torch.sigmoid(A.KAPPA_B * (tall.ages(zz9, B9) - B9['l0'][None, :, None])); want9 = torch.log(1 - ps9 * g9 * torch.sigmoid(xt9))
    check('cut-off term = log(1 - p_s g sigma(x))', float((tall.cutoff_term(xt9, ps9, zz9, B9) - want9).abs().max()) < 1e-4, f'{float((tall.cutoff_term(xt9, ps9, zz9, B9) - want9).abs().max()):.1e}')
    b09 = model(age=False); base9 = M.NIMIWAE56.cutoff_term(b09, xt9, ps9, zz9, B9)
    check('age off: the cut-off term is the D55 one', float((b09.cutoff_term(xt9, ps9, zz9, B9) - base9).abs().max()) == 0.0)
    check('surely past breast height: the gated term equals the D55 one', float((tall.cutoff_term(xt9, ps9, torch.full_like(zz9, 6.0), B9) - base9).abs().max()) < 1e-3)
    check('before breast height a sampled spot without a record costs nothing', float(tall.cutoff_term(xt9.abs() + 5, ps9, torch.full_like(zz9, -6.0), {**B9, 'af_mu': torch.full((6,), math.log(0.5)), 'l0': torch.full((6,), 5.0)})[..., :4].abs().max()) < 1e-3)   # slots 0-3 lie before breast height for this tree
    check('no separate age factor in the weights', tall.extra_lw(torch.zeros(1, 6, M.T_MAX), zz9[:1], B9, None, None) == 0.0)
zg = torch.zeros(1, 6, M.DZ, requires_grad=True); tall.cutoff_term(torch.full((1, 6, M.T_MAX), 2.0), torch.ones(1, 6, M.T_MAX), zg, B9).sum().backward()
check('the gated cut-off term reaches z0', float(zg.grad[..., 0].abs().sum()) > 0)
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
# 16. D73 flow q: the spline and the flow against autograd; at initialisation the flow q is the Gaussian q; DReG with detached flow weights; z0 stays separate
import nimiwae_flow as NF
torch.manual_seed(11); xs = torch.linspace(-6.5, 6.5, 401, dtype=torch.float64).repeat(3, 1).requires_grad_(True); uw, uh, ud = ((torch.randn(3, 1, n_, dtype=torch.float64) * 2).expand(-1, 401, -1) for n_ in (8, 8, 7))   # one spline per row; float64: a formula check (float32 loses 1e-3 where the slope is 1e-4, the flow checks below run float32)
ys, ldf = NF.rq_spline(xs, uw, uh, ud); dy = torch.autograd.grad(ys.sum(), xs)[0]; xb, ldi = NF.rq_spline(ys.detach(), uw, uh, ud, inverse=True)
check('spline: log dy/dx = autograd derivative', float((ldf - torch.log(dy)).abs().max()) < 1e-9, f'{float((ldf - torch.log(dy)).abs().max()):.1e}')
check('spline: inverse(forward(x)) = x and the log-derivatives cancel', float((xb - xs).abs().max()) < 1e-9 and float((ldf + ldi).abs().max()) < 1e-9, f'{float((xb - xs).abs().max()):.1e} {float((ldf + ldi).abs().max()):.1e}')
check('spline: monotone, identity outside [-B, B]', bool((ys[:, 1:] > ys[:, :-1]).all()) and float((ys - xs)[xs.abs() >= 5].abs().max()) == 0.0)
check('spline: zero parameters are the identity', float((NF.rq_spline(xs, torch.zeros_like(uw), torch.zeros_like(uh), torch.full_like(ud, NF.D_ID))[0] - xs).abs().max()) < 1e-9)
fl = NF.CondFlow(7, 5, layers=4, hidden=16, bins=6)
with torch.no_grad():
    for L_ in fl.layers: L_.net[-1].weight.normal_(0, 0.5); L_.net[-1].bias.normal_(0, 0.5)
e7, c5 = torch.randn(3, 7) * 2, torch.randn(3, 5); z7, ld7 = fl(e7, c5); e7b, ldi7 = fl(z7, c5, inverse=True)
J = [torch.autograd.functional.jacobian(lambda x: fl(x[None], c5[i:i + 1])[0][0], e7[i]) for i in range(3)]
ldJ = torch.stack([torch.linalg.slogdet(j)[1] for j in J])
check('flow: log |dT/de| = log |det| of the autograd Jacobian', float((ld7 - ldJ).abs().max()) < 1e-3, f'{float((ld7 - ldJ).abs().max()):.1e}')
check('flow: inverse(T(e)) = e, log-determinants cancel', float((e7b - e7).abs().max()) < 1e-3 and float((ld7 + ldi7).abs().max()) < 1e-3)
def model_q(q, age=True, perturb=False):
    torch.manual_seed(0); m_ = A.NIMIWAE70(3, 3, 3, 3, 3, age=age, q=q)
    if perturb:
        with torch.no_grad():
            for L_ in m_.flow.layers: L_.net[-1].weight.normal_(0, 0.3); L_.net[-1].bias.normal_(0, 0.3)
    return m_
mg, mfl = model_q('gauss'), model_q('flow'); B16 = batch()
with torch.no_grad():
    torch.manual_seed(7); og = mg.joint(B16, 8); torch.manual_seed(7); of = mfl.joint(B16, 8)
check('flow at initialisation = the Gaussian q: same draws, same weights', float((og['z'] - of['z']).abs().max()) < 1e-4 and float((og['lw'] - of['lw']).abs().max()) < 1e-3, f"{float((og['lw'] - of['lw']).abs().max()):.1e}")
mfp = model_q('flow', perturb=True)
with torch.no_grad():
    torch.manual_seed(7); op = mfp.joint(B16, 8, dreg=True); mq16, lq16 = mfp.q(B16)
    torch.manual_seed(7); zs16, lqs16 = mfp.sample_z(mq16, lq16, 8)
    check('flow: z0 is the Gaussian draw, untouched', float((op['z'][..., 0] - og['z'][..., 0]).abs().max()) < 1e-5)
    check('flow: the other dimensions move', float((op['z'][..., 1:] - og['z'][..., 1:]).abs().max()) > 1e-2)
    check('flow: density of own draws (forward) = density at given z (inverse)', float((lqs16 - mfp.log_qz(zs16, mq16, lq16)).abs().max()) < 1e-3, f'{float((lqs16 - mfp.log_qz(zs16, mq16, lq16)).abs().max()):.1e}')
    check('flow: the DReG weights equal the weights in value', float((op['lw'] - op['lw_dreg']).abs().max()) < 1e-3)
    torch.manual_seed(9); oz = mfp.joint(B16, 8, z=op['z'])
    check('flow: joint scores given draws', float((oz['lw'] - op['lw']).abs().max()) < 1e-3)
fid = {id(p) for p in mfp.flow.parameters()}; check('flow weights are encoder parameters', fid <= {id(p) for p in mfp.enc_params()})
mfp.zero_grad(); torch.manual_seed(7); od = mfp.joint(B16, 8, dreg=True); wt = torch.softmax(od['lw'], 0).detach(); (-(wt ** 2 * od['lw_dreg']).sum(0).mean()).backward()
check('flow: the DReG surrogate trains the flow (through z)', sum(float(p.grad.abs().sum()) for p in mfp.flow.parameters() if p.grad is not None) > 0)
mq16, lq16 = mfp.q(B16); check('flow: the DReG density holds every q parameter fixed (no graph without z)', not mfp.log_qz(od['z'].detach(), mq16, lq16, detach=True).requires_grad and mfp.log_qz(od['z'].detach(), mq16, lq16).requires_grad)
mfx = model_q('flow', perturb=True); mfx.eval(); Bx16 = batch(); Bx16['la'][:] = 0; Bx16['f'][:] = 0; Bx16['af_mu'][:] = math.log(60.0); Bx16['af_sd'][:] = 0.3
gq, pq = A.age_posterior(mfx, Bx16, K=16); zq16 = A.z_quantiles(gq, pq, [0.1, 0.5, 0.9])
check('flow: no birth limit, the grid posterior is still the prior (q factorises)', float(zq16[1].abs().max()) < 0.02 and float((zq16[2] - 1.2816).abs().max()) < 0.03, f'{[round(float(zq16[i, 0]), 3) for i in range(3)]}')
check('flow, age off: every dimension flows', model_q('flow', age=False).f0 == 0 and model_q('flow').f0 == 1)
voc = {k: [0, 1, 2] for k in ('spgrpcd', 'epa_l3', 'major_spgrpcd', 'spcd')}; voc['fortypcd'] = [0, 1]
ms_ = A.make_model(voc, q='flow', flow=dict(layers=4)); mr_ = A.model_from_cfg(voc, ms_.cfg); mr_.load_state_dict(ms_.state_dict())
check('flow: a checkpoint rebuilds from its cfg', mr_.qfam == 'flow' and len(mr_.flow.layers) == 4 and A.model_from_cfg(voc, dict(age=True)).qfam == 'gauss')
# 17. D74: rate and features from visible records, the coupling formula, the anchor term, the Student-t height model, calibration recovers a planted delta
B17 = batch(); n17, T17 = B17['v'].shape
sq17 = pd.DataFrame(dict(slot_kind=np.where(np.tile(np.arange(T17), n17) < 5, 'real', 'pre'), anc_agedia=np.nan, htcd=np.nan))
ti17, po17 = np.repeat(np.arange(n17), T17), np.tile(np.arange(T17), n17)
sq17.loc[(ti17 == 0) & (po17 == 3), 'anc_agedia'] = 40.0; sq17.loc[(po17 == 2), 'htcd'] = 2; sq17.loc[(ti17 == 1) & (po17 == 3), 'htcd'] = 4
B17['mcc'][:, 3] = 1; B17['cc'][:, 3] = 3; B17['mH'][:, 2:4] = 1
A.d74_features(B17, sq17, ti17, po17)
t17 = B17['cov'][..., A.ITYR] * 20; want_r = (3.0 - 2.0) / float(t17[0, 3] - t17[0, 2])
check('D74 rate = DIA change / years between the first and last measured visible records', abs(float(B17['lrate'][0]) - math.log(want_r)) < 1e-4 and float(B17['has_rate'][0]) == 1.0, f'{math.exp(float(B17["lrate"][0])):.4f} vs {want_r:.4f} in/yr')
check('D74 DIA and crown class read at the prior slot la', abs(float(B17['dia_la'][0]) - 3.0) < 1e-4 and int(B17['cc_la'][0]) == 3)
check('D74 anchor at its slot only', float(B17['anc_m'].sum()) == 1.0 and float(B17['anc_m'][0, 3]) == 1.0 and abs(float(B17['anc_y'][0, 3]) - math.log(40.0)) < 1e-5)
check('D74 HTCD 2 / 3 -> visual class, HTCD 4 height masked', int(B17['htcls'][:, 2].min()) == 1 and int(B17['htcls'][:, 3].max()) == 0 and float(B17['mH'][1, 3]) == 0.0 and float(B17['mH'][0, 3]) == 1.0)
h17 = B17.copy(); h17['mD'] = B17['mD'].clone(); h17['mD'][:, 3] = 0; sq_h = sq17.copy(); A.d74_features(h17, sq_h, ti17, po17)
check('D74 a single measured record gives no rate', float(h17['has_rate'].max()) == 0.0)
cp17 = dict(delta={'1': -0.2, '2': -0.2, '3': -0.2, '4': -0.12, '5': -0.09}, m={str(k): -1.5 for k in range(1, 6)}, w={'1': 0.9, '2': 0.9, '3': 0.9, '4': 1.0, '5': 1.0})
B17['af_has'][:] = 1; B17['dia_la'][:] = 8.0; B17['dia_la'][1] = 4.0; B17['cc_la'][2] = 5; A.apply_coupling(B17, cp17)   # trees 0, 2.. eligible at 8 in; tree 1 below 5 in
want0 = -0.2 * (float(B17['lrate'][0]) + 1.5); want2 = -0.09 * (float(B17['lrate'][2]) + 1.5)
check('D74 shift = delta_c (lrate - m_c); none below 5 in; class 5 uses its own delta', abs(float(B17['af_shift'][0]) - want0) < 1e-5 and float(B17['af_shift'][1]) == 0.0 and abs(float(B17['af_shift'][2]) - want2) < 1e-5,
      f'{float(B17["af_shift"][0]):.4f} {float(B17["af_shift"][2]):.4f}')
check('D74 width factor on coupled trees only', abs(float(B17['af_sdf'][0]) - 0.9) < 1e-6 and float(B17['af_sdf'][1]) == 1.0 and float(B17['af_sdf'][2]) == 1.0)
m17 = model(); z17 = torch.randn(3, n17, M.DZ)
with torch.no_grad():
    a17 = m17.ages(z17, B17); la17 = B17['la']; al17 = a17.gather(2, la17[None, :, None].expand(3, -1, 1))[..., 0]
    want_al = torch.exp(B17['af_mu'] + B17['af_shift'] + B17['af_sd'] * B17['af_sdf'] * z17[..., 0])
check('D74 log a_l = af_mu + shift + af_sd w z0', float((al17 - want_al).abs().max()) < 1e-3)
with torch.no_grad():
    ex = m17.extra_lw(None, z17, B17, None, None); bh = (a17 - B17['l0'][None, :, None]).clamp(min=0.1)
    want_ex = (-0.5 * ((math.log(40.0) - torch.log(bh[:, 0, 3])) / A.S_ANCHOR) ** 2 - math.log(A.S_ANCHOR) - 0.5 * math.log(2 * math.pi))
check('D74 anchor term = log N(log AGEDIA; log(a_t - l0), S_ANCHOR) at the anchor, 0 elsewhere', float((ex[:, 0] - want_ex).abs().max()) < 1e-4 and float(ex[:, 1:].abs().max()) == 0.0)
mt = A.NIMIWAE70(3, 3, 3, 3, 3, age=True, height='t')
with torch.no_grad():
    mt.lsHc.copy_(torch.tensor([-2.3, -1.6])); lHp = torch.randn(2, n17, T17) * 0.2 + 2.0
    got = mt.height_ll(lHp, B17); sc = torch.exp(mt.lsHc[B17['htcls']])[None]
    want = torch.distributions.StudentT(A.T_NU, lHp, sc).log_prob(B17['lH'][None].expand_as(lHp))
check('D74 height: Student-t log density by method, against torch.distributions', float((got - want).abs().max()) < 1e-4, f'{float((got - want).abs().max()):.1e}')
check('D74 height off: the Gaussian base term', float((model().height_ll(lHp, B17) - M.NIMIWAE56.height_ll(model(), lHp, B17)).abs().max()) == 0.0)
# calibration recovers a planted delta (synthetic anchors)
g17 = torch.Generator().manual_seed(5); N = 4000
Bc = dict(af_has=torch.ones(N), has_rate=torch.ones(N), dia_la=torch.full((N,), 8.0), cc_la=torch.randint(1, 4, (N,), generator=g17), lrate=torch.randn(N, generator=g17) * 0.6 - 1.6,
          af_mu=torch.randn(N, generator=g17) * 0.3 + math.log(40), l0=torch.full((N,), 2.0), la=torch.zeros(N, dtype=torch.long), cov=torch.zeros(N, M.T_MAX, M.NCOV),
          anc_m=torch.zeros(N, M.T_MAX), anc_y=torch.zeros(N, M.T_MAX))
Bc['cov'][..., A.ITYR] = (torch.arange(M.T_MAX) * 5.0 / 20.0)[None]
true_tot_la = torch.exp(Bc['af_mu'] - 0.25 * (Bc['lrate'] + 1.6) + 0.15 * torch.randn(N, generator=g17))
Bc['anc_m'][:, 2] = 1; Bc['anc_y'][:, 2] = torch.log(true_tot_la + 10.0 - 2.0)                          # anchor 10 yr after la, as breast-height age
cpc = A.calibrate_coupling(Bc, log=lambda *_: None)
check('D74 calibration recovers a planted delta (-0.25)', abs(cpc['delta']['3'] + 0.25) < 0.02 and abs(cpc['delta']['4'] - cpc['delta']['3'] * 0.61) < 1e-9 and cpc['w']['5'] == 1.0, f"delta {cpc['delta']['3']:+.3f}, w {cpc['w']['3']:.3f}")
# the exact grid still returns the prior on z0 when nothing limits age, with a coupling shift present
mx2 = model(); mx2.eval(); Bx2 = batch(); Bx2['la'][:] = 0; Bx2['f'][:] = 0; Bx2['af_mu'][:] = math.log(60.0); Bx2['af_sd'][:] = 0.3
Bx2['af_shift'] = torch.full((6,), 0.4); Bx2['af_sdf'] = torch.full((6,), 0.8)
gq2, pq2 = A.age_posterior(mx2, Bx2, K=16); zq2 = A.z_quantiles(gq2, pq2, [0.1, 0.5, 0.9])
check('D74 grid with a coupling shift: z0 posterior is still N(0, 1) without limits', float(zq2[1].abs().max()) < 0.02 and float((zq2[2] - 1.2816).abs().max()) < 0.03)
# 18. exact-grid age in the products: impute(age='grid') and sample_lineages (grid by default for age models)
mg18 = model(); mg18.eval(); B18 = batch(); B18['la'][:] = 4; B18['f'][:] = 4; B18['af_mu'][:] = math.log(4.0); B18['af_sd'][:] = 0.5
B18['ev'][:, 2:4] = M.EV['true_soft']; B18['real'][:, 2:4] = 0; B18['mD'][:, 2:4] = 0; B18['real'][:, 4] = 1; B18['mD'][:, 4] = 1; B18['lD'][:, 4] = math.log(3.0); B18['ev'][:, 4] = M.EV['true_hard']
torch.manual_seed(21); pg = A.posterior_grid(mg18, B18, K=8, G=21, zmax=3.6)   # the algebra on one grid; the product default (A.PRODUCT_GRID) is wider
check('grid posterior: weights over (draw, end, cause) sum to 1 per tree', float((pg['W'].sum((0, 2, 3)) - 1).abs().max()) < 1e-4)
torch.manual_seed(21)                                                                     # brute force on the same draws: explicit loops over grid age, draw and end
with A.AgeGrid(mg18, B18, K=8, G=21, zmax=3.6) as ag:
    L = ag.log_weights(); P = torch.softmax(L.reshape(-1, 6), 0).reshape(L.shape); pE_bf = torch.zeros(6, M.T_MAX); nv18 = B18['v'].sum(1).long()
    for gi in range(21):
        o18 = ag.joint(gi); post = torch.exp(o18['lpost']).sum(-1)
        for kk in range(8):
            for e in range(M.T_MAX + 1):
                for t in range(M.T_MAX):
                    alive = ((t < e) | (e == M.T_MAX)) & (t < nv18)
                    pE_bf[:, t] += P[gi, kk] * post[kk, :, e] * alive.float() * o18['p1'][kk, :, t]
check('grid posterior: P(exists) equals the brute-force sum over grid age, draw and end', float((pg['WaP1'].sum(0) - pE_bf).abs().max()) < 1e-5, f"{float((pg['WaP1'].sum(0) - pE_bf).abs().max()):.1e}")
torch.manual_seed(22); gq, pq18 = A.age_posterior(mg18, B18, K=8, G=21, zmax=3.6)
torch.manual_seed(22); ld18 = A.lineage_draws_grid(mg18, B18, K=8, n_draws=4000, G=21, zmax=3.6)
zq18 = A.z_quantiles(gq, pq18, [0.5]); zd = ld18['g'].median(1).values
check('grid lineage draws: drawn age latent follows the grid posterior (median)', float((zd - zq18[0]).abs().max()) < 0.12, f'{[round(float(x), 2) for x in zd[:3]]} vs {[round(float(x), 2) for x in zq18[0, :3]]}')
torch.manual_seed(23); L18 = M.sample_lineages(mg18, B18, n_draws=50, K=8)
bh18 = L18['age'] - B18['l0'].numpy()[:, None, None]
check('grid lineages: no tree exists before breast height in its own draw', int(np.nansum((L18['exists'] > 0) & (bh18 <= 0))) == 0)
t18 = (B18['cov'][..., A.ITYR] * 20).numpy()[:, None, :5]; da = L18['age'][..., :5] - L18['age'][..., :1]
check('grid lineages: each draw\'s age steps equal elapsed years', float(np.abs(da - (t18 - t18[..., :1])).max()) < 1e-3)
Pg = M.impute(young, B8, K=16, nsamp=20, age='grid'); limg = torch.sigmoid(A.KAPPA_B * (aY[:4].detach() - B8['l0'][0]))
check('grid impute: P(exists) never exceeds the birth step before birth', bool((Pg['pE'][:, :4] <= limg + 1e-5).all()))
check('product grid: same spacing as the scored grid, wider', abs(2 * A.PRODUCT_GRID['zmax'] / (A.PRODUCT_GRID['G'] - 1) - 2 * A.SCORE_GRID['zmax'] / (A.SCORE_GRID['G'] - 1)) < 1e-9 and A.PRODUCT_GRID['zmax'] > A.SCORE_GRID['zmax'])
torch.manual_seed(24); Pd = M.impute(mg18, B18, K=8, nsamp=20); torch.manual_seed(24); Pd2 = M.impute(mg18, B18, K=8, nsamp=20, age='draws')
check("impute: the default is the importance-draw path the CV scored", float((Pd['pE'] - Pd2['pE']).abs().max()) == 0.0)
# 19. D76: crew misses of not-yet-listed trees: P(no record) = 1 - p_s (1 - eps) g sigma(x) at pre-debut subplot-level slots
B19 = batch(); B19['f'][:] = 2; B19['llim'][:, :2] = math.log(4.95); B19['lex'][:] = math.log(0.95)   # slots 0, 1: before the debut, subplot level
A.d74_features(B19, sq17, ti17, po17)
check('D76 off: miss_keep is 1 everywhere', float((B19['miss_keep'] - 1).abs().max()) == 0.0)
m19 = model(); B19n = {k: v for k, v in B19.items() if k != 'miss_keep'}
with torch.no_grad():
    torch.manual_seed(31); l_a = m19.joint(B19, 4)['lw']; torch.manual_seed(31); l_b = m19.joint(B19n, 4)['lw']
check('D76 off: the weights are bit-identical to the term without the factor', float((l_a - l_b).abs().max()) == 0.0)
A.apply_miss(B19, 0.02)
want_mask = torch.zeros_like(B19['v'], dtype=torch.bool); want_mask[:, :2] = True
check('D76: 1 - eps exactly at pre-debut subplot-level slots, 1 elsewhere', bool(((B19['miss_keep'] < 1) == want_mask).all()) and abs(float(B19['miss_keep'][0, 0]) - 0.98) < 1e-7)
with torch.no_grad():
    xt19 = torch.randn(2, 6, M.T_MAX) * 3; ps19 = (B19['psamp'] * B19['miss_keep'])[None].expand(2, -1, -1); zz19 = torch.randn(2, 6, M.DZ)
    g19 = torch.sigmoid(A.KAPPA_B * (m19.ages(zz19, B19) - B19['l0'][None, :, None]))
    want19 = torch.log(1 - B19['psamp'][None] * (1 - 0.02 * want_mask[None].float()) * g19 * torch.sigmoid(xt19))
check('D76: the term equals log(1 - p_s (1 - eps) g sigma(x))', float((m19.cutoff_term(xt19, ps19, zz19, B19) - want19).abs().max()) < 1e-4)
with torch.no_grad():
    torch.manual_seed(31); l_c = m19.joint(B19, 4)['lw']
check('D76 on: the weights move', float((l_c - l_a).abs().max()) > 0)
# calibration recovers a planted miss rate
g19r = torch.Generator().manual_seed(9); N = 6000; Bc = {}
Bc['v'] = torch.zeros(N, M.T_MAX); Bc['v'][:, :3] = 1
Bc['cov'] = torch.zeros(N, M.T_MAX, M.NCOV); Bc['cov'][..., A.ITYR] = (torch.arange(M.T_MAX) * 5.0 / 20.0)[None]
Bc['lD'] = torch.zeros(N, M.T_MAX); Bc['mD'] = torch.zeros(N, M.T_MAX); Bc['real'] = torch.zeros(N, M.T_MAX)
Bc['sampled'] = torch.ones(N, M.T_MAX); Bc['psamp'] = torch.ones(N, M.T_MAX); Bc['mode'] = torch.zeros(N, M.T_MAX, dtype=torch.long)
Bc['llim'] = torch.full((N, M.T_MAX), math.log(4.95)); Bc['lex'] = torch.full((N, M.T_MAX), math.log(0.95)); Bc['f'] = torch.zeros(N, dtype=torch.long)
half = N // 2                                                                               # growth trees: measured at slots 0 and 1, 6 in, 0.1-0.3 in / yr
d0 = torch.full((half,), 6.0); gr = 0.1 + 0.2 * torch.rand(half, generator=g19r)
Bc['lD'][:half, 0] = torch.log(d0); Bc['lD'][:half, 1] = torch.log(d0 + 5.0 * gr); Bc['mD'][:half, :2] = 1; Bc['real'][:half, :2] = 1
nd = N - half; miss = torch.rand(nd, generator=g19r) < 0.03                                # debut trees: first record at slot 1 after an unrecorded sampled slot 0
rate = torch.where(miss, torch.full((nd,), 1.5), torch.full((nd,), 0.05))
Bc['f'][half:] = 1; Bc['lD'][half:, 1] = torch.log(5.0 + 5.0 * rate); Bc['mD'][half:, 1] = 1; Bc['real'][half:, 1] = 1
eps19 = A.calibrate_miss(Bc, log=lambda *_: None)
check('D76: calibration recovers a planted miss rate (3 %)', abs(eps19 - float(miss.float().mean())) < 1e-9, f'eps {eps19:.4f} vs planted {float(miss.float().mean()):.4f}')
# debuts outside the calibration population change nothing: coded misses (missed mode), partly sampled spots, microplot level, a record at the earlier visit
Bx = {k: torch.cat([v, v[half:half + 400]]) if v.dim() and v.shape[0] == N else v for k, v in Bc.items()}; x0 = N
Bx['lD'][x0:, 1] = math.log(5.0 + 5.0 * 1.5)                                                # every extra debut grew implausibly fast
Bx['mode'][x0:x0 + 100, 0] = M.MODE['missed']; Bx['psamp'][x0 + 100:x0 + 200, 0] = 0.5; Bx['llim'][x0 + 200:x0 + 300, 0] = Bx['lex'][x0 + 200:x0 + 300, 0]
Bx['real'][x0 + 300:, 0] = 1
check('D76: calibration ignores coded, partly sampled, microplot-level and previously recorded debuts', abs(A.calibrate_miss(Bx, log=lambda *_: None) - eps19) < 1e-12)
B19m = {k: v.clone() for k, v in B19.items()}; B19m['llim'][0, 0] = B19m['lex'][0, 0]; A.apply_miss(B19m, 0.02)
check('D76: no factor at a pre-debut microplot-level slot', float(B19m['miss_keep'][0, 0]) == 1.0 and float(B19m['miss_keep'][0, 1]) < 1.0)
# 20. D78 sapling coupling (off by default): delta_s (clamp(lrate) - m_s) on saplings with a prior and a rate, nothing elsewhere
B20 = dict(af_has=torch.ones(4), has_rate=torch.tensor([1.0, 1.0, 0.0, 1.0]), dia_la=torch.tensor([2.0, 4.9, 3.0, 6.0]), cc_la=torch.tensor([3, 3, 3, 3]),
           lrate=torch.log(torch.tensor([0.01, 0.5, 0.05, 0.05])) * torch.tensor([1.0, 1.0, 0.0, 1.0]), af_mu=torch.zeros(4), af_sd=torch.ones(4))
z20 = dict(delta={str(c): 0.0 for c in range(1, 6)}, w={str(c): 1.0 for c in range(1, 6)}, m={str(c): 0.0 for c in range(1, 6)})
A.apply_coupling(B20, z20); off20 = B20['af_shift'].clone()
A.apply_coupling(B20, z20 | dict(sapling=dict(delta=-0.2, lo=math.log(0.02), hi=math.log(0.15), m=-3.0)))
want20 = torch.tensor([-0.2 * (math.log(0.02) + 3.0), -0.2 * (math.log(0.15) + 3.0), 0.0, 0.0])
check('D78 sapling coupling: off by default, clamped formula on eligible saplings only', float(off20.abs().max()) == 0.0 and float((B20['af_shift'] - want20).abs().max()) < 1e-6)
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
    if 'l0' in sq0 and os.path.exists(f'{sys.argv[1]}/site_refs.parquet'):              # D78: one l0 per tree, also in the G1 truth
        sr = pd.read_parquet(f'{sys.argv[1]}/site_refs.parquet').merge(sq0[M.K_ + ['invyr', 'l0']].drop_duplicates(M.K_ + ['invyr']), on=M.K_ + ['invyr'])
        check('frame: G1 truth = AGEDIA + the per-tree l0 on every site ref (D78)', bool(np.allclose(sr.truth - sr.a_obs, sr.l0, atol=1e-6)), f'{len(sr):,} refs')
    if 'af_model' in f_:                                                                 # D79: the known-exception flag = planted-model prior at DIA >= 15 in at the prior's slot
        fr_ = sf_[(sf_.slot_kind == 'real') & sf_.af_mu.notna()].sort_values(['_tid', '_pos']).groupby('_tid').first()
        want_kb = ((fr_.af_model == 'P') & (fr_.dia >= A.KNOWN_BIAS_DIA)).reindex(range(Bf['v'].shape[0]), fill_value=False).to_numpy()
        check('frame: age_known_bias is the planted-model prior at DIA >= 15 in (D79)', (Bf['age_known_bias'].numpy() > 0).tolist() == want_kb.tolist(), f'{int(want_kb.sum())} flagged')
    if 'l0' in f_:                                                                       # D78 frames: the per-tree column of nimiwae_l0, unchanged by build
        want_l0 = f_.drop_duplicates(M.K_).set_index(M.K_).l0; got_l0 = pd.Series(Bf['l0'].numpy(), index=pd.MultiIndex.from_frame(sf_.drop_duplicates('_tid').sort_values('_tid')[M.K_]))
        check('frame: l0 is the frame\'s per-tree l0 (D78)', bool(np.allclose(got_l0.to_numpy(), want_l0.loc[got_l0.index].to_numpy(), atol=1e-5)), f'values {sorted(set(np.round(Bf["l0"].numpy(), 2).tolist()))[:8]}')
    else:
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
