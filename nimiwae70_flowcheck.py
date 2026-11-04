"""D73 check without training: does a trained flow proposal differ from the identity, and what does it buy?
  python -I nimiwae70_flowcheck.py --data data --run out/cv74flow/f0_age1 --name model_e36.pt --fold 0 --device cuda
Same trained model (prior, decoder, encoder), same held trees, same draws: the IWAE bound and ESS with the flow proposal against the
encoder's Gaussian alone (flow switched off). The flow's own movement is reported as |T(e) - e| in units of q's standard deviation and as
log |dT/de|; both near 0 mean the flow stayed close to the identity. The Gaussian here was trained together with the flow, so its numbers
are a lower bound for a Gaussian trained alone."""
import sys, os, argparse, json
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import numpy as np, pandas as pd, torch
import nimiwae56 as M, nimiwae70 as A, nimiwae70_cv as C, nimiwae70_score as S


@torch.no_grad()
def scores(m, B, K, dev, bs=512, seed=0):
    """mean IWAE bound per tree and median ESS for the model's current proposal, with fixed draws"""
    tot, n, ess = 0.0, 0, []
    for i in range(0, B['v'].shape[0], bs):
        torch.manual_seed(seed + i); b = {k: v.to(dev) for k, v in M.sub(B, slice(i, i + bs)).items()}; lw = m.joint(b, K)['lw']
        tot += (torch.logsumexp(lw, 0) - np.log(K)).sum().item(); n += lw.shape[1]; ess.append((1 / (torch.softmax(lw, 0) ** 2).sum(0)).cpu())
    return tot / n, float(torch.cat(ess).median())


@torch.no_grad()
def movement(m, B, dev, K=32, bs=512):
    """|T(e) - e| / sigma_q per transformed coordinate and log |dT/de| per draw"""
    d, ld, inside, rt = [], [], [], []
    for i in range(0, B['v'].shape[0], bs):
        b = {k: v.to(dev) for k, v in M.sub(B, slice(i, i + bs)).items()}; mq, lq = M.NIMIWAE56.q(m, b)
        e = mq + torch.exp(0.5 * lq) * torch.randn(K, *mq.shape, device=dev); er = e[..., m.f0:]
        zr, l = m.flow(er, m._hq)
        eb, _ = m.flow(zr, m._hq, inverse=True); rt.append((eb - er).abs().flatten().cpu())      # forward then inverse: the DReG density's inverse must invert
        d.append(((zr - er).abs() / torch.exp(0.5 * lq[..., m.f0:])[None]).flatten().cpu()); ld.append(l.flatten().cpu())
        inside.append(((er.abs() < m.flow.layers[0].B).float().mean()).cpu()[None])
    d, ld, rt = torch.cat(d), torch.cat(ld), torch.cat(rt)
    q = lambda x, p: float(torch.quantile(x[torch.randperm(len(x))[:200000]], p))
    return dict(shift_sd_median=q(d, 0.5), shift_sd_p90=q(d, 0.9), shift_sd_p99=q(d, 0.99), logdet_mean=float(ld.mean()), logdet_sd=float(ld.std()),
                logdet_p01=q(ld, 0.01), logdet_p99=q(ld, 0.99), share_inside_spline_range=float(torch.cat(inside).mean()),
                roundtrip_max=float(rt.max()), roundtrip_p99=q(rt, 0.99))


def main():
    ap = argparse.ArgumentParser(); ap.add_argument('--data', required=True); ap.add_argument('--run', required=True); ap.add_argument('--name', default='model_e36.pt')
    ap.add_argument('--fold', type=int, default=0); ap.add_argument('--trees', type=int, default=4000); ap.add_argument('--device', default='cpu'); ap.add_argument('--threads', type=int, default=8)
    a = ap.parse_args(); torch.set_num_threads(a.threads)
    m, ck = S.load(a.run, a.name); m.to(a.device); m.eval()
    assert m.qfam == 'flow', 'not a flow checkpoint'
    k = M.K_ + ['invyr']
    tr = pd.read_parquet(f'{a.data}/seq.parquet', filters=[('split', '==', 'train')])
    if m.height == 't': tr = tr.merge(pd.read_parquet(f'{a.data}/ht_refs.parquet'), on=k, how='left')     # the D74 inputs the model was trained with
    held_k, _, _ = C.split_plots(sorted(tr.plot_key.unique()), a.fold); held = tr[tr.plot_key.isin(set(held_k))]
    trees = held[M.K_].drop_duplicates().sample(n=min(a.trees, held.groupby(M.K_).ngroups), random_state=0); held = held.merge(trees, on=M.K_)
    B, _, _ = A.build(held, m.vocab)
    R = dict(checkpoint=f'{a.run}/{a.name}', trees=int(B['v'].shape[0]), cfg=m.cfg, flow_last_layer_abs_sum=[float(L.net[-1].weight.abs().sum() + L.net[-1].bias.abs().sum()) for L in m.flow.layers])
    R['movement'] = movement(m, B, a.device)
    for K in (32, 100):
        m.qfam = 'flow'; bf, ef = scores(m, B, K, a.device)
        m.qfam = 'gauss'; bg, eg = scores(m, B, K, a.device)                                                   # same model and draws, flow switched off
        m.qfam = 'flow'
        R[f'K{K}'] = dict(bound_flow=bf, bound_gauss=bg, bound_gain_flow=bf - bg, ess_flow=ef, ess_gauss=eg)
    print(json.dumps(R, indent=1)); json.dump(R, open(f'{a.run}/flowcheck_{a.name[:-3]}.json', 'w'), indent=1)


if __name__ == '__main__':
    main()
