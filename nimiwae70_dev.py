"""D70 dev run: one arm of the age on / age off comparison. Fits on TRAIN only and saves the model; scoring is nimiwae70_eval.py.
  python -I nimiwae70_dev.py --data DIR --out DIR --age 1 --trees 150000 --epochs 4 --threads 4"""
import sys, os, json, argparse, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import pandas as pd, torch
import nimiwae70 as A

if __name__ == '__main__':
    ap = argparse.ArgumentParser(); ap.add_argument('--data', required=True); ap.add_argument('--out', required=True); ap.add_argument('--age', type=int, default=1)
    ap.add_argument('--trees', type=int, default=150000); ap.add_argument('--epochs', type=int, default=4); ap.add_argument('--k', type=int, default=32); ap.add_argument('--threads', type=int, default=4); ap.add_argument('--seed', type=int, default=0); ap.add_argument('--device', default='cpu'); ap.add_argument('--bs', type=int, default=512); ap.add_argument('--opt', default='adamw', choices=['adamw', 'sf'])
    a = ap.parse_args(); os.makedirs(a.out, exist_ok=True)
    seq = pd.read_parquet(f'{a.data}/seq.parquet'); tr = seq[seq.split == 'train']; del seq
    rec = pd.read_parquet(f'{a.data}/age_rec.parquet'); rec = rec[(rec.split == 'train') & rec.first_of_tree & rec.fit_ok]
    log = open(f'{a.out}/run.log', 'a'); pr = lambda s: (print(s, flush=True), log.write(s + '\n'), log.flush())
    t0 = time.time()
    m, h = A.train(tr, rec, epochs=a.epochs, k=a.k, max_trees=a.trees, seed=a.seed, threads=a.threads, log=pr, age=bool(a.age), device=a.device, bs=a.bs, opt_name=a.opt)
    torch.save(dict(state=m.state_dict(), vocab=m.vocab, cfg=m.cfg, hist=h), f'{a.out}/model.pt')
    pr(f'done {time.time() - t0:.0f} s')
