import os

for f in os.listdir('.'):
    if not f.startswith('model_') or not os.path.isdir(f):
        continue
    slices = os.listdir(f'./{f}')
    for s in slices:
        if not s.startswith('model_slice.'):
            continue
        idstr = s[len("model_slice."):]
        id = int(idstr)
        os.rename(f'./{f}/{s}', f'./{f}/model_slice.{id:03d}')