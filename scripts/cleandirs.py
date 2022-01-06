import os, glob, shutil

def clean(root):
    root = os.path.normpath(root)
    files = glob.glob(root + '/*/*')
    for f in files:

        if os.path.isfile(f):
            os.remove(f)
        else:
            shutil.rmtree(f)
