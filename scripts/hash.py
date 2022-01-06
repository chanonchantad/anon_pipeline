import hashlib, io

convert = {
    str: lambda x : bytes(x, 'utf-8'),
    int: lambda x : bytes(x),
    io.BufferedReader: lambda x : x.read()}

def hash(obj):
    """
    Method to implement deterministic 

    """
    assert type(obj) in convert

    m = hashlib.sha1()
    m.update(convert[type(obj)](obj))

    return m.hexdigest()
