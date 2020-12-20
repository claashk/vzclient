
def compress_const(iterable, max_gap=None):
    """Compresses a time series by reducing number of nodes for const periods

    Eliminates nodes where the measurement does not change. If ``max_gap`` is
    specified, nodes are not deleted, if they
    
    Arguments:
        max_gap (int): Maximum distance between neighbouring nodes.

    Yield:
        int: Compressed items of iterable with constant nodes removed
    """
    it = iter(iterable)
    try:
        x0, y0 = next(it)
        xn, yn = x0, y0
    except StopIteration:
        return

    for x, y in it:
        if x == xn:
            continue

        if y == yn:
            if max_gap is not None and x - x0 > max_gap:
                yield x0, y0
                x0, y0 = xn, yn
            xn = x
            continue

        yield x0, y0
        if xn != x0:
            yield xn, yn

        x0, y0 = x, y
        xn, yn = x0, y0

    yield x0, y0
    if xn != x0:
        yield xn, yn
    return
