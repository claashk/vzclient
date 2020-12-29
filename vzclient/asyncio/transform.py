try:
    import numpy as np
    WITH_NUMPY = True
except ImportError:
    WITH_NUMPY = False

async def chunk_trafo(aiterable, trafo, **kwargs):
    """Compresses a time series by reducing number of nodes for const periods

    Eliminates nodes where the measurement does not change. If ``max_gap`` is
    specified, nodes are not deleted, if they

    Arguments:
        iterable (async interable): Async generator
        max_gap (int): Maximum distance between neighbouring nodes.

    Yield:
        list: Compressed items of iterable with constant nodes removed
    """
    async for chunk in aiterable:
        yield trafo(chunk, **kwargs)

if WITH_NUMPY:
    def linear(chunk, scale=1., offset=0.):
        a = np.fromiter((x for t,x in chunk), dtype=np.float64)
        a *= scale
        a += offset
        return [(t, x) for (t, _,), x in zip(chunk, a)]
else:
    def linear(chunk, scale=1., offset=0.):
        return [(t, scale * x + offset) for t, x in chunk]

