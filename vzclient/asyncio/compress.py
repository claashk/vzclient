from ..compress import Compressor


async def compress_const(aiterable, max_gap=None):
    """Compresses a time series by reducing number of nodes for const periods

    Eliminates nodes where the measurement does not change. If ``max_gap`` is
    specified, nodes are not deleted, if they

    Arguments:
        iterable (async interable): Async generator yielding chunks of data
        max_gap (int): Maximum distance between neighbouring nodes.

    Yield:
        list: Compressed items of iterable with constant nodes removed
    """
    compressor = Compressor(max_gap=max_gap)
    ait = aiterable.__aiter__()
    try:
        chunk = await ait.__anext__()
        yield list(compressor.compress(compressor.iter(chunk)))
    except StopAsyncIteration:
        return
    async for chunk in ait:
        yield list(compressor.compress(chunk))
    yield list(compressor.finalize())