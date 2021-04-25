import pytest
from dcp.storage.memory.iterator import SampleableIterator


def test_sampleable_iterator():
    itr = (i for i in range(100))
    si = SampleableIterator(itr)

    assert si.get_first() == 0
    assert list(si.head(10)) == list(range(10))
    assert next(si) == 0  # Take next, begin iteration
    # Exhaust rest of iterator
    assert list(si) == list(range(1, 100))
    # Now try again
    assert si.get_first() == 0  # Fine because part of sample
    assert list(si.head(10)) == list(range(10))  # ditto
    with pytest.raises(Exception):
        # Raises because out of sample, iterator already exhausted
        assert list(si) == list(range(20))

    # Test smaller
    itr = (i for i in range(10))
    si = SampleableIterator(itr)
    # We exhaust the iterator inside of head()
    assert list(si.head(20)) == list(range(10))
    # Should still iterate properly
    assert list(si) == list(range(10))
    assert list(si.head(20)) == list(range(10))
