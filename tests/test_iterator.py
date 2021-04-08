from datacopy.storage.memory.iterator import SampleableIterator
import pytest


def test_sampleable_iterator():
    itr = (i for i in range(100))

    si = SampleableIterator(itr)

    assert si.get_first() == 0
    assert list(si.head(10)) == list(range(10))
    # Exhaust entire iterator
    assert list(si) == list(range(100))
    # Now try again
    assert si.get_first() == 0  # Fine because part of sample
    assert list(si.head(10)) == list(range(10))  # ditto
    with pytest.raises(Exception):
        # Raises because out of sample, iterator already exhausted
        assert list(si) == list(range(20))

