# coding:utf-8
from __future__ import print_function
import pickle

import numpy
numpy.random.seed(42)


def prepare_data(seqs, labels):
    """Create the matrices from the datasets.

    This pad each sequence to the same lenght: the lenght of the
    longuest sequence or maxlen.

    if maxlen is set, we will cut all sequence to this maximum
    lenght.

    This swap the axis!
    """
    # x: a list of sentences

    lengths = [len(s) for s in seqs]
    n_samples = len(seqs)
    maxlen = numpy.max(lengths)
    x = numpy.zeros((n_samples, maxlen), dtype=numpy.int64)
    x_mask = numpy.ones((n_samples, maxlen), dtype=numpy.float32)
    for idx, s in enumerate(seqs):
        x[idx, :lengths[idx]] = s

    x_mask *= (1 - (x == 0)) #将x的非0元素变为1
    # seq_length = [i if i <= maxlen else maxlen for i in lengths]

    return x, x_mask, labels, lengths


def load_data():
    '''
    Load the dataset
    '''

    path_train_data = 'data/lastfm_train.pkl'
    path_valid_data = 'data/lastfm_valid.pkl'
    path_test_data = 'data/lastfm_test.pkl'

    f1 = open(path_train_data, 'rb')
    train = pickle.load(f1)
    f1.close()

    f2 = open(path_valid_data, 'rb')
    valid = pickle.load(f2)
    f2.close()

    f3 = open(path_test_data, 'rb')
    test = pickle.load(f3)
    f3.close()

    return train, valid, test
