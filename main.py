# coding:utf-8
from __future__ import absolute_import
import tensorflow as tf
import os
import time
from csrm import CSRM
import argparse
import data_process
import numpy as np

def parse_args():
    parser = argparse.ArgumentParser(description="Run CSRM.")
    parser.add_argument('--dataset', nargs='?', default='lastfm',
                        help='Choose a dataset.')
    parser.add_argument('--epoch', type=int, default=50,
                        help='Number of epochs.')
    parser.add_argument('--batch_size', type=int, default=512,
                        help='Batch size.')
    parser.add_argument('--n_items', type=int, default=39164,
                        help='Item size 37484, 39164')
    parser.add_argument('--dim_proj', type=int, default=150,
                        help='Item embedding dimension. initial:50')
    parser.add_argument('--hidden_units', type=int, default=150,
                        help='Number of GRU hidden units. initial:100')
    parser.add_argument('--patience', type=int, default=10,
                        help='Number of epoch to wait before early stop if no progress.')
    parser.add_argument('--display_frequency', type=int, default=100,
                        help='Display to stdout the training progress every N updates.')
    parser.add_argument('--lr', type=float, default=0.0005,
                        help='Learning rate.')
    parser.add_argument('--keep_probability', nargs='?', default='[0.75,0.5]',
                        help='Keep probability (i.e., 1-dropout_ratio). 1: no dropout.')
    parser.add_argument('--no_dropout', nargs='?', default='[1.0,1.0]',
                        help='Keep probability (i.e., 1-dropout_ratio). 1: no dropout.')
    parser.add_argument('--memory_size', type=int, default=512,
                        help='.')
    parser.add_argument('--memory_dim', type=int, default=100,
                        help='.')
    parser.add_argument('--shift_range', type=int, default=1,
                        help='.')
    parser.add_argument('--controller_layer_numbers', type=int, default=0,
                        help='.')
    return parser.parse_args()


def main():
    # 指定运行的GPU
    os.environ['CUDA_VISIBLE_DEVICES'] = '0'
    config = tf.ConfigProto()
    config.gpu_options.allow_growth = True

    args = parse_args()

    # 输出参数
    print "args: ", args

    load_data_start_time = time.time()
    # 载入数据集
    train, valid, test = data_process.load_data()

    print("Loading data done. (%0.3f s)" % (time.time() - load_data_start_time))
    print("%d train examples." % len(train[0]))
    print("%d valid examples." % len(valid[0]))

    # 数据集统计信息
    print("%d test examples." % len(test[0]))

    keep_probability = np.array(args.keep_probability)
    no_dropout = np.array(args.no_dropout)

    result_path = "./save/" + args.dataset

    # Build model
    with tf.Session(config=config) as sess:
        # 建立模型
        model = CSRM(
            sess=sess,
            n_items=args.n_items,
            dim_proj=args.dim_proj,
            hidden_units=args.hidden_units,
            patience=args.patience,
            memory_size=args.memory_size,
            memory_dim=args.memory_dim,
            shift_range=args.shift_range,
            controller_layer_numbers=args.controller_layer_numbers,
            batch_size=args.batch_size,
            epoch=args.epoch,
            lr=args.lr,
            keep_probability=keep_probability,
            no_dropout=no_dropout,
            display_frequency=args.display_frequency)

        # 训练
        model.train(train, valid, test, result_path)

if __name__ == '__main__':
    main()
