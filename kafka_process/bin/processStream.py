#!/usr/bin/env python

"""Consumes stream for printing all messages to the console.
"""

import argparse
import json
import sys
import time
import socket
from confluent_kafka import Consumer, KafkaError, KafkaException
import pandas as pd 
import numpy as np

from numpy.random import RandomState
import pickle

from sklearn.preprocessing import MinMaxScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.metrics import plot_confusion_matrix

# CLASSIFIERS FOR TRAINING
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier, GradientBoostingClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis, QuadraticDiscriminantAnalysis
from sklearn.linear_model import LogisticRegression
from collections import Iterable
import statistics
from statistics import mode

def flatten(lis):
    for item in lis:
        if isinstance(item, Iterable) and not isinstance(item, str):
            for x in flatten(item):
                yield x
        else:        
             yield item


def msg_process(msg):

    modd = ['/Users/ashwinv/Documents/SEM5/dbms/project/LogisticRegression_model.pickle',
            '/Users/ashwinv/Documents/SEM5/dbms/project/AdaBoostClassifier_model.pickle',
            '/Users/ashwinv/Documents/SEM5/dbms/project/DecisionTreeClassifier_model.pickle',
            '/Users/ashwinv/Documents/SEM5/dbms/project/GaussianNB_model.pickle',
            '/Users/ashwinv/Documents/SEM5/dbms/project/GradientBoostingClassifier_model.pickle',
            '/Users/ashwinv/Documents/SEM5/dbms/project/KNeighborsClassifier_model.pickle',
            '/Users/ashwinv/Documents/SEM5/dbms/project/LinearDiscriminantAnalysis_model.pickle',
            '/Users/ashwinv/Documents/SEM5/dbms/project/QuadraticDiscriminantAnalysis_model.pickle',
            '/Users/ashwinv/Documents/SEM5/dbms/project/RandomForestClassifier_model.pickle',
            '/Users/ashwinv/Documents/SEM5/dbms/project/SVC_model.pickle']

    # Print the current time and the message.
    #time_start = time.strftime("%Y-%m-%d %H:%M:%S")

    val = msg.value()
    dval = json.loads(val)


    d = list(dval.values())
    tn = np.array(d[0])
    tn = tn.reshape(1, -1) 

    lis = []
    for model in modd:
        loaded_model = pickle.load(open(model, 'rb'))
        result = loaded_model.predict(tn)
        result = result.tolist()
        lis.append(result)
    li = list(flatten(lis))
    try:
        if(int(mode(li))==1):
            print("fraud")
        else:
            print("not fraud")
    except:
        print("not fraud")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('topic', type=str,
                        help='Name of the Kafka topic to stream.')

    args = parser.parse_args()

    conf = {'bootstrap.servers': 'localhost:9092',
            'default.topic.config': {'auto.offset.reset': 'smallest'},
            'group.id': socket.gethostname()}

    consumer = Consumer(conf)

    running = True

    try:
        while running:
            consumer.subscribe([args.topic])

            msg = consumer.poll(1)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                    sys.stderr.write('Topic unknown, creating %s topic\n' %
                                     (args.topic))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)

    except KeyboardInterrupt:
        pass

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


if __name__ == "__main__":
    main()
