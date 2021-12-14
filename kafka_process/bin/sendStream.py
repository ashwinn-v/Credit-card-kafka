def warn(*args, **kwargs):
    pass
import warnings
warnings.warn = warn
import streamlit as st
import argparse
import json
import sys
import time
import socket
from confluent_kafka import Consumer, KafkaError, KafkaException
import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt
from numpy.random import RandomState
import pickle
from PIL import Image
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

    modd = ['/Users/ashwinv/Documents/SEM5/dbms/project/pkl/LogisticRegression_model.pickle',
            '/Users/ashwinv/Documents/SEM5/dbms/project/pkl/AdaBoostClassifier_model.pickle',
            '/Users/ashwinv/Documents/SEM5/dbms/project/pkl/DecisionTreeClassifier_model.pickle',
            # '/Users/ashwinv/Documents/SEM5/dbms/project/pkl/GaussianNB_model.pickle',
            '/Users/ashwinv/Documents/SEM5/dbms/project/pkl/GradientBoostingClassifier_model.pickle',
            '/Users/ashwinv/Documents/SEM5/dbms/project/pkl/KNeighborsClassifier_model.pickle',
            '/Users/ashwinv/Documents/SEM5/dbms/project/pkl/LinearDiscriminantAnalysis_model.pickle',
            '/Users/ashwinv/Documents/SEM5/dbms/project/pkl/QuadraticDiscriminantAnalysis_model.pickle',
            '/Users/ashwinv/Documents/SEM5/dbms/project/pkl/plkRandomForestClassifier_model.pickle'
            # '/Users/ashwinv/Documents/SEM5/dbms/project/pkl/plkSVC_model.pickle'
            ]

    # Print the current time and the message.
    #time_start = time.strftime("%Y-%m-%d %H:%M:%S")

    val = msg.value()
    dval = json.loads(val)
    f = 0

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
        if(mode(li)==1):
            print("fraud")
            f = 1
        else:
            print("not fraud")
    except:
        print("not fraud")
    return f
#################
    

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
        wnf = 0
        wf = 0
        p1 = st.empty()
        p2 = st.empty()
        p3 = st.empty()
        p4 = st.empty()
        p5 = st.empty()
        while running:
            fnf = 0
            ff = 0
            for _ in range(5):
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
                    f = msg_process(msg)
                if(f==1):
                    ff = ff+1
                else:
                    fnf =fnf+1
            wf = wf+ff
            wnf = wnf+fnf

            
            p1.header("Total Number of Credit Cards Tested : {}".format(wnf+wf))
        
            
            p2.metric("Total Number of Genuine Cards Detected:", wnf)
            
            p3.metric("Total Number of Fraud Cards Detected:", wf)
            # st.sidebar.write("Total Positive Tweets are : {}".format(pos_count))
            # st.sidebar.write("Total Negative Tweets are : {}".format(neg_count))
            # st.sidebar.write("Total Neutral Tweets are : {}".format(neutral_count))
            
            
            p4.subheader("Pie Chart Analysis")
            d=np.array([wnf,wf])
            explode = (0.1, 0.1)
            plt.pie(d,shadow=True,explode=explode,labels=["Genuine","Fraud"])
            # plt.pie(d,shadow=True,explode=explode,labels=["not fraud","fraud"],autopct='%1.2f%%')
            plt.savefig("/Users/ashwinv/Documents/SEM5/dbms/project/code/imfs/pie.jpeg")
            plt.clf()
            img = Image.open("/Users/ashwinv/Documents/SEM5/dbms/project/code/imfs/pie.jpeg")
            p5.image(img)

    except KeyboardInterrupt:
        pass

    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

st.title('Real Time Fraudulent Credit Card Detection')
image = Image.open('/Users/ashwinv/Documents/SEM5/dbms/project/code/imfs/sad.png')
st.image(image, caption='Welcome to our webapp!', use_column_width=True)
st.subheader('19AIE304 Big Data and Database Management')
st.subheader("Group 4", anchor=None)
st.subheader("Team Members", anchor=None)
st.markdown('Ashwin V   &emsp;&emsp; &emsp; &emsp;19018  <br> Devagopal AM &emsp; &emsp;19025  <br> Nived PA &emsp;&emsp;&emsp;&emsp;&nbsp;  19045<br>Vishal Menon &nbsp;&nbsp;&emsp; &emsp;19070 ', unsafe_allow_html=True)
st.subheader("Submitted to", anchor=None)
st.markdown("Dr. Divya Udayan J")

if st.button("Start Real Time Streaming and Prediction"):
    main()