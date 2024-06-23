import json

import pandas as pd 
import streamlit as st
from matplotlib import pyplot as plt
from confluent_kafka import Consumer

from producers.configurations import (data_producer_config_1,
                                      data_processor_topic_config)

if "data" not in st.session_state:
    st.session_state["data"] = []


def main():
    topic_consume = [data_processor_topic_config]
    conf_consume = {**data_producer_config_1, 'group.id': 'data_visualizers'}
    consumer = Consumer(conf_consume)
    consumer.subscribe(topic_consume)

    st.set_page_config(
        page_title='USA Real Estate Monitor',
        layout='wide',
    )

    st.text('Price distribution: observed vs expected')
    hists = st.empty()
    st.text('Real Estate objects by State')
    state_count = st.empty()
    st.text('Price distribution by number of bedrooms')
    price_by_bed = st.empty()
    st.text('Price distribution by number of bathrooms')
    price_by_bath = st.empty()


    while True:
        msg = consumer.poll(timeout=1000)

        if msg is not None:
            data = json.loads(msg.value().decode('utf-8'))
            print('Vizualizer received', data)
            st.session_state["data"].append(data)

        collected_data = pd.DataFrame(st.session_state["data"])

        fig, ax = plt.subplots(figsize=(5, 2))
        collected_data.price.hist(ax=ax, alpha=0.5)
        collected_data.prediction.hist(ax=ax, alpha=0.6)
        fig.legend(labels=['Actual', 'Predited'])
        ax.set_xlabel('Price in dollars')
        hists.pyplot(fig)

        state_count.bar_chart(data=collected_data.state.value_counts())
        price_by_bed.scatter_chart(data=collected_data, x='bed', y='price', color='state')
        price_by_bath.scatter_chart(data=collected_data, x='bath', y='price', color='state')


if __name__ == '__main__':
    main()