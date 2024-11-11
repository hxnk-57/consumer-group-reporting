import pandas as pd
import streamlit as st
from dotenv import load_dotenv
import os

load_dotenv()

REPORT_NAME = "consumer_group_states.csv"


def load_and_process_csv() -> pd.DataFrame:
    df = pd.read_csv(REPORT_NAME)
    dummies = pd.get_dummies(df['state'], prefix='', prefix_sep='')
    df_with_dummies = pd.concat([df[['consumer_group_id']], dummies], axis=1)
    result = df_with_dummies.groupby('consumer_group_id').sum().reset_index()
    result.columns.name = None  # Remove the name of the index
    
    result.rename(columns=({"consumer_group_id" : "Consumer Group", "PREPARING_REBALANCE" : "Rebalaces"}), inplace=True)    
    
    if 'Rebalaces' in result.columns:
        result = result.sort_values(by=['Rebalaces'], ascending=False)
    return result.drop(columns="STABLE")


def calculate_window() -> str:
    df = pd.read_csv(REPORT_NAME)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    return f"{df["timestamp"].min()} - {df["timestamp"].max()}"
    

def main():
    st.title("Consumer Group Monitoring")
    st.header(f"CLUSTER: {os.getenv("CLUSTER_ALIAS")}")
    st.subheader(f"Window: {calculate_window()}")
    st.write("Consumer Groups that have experienced a rebalance within the window period will apear here.")

    if st.button("Refresh"):
        st.write("Data refreshed!")
        processed_data = load_and_process_csv()
        st.dataframe(processed_data)


if __name__ == '__main__':
    main()