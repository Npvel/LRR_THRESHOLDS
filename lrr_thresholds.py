import pandas as pd
import streamlit as st

from file_proccessing import data_clean


st.markdown(""" <style> .font {
font-size:35px ; font-family: 'Times'; color: #4d4643;} 
</style> """, unsafe_allow_html=True)
st.markdown('<h1 class="font">LRR  THRESHOLDS  </h1>', unsafe_allow_html=True)   

col1, col2 = st.columns([0.3, 0.2])
with col1:
    pd.set_option('display.max_colwidth', 40)
    uploaded_file = st.file_uploader("Choose a file", type = ['csv'])
    global data
    if uploaded_file is not None:
        try:

            data = pd.read_csv(uploaded_file, sep = ';', encoding='iso-8859-3', error_bad_lines=False,
                        dtype={'Alim Tp Nat Id': str, 'Alim Tp Review Categ Cde':str,
                               'Alim Tp Grade Man Origin Cde':str, 'Alim Tp Hg Eh Id':str, "Alim Tp Eh Id":str,
                               'Trade_Sector_ID':str, 'Trade_Sector_ID':str , 'Alim Tp Grp Grade':str, 
                               'Alim Tp Grp Max Exp Agreed Eur':str, 
                              "Alim Tp Address Pc":str, 'Alim Tp Review Categ Cde':str, 'Alim Tp Grp Eh Id':str,
                              'Alim Tp Hg Grp Grade':str, 'Alim Tp Grp Exp Global Eur':str}, 
                        parse_dates = ['Alim Tp Grade Man Dt', 'Alim Tp Grade Auto Dt', 'Alim Tp Adt Dt'])

        except KeyError:
            st.error("Please upolad correct file from CBIS")


lrr_count, lrr_buyers = data_clean(data)

st.dataframe(lrr_count)

st.dataframe(lrr_buyers)
