import pandas as pd
import numpy as np

def data_clean(df):

    df.rename(columns={"Alim Tp Eh Id": "EHID_Buyer", "Alim Tp Name": "Buyer", "Alim Tp Nat Id Type": "Type_Nat_ID",
                                  "Alim Tp Nat Id": "National_ID", "Alim Tp Address Ctry": "Country", "Alim Tp Address Pc":
                                  "Post_Index", "Alim Tp Address Town": "City", "Alim Tp Nace Cde": "Nace_ID",
                                  "Alim Tp Nace Name": "Nace_name", "Alim Tp Trade Sector Cde": "Trade_Sector_ID",
                                  "Alim Tp Eh Trade Sector Name": "Trade_Sector_Name", "Alim Tp Grade Man": "Manual_Grade", 
                                  "Alim Tp Grade Man Dt": "Manual_Grade_date", "Alim Tp Grade Auto": "AutoGrade",
                                  "Alim Tp Grade Auto Dt": "AutoGrade_date", "Alim Req Perm Amt": "Requested_amount",
                                  "Alim Ref Ccy": "Currency", "Alim Req Perm Eur": "Requested_amount_Eur", "Alim Perm Amt":
                                  "Granted_amount", "Alim Temp Inc Amt": "Temporary_limit", "Alim Perm Eur": "Active_limit_Eur",
                                  "Alim Temp Inc Eur": "Temporary_limit_Eur", "Alim Crea Dt": "Decision_date", 
                                  "Alim Perm Ref Dt": "Alim Perm Ref Dt", "Alim Perm Expy Dt": "Expiration_date",
                                  "Alim Temp Inc Ref Dt": "Alim Temp Inc Ref Dt", "Alim Temp Inc Expy Dt": "Temp_decision_exp_date",
                                  "Alim Ctr Local Nr": "Contract_#", "Alim Ctr Name": "Customer", "Alim Ctr Insrd Tp Eh Id": 
                                  "EHID_Customer","Alim Ctr Status Cde": "Contract_Status", "Alim Ctr Dur Start Dt": 
                                  "Contract_signed_date", "Alim Ctr Dur End Dt": "Contract_expiration_date", 
                                  "Alim Tp Ins Nat Id Type": "Customer_Nat_Id_Type", "Alim Tp Ins Nat Id": "Customer_national_ID", 
                                  "Alim Tp Ins Address Ctry": "Customer_country", "Alim Tp Ins Address Pc": "Customer_Post_Index", 
                                  "Alim Tp Ins Address Town": "Customer_city", "Alim Ctr Prod Name": "Contract_origin",
                                   "Alim Tp Hg Eh Id": "Group_EHID", "Alim Tp Hg Name": "Group_name",
                                  'Alim Tp Hg Grade Man':'Group_grade',
                                   "Alim Tp Ins Name": "Customer_name", 'Alim Tp Adt Grade': 'Current_grade(ADT)',
                         'Alim Tp Adt Dt': 'Current_grade_date', 'Alim Tp Review Categ Cde': 'Risk_category'}, inplace=True)

    df = df.sort_values("Decision_date").drop_duplicates(subset=["EHID_Buyer", "Contract_#"], keep='last')

    df['Active_limit_Eur'] = df['Active_limit_Eur'].str.replace(',', '.')

    df['Active_limit_Eur'] = pd.to_numeric(df['Active_limit_Eur'], downcast='float')

    df['Nace_ID'] = df['Nace_ID'].astype('str')
    df['Trade_Sector_ID'] = df['Trade_Sector_ID'].astype('str')
    df['Current_grade(ADT)'] = df['Current_grade(ADT)'].astype('str')
    df['Alim Tp Grp Grade'] = df['Alim Tp Grp Grade'].astype('str')
    df['Alim Tp Grp Exp Global Eur'] = df['Alim Tp Grp Exp Global Eur'].astype('str')
    df['Alim Tp Grp Grade'] = df['Alim Tp Grp Grade'].apply(lambda x: x[-1])
    df['Current_grade(ADT)'] = df['Current_grade(ADT)'].apply(lambda x: x.split('.')[0])


    monitoring_list = df[['EHID_Buyer','Buyer', 'National_ID', 'Country', 'Nace_name', 'Trade_Sector_Name', 'Trade_Sector_ID',
                             'Nace_ID','Current_grade(ADT)', 'Current_grade_date', 'Risk_category', 'Group_name','Group_EHID',
                             'Alim Tp Grp Name','Alim Tp Grp Eh Id','Currency', 'Granted_amount', 'Temporary_limit',
                               'Active_limit_Eur', 
                             'Contract_Status','Contract_signed_date',  'Alim Tp Legal Form Cde', 'Customer_name', 'Contract_#',
                             'Customer_Nat_Id_Type','Alim Tp Grp Grade', 'Alim Tp Grp Max Exp Agreed Eur', 'Alim Ctr Inf Ehbu Id',
                             'Alim Tp Grp Exp Global Eur','Alim Tp Hg Address Ctry', 'Alim Pltcl Risk Name']].copy()

    monitoring_list = monitoring_list[(monitoring_list['Contract_Status'] == "SIG") | (monitoring_list['Contract_Status'] == "SUS")]
    monitoring_list['National_ID'] = monitoring_list['National_ID'].astype(str)

    monitoring_list_sum = monitoring_list.groupby(['EHID_Buyer', 'National_ID', 'Country', 'Buyer',  'Current_grade(ADT)', 
                                                'Current_grade_date', 'Trade_Sector_Name', 'Trade_Sector_ID', 'Nace_name',
                                                'Nace_ID', 'Risk_category','Group_name', 'Group_EHID', 'Alim Tp Grp Grade','Currency',
                                                'Alim Tp Grp Exp Global Eur', 'Alim Tp Hg Address Ctry', 'Alim Pltcl Risk Name',
                                                'Alim Tp Legal Form Cde', "Alim Ctr Inf Ehbu Id"]).sum().reset_index()

    monitoring_list_sum['Risk_category'] = monitoring_list_sum['Risk_category'].apply(lambda x : f'{x}')

    monitoring_list_sum = monitoring_list_sum.sort_values('Active_limit_Eur', ascending=False)

    monitoring_list_sum['Alim Tp Grp Exp Global Eur'] = monitoring_list_sum['Alim Tp Grp Exp Global Eur'].str.replace(',', '.')
    monitoring_list_sum['Alim Tp Grp Exp Global Eur'] = pd.to_numeric(monitoring_list_sum['Alim Tp Grp Exp Global Eur'], downcast='integer')

    monitoring_list_sum.loc[((monitoring_list_sum['Alim Tp Grp Grade'] == '1') |
                        (monitoring_list_sum['Alim Tp Grp Grade'] == '2') |
                        (monitoring_list_sum['Alim Tp Grp Grade'] == '3')) &
                        ((monitoring_list_sum['Alim Tp Grp Exp Global Eur'] > 200000000) &
                        (monitoring_list_sum['Alim Tp Grp Exp Global Eur'] <= 1000000000)), 'LRR_type'] = 'Region'

    monitoring_list_sum.loc[((monitoring_list_sum['Alim Tp Grp Grade'] == '1') |
                            (monitoring_list_sum['Alim Tp Grp Grade'] == '2') |
                            (monitoring_list_sum['Alim Tp Grp Grade'] == '3')) &
                            ((monitoring_list_sum['Alim Tp Grp Exp Global Eur'] <= 200000000) &
                            (monitoring_list_sum['Alim Tp Grp Exp Global Eur'] >= 75000000)), 'LRR_type'] = 'Local'

    monitoring_list_sum.loc[(monitoring_list_sum['Alim Tp Grp Grade'] == '4') &
                            ((monitoring_list_sum['Alim Tp Grp Exp Global Eur'] > 100000000) &
                            (monitoring_list_sum['Alim Tp Grp Exp Global Eur'] <= 500000000)), 'LRR_type'] = 'Region'
                            
    monitoring_list_sum.loc[(monitoring_list_sum['Alim Tp Grp Grade'] == '4') &
                            ((monitoring_list_sum['Alim Tp Grp Exp Global Eur'] <= 100000000) &
                            (monitoring_list_sum['Alim Tp Grp Exp Global Eur'] >= 40000000)), 'LRR_type'] = 'Local'
                            
    monitoring_list_sum.loc[(monitoring_list_sum['Alim Tp Grp Grade'] == '5') &
                            ((monitoring_list_sum['Alim Tp Grp Exp Global Eur'] > 50000000) &
                            (monitoring_list_sum['Alim Tp Grp Exp Global Eur'] <= 250000000)), 'LRR_type'] = 'Region'
                            
    monitoring_list_sum.loc[(monitoring_list_sum['Alim Tp Grp Grade'] == '5') &
                            ((monitoring_list_sum['Alim Tp Grp Exp Global Eur'] <= 50000000) &
                            (monitoring_list_sum['Alim Tp Grp Exp Global Eur'] >= 25000000)), 'LRR_type'] = 'Local'  

    monitoring_list_sum.loc[(monitoring_list_sum['Alim Tp Grp Grade'] == '6') &
                            ((monitoring_list_sum['Alim Tp Grp Exp Global Eur'] > 25000000) &
                            (monitoring_list_sum['Alim Tp Grp Exp Global Eur'] <= 150000000)), 'LRR_type'] = 'Region'
                            
    monitoring_list_sum.loc[(monitoring_list_sum['Alim Tp Grp Grade'] == '6') &
                            ((monitoring_list_sum['Alim Tp Grp Exp Global Eur'] < 25000000) &
                            (monitoring_list_sum['Alim Tp Grp Exp Global Eur'] >= 15000000)), 'LRR_type'] = 'Local' 

    monitoring_list_sum.loc[((monitoring_list_sum['Alim Tp Grp Grade'] == '1') |
                            (monitoring_list_sum['Alim Tp Grp Grade'] == '2') |
                            (monitoring_list_sum['Alim Tp Grp Grade'] == '3')) &
                            ((monitoring_list_sum['Alim Tp Grp Exp Global Eur'] > 200000000) &
                            (monitoring_list_sum['Alim Tp Grp Exp Global Eur'] <= 1000000000)), 'LRR_type'] = 'Region'

    monitoring_list_sum.loc[((monitoring_list_sum['Alim Tp Grp Grade'] == '1') |
                            (monitoring_list_sum['Alim Tp Grp Grade'] == '2') |
                            (monitoring_list_sum['Alim Tp Grp Grade'] == '3')) &
                            ((monitoring_list_sum['Alim Tp Grp Exp Global Eur'] <= 200000000) &
                            (monitoring_list_sum['Alim Tp Grp Exp Global Eur'] >= 75000000)), 'LRR_type'] = 'Local'

    monitoring_list_sum.loc[(monitoring_list_sum['Alim Tp Grp Grade'] == '4') &
                            ((monitoring_list_sum['Alim Tp Grp Exp Global Eur'] > 100000000) &
                            (monitoring_list_sum['Alim Tp Grp Exp Global Eur'] <= 500000000)), 'LRR_type'] = 'Region'
                            
    monitoring_list_sum.loc[(monitoring_list_sum['Alim Tp Grp Grade'] == '4') &
                            ((monitoring_list_sum['Alim Tp Grp Exp Global Eur'] <= 100000000) &
                            (monitoring_list_sum['Alim Tp Grp Exp Global Eur'] >= 40000000)), 'LRR_type'] = 'Local'
                            
    monitoring_list_sum.loc[(monitoring_list_sum['Alim Tp Grp Grade'] == '5') &
                            ((monitoring_list_sum['Alim Tp Grp Exp Global Eur'] > 50000000) &
                            (monitoring_list_sum['Alim Tp Grp Exp Global Eur'] <= 250000000)), 'LRR_type'] = 'Region'
                            
    monitoring_list_sum.loc[(monitoring_list_sum['Alim Tp Grp Grade'] == '5') &
                            ((monitoring_list_sum['Alim Tp Grp Exp Global Eur'] <= 50000000) &
                            (monitoring_list_sum['Alim Tp Grp Exp Global Eur'] >= 25000000)), 'LRR_type'] = 'Local'  

    monitoring_list_sum.loc[(monitoring_list_sum['Alim Tp Grp Grade'] == '6') &
                            ((monitoring_list_sum['Alim Tp Grp Exp Global Eur'] > 2500000) &
                            (monitoring_list_sum['Alim Tp Grp Exp Global Eur'] <= 15000000)), 'LRR_type'] = 'Region'
                            
    monitoring_list_sum.loc[(monitoring_list_sum['Alim Tp Grp Grade'] == '6') &
                            ((monitoring_list_sum['Alim Tp Grp Exp Global Eur'] < 25000000) &
                            (monitoring_list_sum['Alim Tp Grp Exp Global Eur'] >= 15000000)), 'LRR_type'] = 'Local' 

    monitoring_list_sum.loc[(monitoring_list_sum['Alim Tp Grp Grade'] == '7') &
                            ((monitoring_list_sum['Alim Tp Grp Exp Global Eur'] < 50000000) &
                            (monitoring_list_sum['Alim Tp Grp Exp Global Eur'] > 10000000)), 'LRR_type'] = 'Region'
                            
    monitoring_list_sum.loc[(monitoring_list_sum['Alim Tp Grp Grade'] == '7') &
                            ((monitoring_list_sum['Alim Tp Grp Exp Global Eur'] <= 10000000) &
                            (monitoring_list_sum['Alim Tp Grp Exp Global Eur'] >= 5000000)), 'LRR_type'] = 'Local' 


    monitoring_list_sum.loc[((monitoring_list_sum['Alim Tp Grp Grade'] == '8') |
                            (monitoring_list_sum['Alim Tp Grp Grade'] == '9')) &
                            ((monitoring_list_sum['Alim Tp Grp Exp Global Eur'] < 20000000) &
                            (monitoring_list_sum['Alim Tp Grp Exp Global Eur'] > 2000000)), 'LRR_type'] = 'Region'

    monitoring_list_sum.loc[((monitoring_list_sum['Alim Tp Grp Grade'] == '8') |
                            (monitoring_list_sum['Alim Tp Grp Grade'] == '9')) &
                            ((monitoring_list_sum['Alim Tp Grp Exp Global Eur'] <= 2000000) &
                            (monitoring_list_sum['Alim Tp Grp Exp Global Eur'] >= 1000000)), 'LRR_type'] = 'Local'
                    
    lrr_cc = monitoring_list_sum[['Group_name', 'Alim Tp Grp Grade', 'Alim Tp Grp Exp Global Eur', 'LRR_type', 'Alim Tp Hg Address Ctry']]

    lrr_cc = lrr_cc.drop_duplicates(subset=['Group_name', 'Alim Tp Grp Exp Global Eur'])


    lrr_cc = lrr_cc[lrr_cc['Alim Tp Hg Address Ctry'] == 'RU']

    lrr_cc = pd.pivot_table(lrr_cc, index='Alim Tp Grp Grade', fill_value=0, columns='LRR_type', values='Group_name', aggfunc='count')

    lrr_table = monitoring_list_sum[['Group_EHID','Group_name', 'Alim Tp Grp Grade', 'Alim Tp Grp Exp Global Eur','LRR_type', 'Alim Tp Hg Address Ctry']]
    
    lrr_table = lrr_table.drop_duplicates(subset=['Group_EHID', 'Alim Tp Grp Exp Global Eur'])

    lrr_table = lrr_table[lrr_table['Alim Tp Hg Address Ctry'] == 'RU']
    
    lrr_table['LRR_type'] = lrr_table['LRR_type'].fillna('net')
    
    lrr_table = lrr_table[lrr_table['LRR_type'] != 'net']

    lrr_table = lrr_table.sort_values('Alim Tp Grp Exp Global Eur', ascending=False)

    return lrr_cc, lrr_table

      