from datetime import datetime
import pandas as pd
import numpy as np
import warnings
import pickle
import mysql.connector as msql
from mysql.connector import Error
import os
import json
import re
import math
import ast

class WoE_Binning:
    def transform(self, X):
        X_new = pd.DataFrame()
        X_new["annualRevenue:missing"] = np.where(
            X["annualRevenue"].isnull(), 1, 0)
        X_new["annualRevenue:upto 3575000"] = np.where(
            (X["annualRevenue"] <= 3575000), 1, 0
        )
        X_new["annualRevenue:3575000 to 10052475"] = np.where(
            (X["annualRevenue"] > 3575000) & (
                X["annualRevenue"] <= 10052475), 1, 0
        )
        X_new["annualRevenue:10052475 to 26930889.25"] = np.where(
            (X["annualRevenue"] > 10052475) & (
                X["annualRevenue"] <= 26930889.25), 1, 0
        )
        X_new["annualRevenue:26930889.25 to 18200000000"] = np.where(
            (X["annualRevenue"] > 26930889.25), 1, 0
        )

        # [(-0.001, 12.0] < (12.0, 19.0] < (19.0, 141.0]],
        X_new["daysBeyondPayments:upto 12"] = np.where(
            (X["daysBeyondPayments"] <= 12), 1, 0
        )
        X_new["daysBeyondPayments:12 to 19"] = np.where(
            (X["daysBeyondPayments"] > 12) & (
                X["daysBeyondPayments"] <= 19), 1, 0
        )
        X_new["daysBeyondPayments:19 to 141"] = np.where(
            (X["daysBeyondPayments"] > 19), 1, 0
        )
        X_new["daysBeyondPayments:missing"] = np.where(
            X["daysBeyondPayments"].isnull(), 1, 0
        )

        # [(-0.001, 2800.0] < (2800.0, 30500.0] < (30500.0, 6554050.0]]
        X_new["totalPastDueBalanceOwned:upto 2800"] = np.where(
            (X["totalPastDueBalanceOwned"] <= 2800), 1, 0
        )
        X_new["totalPastDueBalanceOwned:2800 to 30500"] = np.where(
            (X["totalPastDueBalanceOwned"] > 2800)
            & (X["totalPastDueBalanceOwned"] <= 30500),
            1,
            0,
        )
        X_new["totalPastDueBalanceOwned:above 30500"] = np.where(
            (X["totalPastDueBalanceOwned"] > 30500), 1, 0
        )
        X_new["totalPastDueBalanceOwned:missing"] = np.where(
            X["totalPastDueBalanceOwned"].isnull(), 1, 0
        )

        # [(-0.001, 1.0] < (1.0, 2.0] < (2.0, 3.0] < (3.0, 5.0]]
        X_new["delinquencyScoreClass:missing"] = np.where(
            X["delinquencyScoreClass"].isnull(), 1, 0
        )
        X_new["delinquencyScoreClass:3 to 5"] = np.where(
            (X["delinquencyScoreClass"] > 3), 1, 0
        )
        X_new["delinquencyScoreClass:2 to 3"] = np.where(
            (X["delinquencyScoreClass"] > 2) & (
                X["delinquencyScoreClass"] <= 3), 1, 0
        )
        X_new["delinquencyScoreClass:1 to 2"] = np.where(
            (X["delinquencyScoreClass"] > 1) & (
                X["delinquencyScoreClass"] <= 2), 1, 0
        )
        X_new["delinquencyScoreClass:upto 1"] = np.where(
            (X["delinquencyScoreClass"] <= 1), 1, 0
        )

        # [(-0.001, 484.0] < (484.0, 532.0] < (532.0, 573.0] < (573.0, 611.0]]
        X_new["delinquencyRawScore:missing"] = np.where(
            X["delinquencyRawScore"].isnull(), 1, 0
        )
        X_new["delinquencyRawScore:upto 484"] = np.where(
            (X["delinquencyRawScore"] <= 484), 1, 0
        )
        X_new["delinquencyRawScore:484 to 532"] = np.where(
            (X["delinquencyRawScore"] > 484) & (
                X["delinquencyRawScore"] <= 532), 1, 0
        )
        X_new["delinquencyRawScore:532 to 573"] = np.where(
            (X["delinquencyRawScore"] > 532) & (
                X["delinquencyRawScore"] <= 573), 1, 0
        )
        X_new["delinquencyRawScore:greater than 573"] = np.where(
            (X["delinquencyRawScore"] > 573), 1, 0
        )

        # [(12.999, 65.0] < (65.0, 72.0] < (72.0, 79.0] < (79.0, 80.0]]
        X_new["paydexScore:missing"] = np.where(
            X["paydexScore"].isnull(), 1, 0)
        X_new["paydexScore:upto 65"] = np.where(
            (X["paydexScore"] <= 65.0), 1, 0)
        X_new["paydexScore:65 to 70"] = np.where(
            (X["paydexScore"] > 65.0) & (X["paydexScore"] <= 70.0), 1, 0
        )
        X_new["paydexScore:70 to 79"] = np.where(
            (X["paydexScore"] > 70.0) & (X["paydexScore"] <= 79.0), 1, 0
        )
        X_new["paydexScore:above 79"] = np.where(
            (X["paydexScore"] > 79.0), 1, 0)

        # [(-0.001, 22.0] < (22.0, 50.0] < (50.0, 68.0] < (68.0, 99.0]]
        X_new["Intelliscore Plus:missing"] = np.where(
            X["Intelliscore Plus"].isnull(), 1, 0
        )
        X_new["Intelliscore Plus:upto 19"] = np.where(
            (X["Intelliscore Plus"] <= 19), 1, 0
        )
        X_new["Intelliscore Plus:19 to 50"] = np.where(
            (X["Intelliscore Plus"] > 19) & (
                X["Intelliscore Plus"] <= 50), 1, 0
        )
        X_new["Intelliscore Plus:50 to 65"] = np.where(
            (X["Intelliscore Plus"] > 50) & (
                X["Intelliscore Plus"] <= 65), 1, 0
        )
        X_new["Intelliscore Plus:above 65"] = np.where(
            (X["Intelliscore Plus"] > 65), 1, 0
        )

        # [(-0.001, 29.5] < (29.5, 69.0] < (69.0, 88.5] < (88.5, 100.0]]
        X_new["FSR Score:missing"] = np.where(X["FSR Score"].isnull(), 1, 0)
        X_new["FSR Score:upto 27"] = np.where((X["FSR Score"] <= 27), 1, 0)
        X_new["FSR Score:27 to 67"] = np.where(
            (X["FSR Score"] > 27) & (X["FSR Score"] <= 67), 1, 0
        )
        X_new["FSR Score:67 to 86"] = np.where(
            (X["FSR Score"] > 67) & (X["FSR Score"] <= 86), 1, 0
        )
        X_new["FSR Score:above 86"] = np.where((X["FSR Score"] > 86), 1, 0)

        return X_new


class ValidationError(Exception):
    def __init__(self, msg):
        self.msg = msg


def calculate_final_score_bureau_1(owedAmount, daysBeyondPayments, paydexScore, delinquencyRawScore):
    # Define the bins and scores for different variables
    owedAmount_bins = [0, 1000, 10000]
    owedAmount_scores = [4, 3, 2, 1]

    daysBeyondPayments_bins = [0, 10, 15]
    daysBeyondPayments_scores = [4, 3, 2, 1]

    paydexScore_bins = [0, 49, 79, 100]
    paydexScore_scores = [1, 2, 3]

    delinquencyRawScore_bins = [101, 452, 480, 530, 580, 670]
    delinquencyRawScore_scores = [1, 2, 3, 4, 5]

    # Define a function to calculate the score and percentage
    def calculate_score_and_percentage(value, bins, scores):
        if value is None or value == "null" or value == "None" or value == "":
            return 0  # Return 0 for non-numeric values
        try:
            value = float(value)
        except ValueError:
            return 0  # Return 0 if value cannot be converted to a float

        for i in range(len(bins) - 1):
            if value <= bins[i]:
                return scores[i]
        return scores[-1]

    # Calculate the scores for each variable
    owedAmount_score = calculate_score_and_percentage(
        owedAmount, owedAmount_bins, owedAmount_scores)
    daysBeyondPayments_score = calculate_score_and_percentage(
        daysBeyondPayments, daysBeyondPayments_bins, daysBeyondPayments_scores)
    paydexScore_score = calculate_score_and_percentage(
        paydexScore, paydexScore_bins, paydexScore_scores)
    delinquencyRawScore_score = calculate_score_and_percentage(
        delinquencyRawScore, delinquencyRawScore_bins, delinquencyRawScore_scores)
    # Calculate the average score
    average_score = ((owedAmount_score + daysBeyondPayments_score +
                     paydexScore_score + delinquencyRawScore_score)/16)*10
    # Round the final score to the nearest whole number
    final_score = round(average_score, 2)

    # Determine remarks and financial advice
    remarks = ""
    financial_advice = ""

    if final_score >= 8 and owedAmount == 0 and daysBeyondPayments == 0:
        remarks = "Excellent Financial Status"
        financial_advice = "Financially sound, prompt payers, excellent liquidity management."
    elif final_score >= 8:
        remarks = "Strong Financial Status"
        financial_advice = "Slight liquidity challenges, manageable overdue payments, potential for improved working capital management."
    elif final_score < 8 and final_score > 5:
        remarks = "Average Financial Status"
        financial_advice = "Cash flow strain, consistent late payments, heightened credit risk, require vigilant receivables management."
    else:
        remarks = "Below Average Financial Status"
        financial_advice = "Severe liquidity issues, chronic delinquency, critical credit risk, implement stringent credit control measures."

    return final_score, remarks, financial_advice


def calculate_final_score_bureau_2(intelli_score, fsr_score):
    # Define the bins and scores for different variables
    intelli_bins = [1, 10, 25, 50, 75, 100]
    intelli_scores = [1, 2, 3, 4, 5, 6, 7]

    fsr_bins = [1, 3, 10, 30, 60, 100]
    fsr_scores = [1, 2, 3, 4, 5, 6, 7]

    # Define a function to calculate the score and percentage
    def calculate_score_and_percentage(value, bins, scores):
        if value is None or value == "null" or value == "None" or value == "":
            return 0  # Return 0 for non-numeric values
        try:
            value = float(value)
        except ValueError:
            return 0  # Return 0 if value cannot be converted to a float

        for i in range(len(bins) - 1):
            if value <= bins[i]:
                return scores[i]
        return scores[-1]

    # Calculate the scores for each variable
    intelli_score_score = calculate_score_and_percentage(
        intelli_score, intelli_bins, intelli_scores)
    fsr_score_score = calculate_score_and_percentage(
        fsr_score, fsr_bins, fsr_scores)

    # Calculate the average score
    average_score = ((intelli_score_score + fsr_score_score) / 14) * 10
    # Round the final score to the nearest whole number
    final_score = round(average_score, 2)

    # Determine remarks and financial advice
    remarks = ""
    financial_advice = ""

    if final_score >= 8:
        remarks = "Excellent Financial Status"
        financial_advice = "Excellent financial status with a stable financial position and well-established business."
    elif final_score < 8 and final_score >= 4:
        remarks = "Average Financial Status"
        financial_advice = "Average financial status with a stable financial position."
    else:
        remarks = "Below Average Financial Status"
        financial_advice = "Average financial status with an unstable financial position. Address stability concerns to enhance financial performance."

    return final_score, remarks, financial_advice


def calculate_final_score_bureau_3(google_rating):

    if google_rating is None or google_rating == "" or \
        (not isinstance(google_rating, (int, float)) and not isinstance(google_rating, np.integer)) or \
        (isinstance(google_rating, float) and math.isnan(google_rating)) or \
        (isinstance(google_rating, str) and google_rating.lower() == "nan"):
        # If the rating is not present or not a valid numeric value, assign a score of 0
        percentage_rating = 0.0
    else:
        # Calculate the percentage rating
        percentage_rating = (google_rating / 5.0) * 10

    final_score = round(percentage_rating, 2)

    # Determine remarks and financial advice
    remarks = ""
    financial_advice = ""

    if percentage_rating >= 9:
        remarks = "Excellent Popularity"
        financial_advice = "Customers are extremely satisfied and often describe their experience as outstanding."
    elif percentage_rating >= 8:
        remarks = "Good Popularity"
        financial_advice = "Customers have a very positive experience but there is some room for improvement."
    elif percentage_rating >= 6:
        remarks = "Average Popularity"
        financial_advice = "Customers are neither highly satisfied nor highly dissatisfied. There are notable drawbacks or areas that need improvement."
    elif percentage_rating >= 4:
        remarks = "Below Average Popularity"
        financial_advice = "Customers have an experience below expectations or disappointing. There are notable issues or shortcomings that need addressing."
    else:
        remarks = "Poor Popularity"
        financial_advice = "Customers are very dissatisfied and describe their experience as terrible, awful, or bad. The business or service needs substantial improvement."

    return final_score, remarks, financial_advice


# Adding percentile scoring

def percentile_scoring(input_df, mlDnbScore, mlExperianScore, mlGooglePlaceScore , ml_risk_score_input):

    filtered_df = input_df[['mlDnbScore',
                            'mlExperianScore', 'mlGooglePlaceScore','mlRiskScore']]
    # Filter rows where at least one column is not NaN
    score_df = filtered_df[filtered_df.notna().any(
        axis=1)].reset_index(drop=True)

    # Combine existing DataFrame with the new scores
    new_data = {
        'mlDnbScore': [mlDnbScore],
        'mlExperianScore': [mlExperianScore],
        'mlGooglePlaceScore': [mlGooglePlaceScore],
        'mlRiskScore':[ml_risk_score_input]
    }
    new_df = pd.DataFrame(new_data)
    # calculate_df = score_df.append(new_df, ignore_index=True)
    calculate_df = pd.concat([score_df, new_df], ignore_index=True)
    

    # Calculate percentile scores for each column in the updated DataFrame
    for column in ["mlDnbScore", "mlExperianScore", "mlGooglePlaceScore","mlRiskScore"]:
        calculate_df[column + '_Percentile'] = (
            calculate_df[column].rank(pct=True) * 100).round(2)
    # print(calculate_df[["mlDnbScore_Percentile", "mlExperianScore_Percentile", "mlGooglePlaceScore_Percentile","mlRiskScore_Percentile"]])

    # Extract and return percentile scores for the new company's scores
    new_percentile_scores = {
        'mlDnbScore_Percentile': calculate_df.iloc[-1]["mlDnbScore" + '_Percentile'],
        'mlExperianScore_Percentile': calculate_df.iloc[-1]["mlExperianScore" + '_Percentile'],
        'mlGooglePlaceScore_Percentile': calculate_df.iloc[-1]["mlGooglePlaceScore" + '_Percentile'],
        'mlRiskScore_Percentile': calculate_df.iloc[-1]["mlRiskScore" + '_Percentile']
    }

    return new_percentile_scores['mlDnbScore_Percentile'], new_percentile_scores['mlExperianScore_Percentile'], new_percentile_scores['mlGooglePlaceScore_Percentile'],new_percentile_scores['mlRiskScore_Percentile']

#Connect to the databse 
def run_query(query_text):
    host_name = os.environ["MYSQL_DATABASE_URL"]
    user_name = os.environ["MYSQL_DATABASE_USERNAME"]
    password_db = os.environ["MYSQL_DATABASE_PASSWORD"]
    port_db = os.environ["MYSQL_DATABASE_PORT"]
    db_name = os.environ["MYSQL_DATABASE_NAME"]
    try:
        print("Connect with DB")
        conn = msql.connect(
            host=host_name,
            user=user_name,
            password=password_db,
            port=port_db,
            database=db_name,
        )
        if conn.is_connected():
            print("Running query\n")
            df = pd.read_sql_query(query_text, conn)
            print("Read sucessfully\n")
            conn.close()
    except Error as e:
        print("Error while connecting to MySQL", e)

    return df


def get_company_info():
    # read CompanyScore table
    query = """SELECT bizLegalName , dunsNumber, bin, city, state, country, mlRiskScore , mlCreditLimit ,
        JSON_EXTRACT(dnbJson, '$."lineOfBusiness"') as "dnb_lineBusiness",
        JSON_EXTRACT(dnbJson, '$."naicsFullDescripion"') as "dnb_industry" ,
        JSON_EXTRACT(dnbJson, '$."naicsCodes"') as "dnb_naics" ,
        JSON_EXTRACT(experianBizJson, '$."naicsCodes"[*]."code"') as "experian_naics",
        JSON_EXTRACT(experianBizJson, '$."naicsCodes"[*]."definition"') as "experian_industry" ,
        JSON_EXTRACT(dnbJson, '$."naicsFullDescripion"') as "dnb_naicsFullDescripion" ,
        JSON_EXTRACT(experianBizJson, '$."naicsCodes"') as "experian_naicsFullDescripion",mlDnbScore,
        mlExperianScore,mlGooglePlaceScore
        from  CompanyScore ;"""
    compScore_df = run_query(query)
    return compScore_df


def func_strip(in_list):
    out_list = []
    for i in in_list:
        out_list.append(i.strip())
    return out_list


def extract_industry(data):
    descriptions = []
    json_data = json.loads(data)
    # Extract the "description" field
    descriptions = [
        re.sub(" +", " ", item["description"]).lower() for item in json_data
    ]
    return descriptions


def clean_exp_naics(data):
    if data is not None:
        return data.strip("][").split(",")
    
def extract_codes_experian(data):
    if data is not None:
        codes = [item['code'] for item in data if re.match(r'\b\d{6}\b', item['code'])]
        return codes
    else:
        return []

def extract_codes_dnb(data):
    if data is not None:
        codes = re.findall(r'\b\d{6}\b', data)
        return codes
    else:
        return []


def clean_dnb_naics(data):
    if data is not None:
        out = data.strip('"').split()
        # print(out)
        clean_naics = []
        for x in out:
            if "," in x:
                x = x.strip(",")
            clean_naics.append(x)
        return ['"' + x + '"' for x in clean_naics]


def clean_exp_industry(data):
    if data is not None:
        if isinstance(data, str):
            data = re.sub(" +", " ", data)
            return str(data).lower().strip('] ["').split('", "')
        

def getPeersScore(input,input_prod_df):
    tmp_df = pd.DataFrame()
    result = pd.DataFrame()
    compScore_df=input_prod_df

    compScore_df["bizLegalName"] = [x.strip()
                                    for x in compScore_df["bizLegalName"]]
    compScore_df["bizLegalName"] = [
        str(x).strip(".").lower() for x in compScore_df["bizLegalName"]
    ]

    # clean DnB naics
    compScore_df["dnb_naics"] = [clean_dnb_naics(
        x) for x in compScore_df["dnb_naics"]]

    # clean Experian naics
    compScore_df["experian_naics"] = [
        clean_exp_naics(x) for x in compScore_df["experian_naics"]
    ]

    # insert DnB naics value if experian naics is NULL
    compScore_df.experian_naics.fillna(compScore_df.dnb_naics, inplace=True)
    compScore_df["experian_naics"] = [
        func_strip(x) for x in compScore_df["experian_naics"]
    ]

    # Clean the dnb industry by extracting naics description
    compScore_df["dnb_industry"] = compScore_df["dnb_industry"].apply(
        json.loads)
    compScore_df["dnb_industry"] = [
        extract_industry(x) for x in compScore_df["dnb_industry"]
    ]

    # clean Experian industry
    compScore_df["experian_industry"] = [
        clean_exp_industry(x) for x in compScore_df["experian_industry"]
    ]

    # insert DnB Industry value if experian industry is NULL
    compScore_df.experian_industry.fillna(
        compScore_df.dnb_industry, inplace=True)

    # insert DnB FullDescripion value if experian FullDescripion is NULL
    compScore_df.experian_naicsFullDescripion.fillna(
        compScore_df.dnb_naicsFullDescripion, inplace=True
    )
    compScore_df["experian_naicsFullDescripion"] = compScore_df[
        "experian_naicsFullDescripion"
    ].str.replace("definition", "description")

    cp_naics_list = input[0]
    # Convert numeric values to strings with double quotes and wrap them in a list
    result_list = ['"' + str(value) + '"' for value in cp_naics_list]


    # Fetch COMPANYSCORE table except input CP, and make backup column for industry
    compScore_without_cp = compScore_df

    # Explode the NAICS
    compScore_without_cp_exp = compScore_without_cp.explode(
        ["experian_naics", "experian_industry"]
    ).reset_index()
    compScore_without_cp_exp = compScore_without_cp_exp.drop(columns=["index"])

    for naic in result_list:
        # print(naic)
        tmp_df = compScore_without_cp_exp.loc[
            compScore_without_cp_exp["experian_naics"] == naic
        ]

        if not tmp_df.empty:
            result = pd.concat([result, tmp_df])

    select_keys = [
        "bizLegalName",
        "dunsNumber",
        "mlRiskScore",
        "mlDnbScore",
        "mlExperianScore",
        "mlGooglePlaceScore"
    ]
    if not result.empty:
        result = result.loc[:, select_keys].rename(
            columns={"mlDnbScore": "Source1","mlExperianScore": "Source2","mlGooglePlaceScore": "Source3"}
        )
        result = result.drop_duplicates()
        peer_list = result.to_dict("records")

    return peer_list
        

def generateRiskScore(input):
    current_date = datetime.utcnow()

    # read the input from json and denormalize
    denorm_list_cols = [
        "bizLegalName",
        "bizDBA",
        "city",
        "state",
        "stateCode",
        "postalCode",
        "country",
        "countryCode",
        "assessment_annualSales",
        "assessment_maximumRecommendedCredit",
        "assessment_owedAmount",
        "assessment_highestCreditAmount",
        "assessment_daysBeyondPayments",
        "assessment_tradingExperienceCount",
        "assessment_tradingExperienceTotal",
        "assessment_totalPastDueBalanceOwned",
        "assessment_paydexScore",
        "assessment_delinquencyRawScore",
        "assessment_delinquencyScoreClass",
        "assessment_delinquencyScorePercentile",
        "assessment_delinquencyProbability",
        "assessment_dunsNumber",
        "assessment_naicsCodes",
        "experianBizJson_sicCodes",
        "experianBizJson_inquiries",
        "experianBizJson_naicsCodes",
        "experianBizJson_bondDetails",
        "experianBizJson_competitors",
        "experianBizJson_fortune1000",
        "experianBizJson_uccCoDebtors",
        "experianBizJson_businessFacts_businessType",
        "experianBizJson_businessFacts_employeeSize",
        "experianBizJson_businessFacts_salesRevenue",
        "experianBizJson_businessFacts_salesSizeCode",
        "experianBizJson_businessFacts_publicIndicator",
        "experianBizJson_businessFacts_yearsInBusiness",
        "experianBizJson_businessFacts_employeeSizeCode",
        "experianBizJson_businessFacts_nonProfitIndicator",
        "experianBizJson_businessFacts_dateOfIncorporation",
        "experianBizJson_businessFacts_fileEstablishedDate",
        "experianBizJson_businessFacts_locationEmployeeSize",
        "experianBizJson_businessFacts_locationSalesRevenue",
        "experianBizJson_businessFacts_stateOfIncorporation",
        "experianBizJson_businessFacts_locationEmployeeSizeCode",
        "experianBizJson_businessFacts_yearsInBusinessIndicator_code",
        "experianBizJson_businessFacts_yearsInBusinessIndicator_definition",
        "experianBizJson_paymentTotals_tradelines_dbt",
        "experianBizJson_paymentTotals_tradelines_dbt30",
        "experianBizJson_paymentTotals_tradelines_dbt60",
        "experianBizJson_paymentTotals_tradelines_dbt90",
        "experianBizJson_paymentTotals_tradelines_dbt91Plus",
        "experianBizJson_paymentTotals_tradelines_numberOfLines",
        "experianBizJson_paymentTotals_tradelines_currentPercentage",
        "experianBizJson_paymentTotals_tradelines_totalAccountBalance_amount",
        "experianBizJson_paymentTotals_tradelines_totalHighCreditAmount_amount",
        "experianBizJson_paymentTotals_combinedTradelines_dbt",
        "experianBizJson_paymentTotals_combinedTradelines_dbt30",
        "experianBizJson_paymentTotals_combinedTradelines_dbt60",
        "experianBizJson_paymentTotals_combinedTradelines_dbt90",
        "experianBizJson_paymentTotals_combinedTradelines_dbt91Plus",
        "experianBizJson_paymentTotals_combinedTradelines_numberOfLines",
        "experianBizJson_paymentTotals_combinedTradelines_currentPercentage",
        "experianBizJson_paymentTotals_combinedTradelines_totalAccountBalance_amount",
        "experianBizJson_paymentTotals_combinedTradelines_totalHighCreditAmount_amount",
        "experianBizJson_paymentTotals_additionalTradelines_dbt",
        "experianBizJson_paymentTotals_additionalTradelines_dbt30",
        "experianBizJson_paymentTotals_additionalTradelines_dbt60",
        "experianBizJson_paymentTotals_additionalTradelines_dbt90",
        "experianBizJson_paymentTotals_additionalTradelines_dbt91Plus",
        "experianBizJson_paymentTotals_additionalTradelines_numberOfLines",
        "experianBizJson_paymentTotals_additionalTradelines_currentPercentage",
        "experianBizJson_paymentTotals_additionalTradelines_totalAccountBalance_amount",
        "experianBizJson_paymentTotals_additionalTradelines_totalHighCreditAmount_amount",
        "experianBizJson_paymentTotals_newlyReportedTradelines_dbt",
        "experianBizJson_paymentTotals_newlyReportedTradelines_dbt30",
        "experianBizJson_paymentTotals_newlyReportedTradelines_dbt60",
        "experianBizJson_paymentTotals_newlyReportedTradelines_dbt90",
        "experianBizJson_paymentTotals_newlyReportedTradelines_dbt91Plus",
        "experianBizJson_paymentTotals_newlyReportedTradelines_numberOfLines",
        "experianBizJson_paymentTotals_newlyReportedTradelines_currentPercentage",
        "experianBizJson_paymentTotals_newlyReportedTradelines_totalAccountBalance_amount",
        "experianBizJson_paymentTotals_newlyReportedTradelines_totalHighCreditAmount_amount",
        "experianBizJson_paymentTotals_continuouslyReportedTradelines_dbt",
        "experianBizJson_paymentTotals_continuouslyReportedTradelines_dbt30",
        "experianBizJson_paymentTotals_continuouslyReportedTradelines_dbt60",
        "experianBizJson_paymentTotals_continuouslyReportedTradelines_dbt90",
        "experianBizJson_paymentTotals_continuouslyReportedTradelines_dbt91Plus",
        "experianBizJson_paymentTotals_continuouslyReportedTradelines_numberOfLines",
        "experianBizJson_paymentTotals_continuouslyReportedTradelines_currentPercentage",
        "experianBizJson_paymentTotals_continuouslyReportedTradelines_totalAccountBalance_amount",
        "experianBizJson_paymentTotals_continuouslyReportedTradelines_totalHighCreditAmount_amount",
        "experianBizJson_taxLienDetail",
        "experianBizJson_businessHeader_bin",
        "experianBizJson_businessHeader_phone",
        "experianBizJson_businessHeader_taxId",
        "experianBizJson_businessHeader_address_zip",
        "experianBizJson_businessHeader_address_city",
        "experianBizJson_businessHeader_address_state",
        "experianBizJson_businessHeader_address_street",
        "experianBizJson_businessHeader_address_zipExtension",
        "experianBizJson_businessHeader_dbaNames",
        "experianBizJson_businessHeader_websiteUrl",
        "experianBizJson_businessHeader_businessName",
        "experianBizJson_businessHeader_foreignCountry",
        "experianBizJson_businessHeader_legalBusinessName",
        "experianBizJson_businessHeader_customerDisputeIndicator",
        "experianBizJson_businessHeader_corporateLinkageIndicator",
        "experianBizJson_judgmentDetail",
        "experianBizJson_licenseDetails",
        "experianBizJson_bankruptcyDetail",
        "experianBizJson_corporateLinkage",
        "experianBizJson_executiveSummary_businessDbt_code",
        "experianBizJson_executiveSummary_businessDbt_definition",
        "experianBizJson_executiveSummary_commonTerms_first",
        "experianBizJson_executiveSummary_commonTerms_third",
        "experianBizJson_executiveSummary_commonTerms_second",
        "experianBizJson_executiveSummary_industryDbt",
        "experianBizJson_executiveSummary_predictedDbt",
        "experianBizJson_executiveSummary_allIndustryDbt",
        "experianBizJson_executiveSummary_predictedDbtDate",
        "experianBizJson_executiveSummary_industryDescription",
        "experianBizJson_executiveSummary_paymentTrendIndicator_code",
        "experianBizJson_executiveSummary_paymentTrendIndicator_definition",
        "experianBizJson_executiveSummary_highCreditAmountExtended",
        "experianBizJson_executiveSummary_industryPaymentComparison_code",
        "experianBizJson_executiveSummary_industryPaymentComparison_definition",
        "experianBizJson_executiveSummary_lowestTotalAccountBalance_amount",
        "experianBizJson_executiveSummary_lowestTotalAccountBalance_modifier",
        "experianBizJson_executiveSummary_currentTotalAccountBalance_amount",
        "experianBizJson_executiveSummary_currentTotalAccountBalance_modifier",
        "experianBizJson_executiveSummary_highestTotalAccountBalance_amount",
        "experianBizJson_executiveSummary_highestTotalAccountBalance_modifier",
        "experianBizJson_executiveSummary_medianCreditAmountExtended",
        "experianBizJson_insuranceDetails",
        "experianBizJson_scoreInformation_fsrScore_score",
        "experianBizJson_scoreInformation_fsrScore_modelCode",
        "experianBizJson_scoreInformation_fsrScore_riskClass_code",
        "experianBizJson_scoreInformation_fsrScore_riskClass_definition",
        "experianBizJson_scoreInformation_fsrScore_modelTitle",
        "experianBizJson_scoreInformation_fsrScore_probability",
        "experianBizJson_scoreInformation_fsrScore_profileNumber",
        "experianBizJson_scoreInformation_fsrScore_limitedProfile",
        "experianBizJson_scoreInformation_fsrScore_percentileRanking",
        "experianBizJson_scoreInformation_fsrScore_publiclyHeldCompany",
        "experianBizJson_scoreInformation_fsrScore_customerDisputeIndicator",
        "experianBizJson_scoreInformation_fsrScoreTrends",
        "experianBizJson_scoreInformation_commercialScore_score",
        "experianBizJson_scoreInformation_commercialScore_modelCode",
        "experianBizJson_scoreInformation_commercialScore_riskClass_code",
        "experianBizJson_scoreInformation_commercialScore_riskClass_definition",
        "experianBizJson_scoreInformation_commercialScore_modelTitle",
        "experianBizJson_scoreInformation_commercialScore_probability",
        "experianBizJson_scoreInformation_commercialScore_profileNumber",
        "experianBizJson_scoreInformation_commercialScore_limitedProfile",
        "experianBizJson_scoreInformation_commercialScore_customModelCode",
        "experianBizJson_scoreInformation_commercialScore_percentileRanking",
        "experianBizJson_scoreInformation_commercialScore_publiclyHeldCompany",
        "experianBizJson_scoreInformation_commercialScore_highCreditLimitAmount",
        "experianBizJson_scoreInformation_commercialScore_customerDisputeIndicator",
        "experianBizJson_scoreInformation_commercialScore_recommendedCreditLimitAmount",
        "experianBizJson_scoreInformation_fsrScoreFactors",
        "experianBizJson_scoreInformation_commercialScoreTrends",
        "experianBizJson_scoreInformation_commercialScoreFactors",
        "experianBizJson_uccFilingsDetail",
        "experianBizJson_collectionsDetail",
        "experianBizJson_consumerStatement",
        "experianBizJson_economicDiversity_sba8aIndicator",
        "experianBizJson_economicDiversity_hubZoneIndicator",
        "experianBizJson_economicDiversity_womenOwnedIndicator",
        "experianBizJson_economicDiversity_sbaCertifiedIndicator",
        "experianBizJson_economicDiversity_veteranOwnedIndicator",
        "experianBizJson_economicDiversity_disadvantagedIndicator",
        "experianBizJson_economicDiversity_minorityOwnedIndicator",
        "experianBizJson_economicDiversity_disabledVeteranOwnedIndicator",
        "experianBizJson_economicDiversity_historicalBlackCollegeAndUniversitiesIndicator",
        "experianBizJson_uccFilingsSummary_uccFilingsTrends",
        "experianBizJson_governmentActivity_activeDate",
        "experianBizJson_governmentActivity_terminationDate",
        "experianBizJson_leasingInformation",
        "experianBizJson_executiveInformation",
        "experianBizJson_experianBizUpdatedAt",
        "experianBizJson_monthlyPaymentTrends",
        "experianBizJson_corporateRegistration_agentName",
        "experianBizJson_corporateRegistration_profitFlag",
        "experianBizJson_corporateRegistration_statusFlag_code",
        "experianBizJson_corporateRegistration_statusFlag_definition",
        "experianBizJson_corporateRegistration_agentAddress_zip",
        "experianBizJson_corporateRegistration_agentAddress_city",
        "experianBizJson_corporateRegistration_agentAddress_state",
        "experianBizJson_corporateRegistration_agentAddress_street",
        "experianBizJson_corporateRegistration_businessType",
        "experianBizJson_corporateRegistration_federalTaxID",
        "experianBizJson_corporateRegistration_charterNumber",
        "experianBizJson_corporateRegistration_stateOfOrigin",
        "experianBizJson_corporateRegistration_incorporatedDate",
        "experianBizJson_corporateRegistration_recentFilingDate",
        "experianBizJson_corporateRegistration_existenceTermDate",
        "experianBizJson_corporateRegistration_statusDescription",
        "experianBizJson_corporateRegistration_existenceTermYears",
        "experianBizJson_corporateRegistration_originalFilingDate",
        "experianBizJson_corporateRegistration_domesticForeignIndicator",
        "experianBizJson_expandedCreditSummary_currentDbt",
        "experianBizJson_expandedCreditSummary_uccFilings",
        "experianBizJson_expandedCreditSummary_legalBalance",
        "experianBizJson_expandedCreditSummary_taxLienCount",
        "experianBizJson_expandedCreditSummary_judgmentCount",
        "experianBizJson_expandedCreditSummary_oldestUccDate",
        "experianBizJson_expandedCreditSummary_bankruptcyCount",
        "experianBizJson_expandedCreditSummary_collectionCount",
        "experianBizJson_expandedCreditSummary_ofacMatchWarning_code",
        "experianBizJson_expandedCreditSummary_ofacMatchWarning_definition",
        "experianBizJson_expandedCreditSummary_singleHighCredit",
        "experianBizJson_expandedCreditSummary_taxLienIndicator",
        "experianBizJson_expandedCreditSummary_allTradelineCount",
        "experianBizJson_expandedCreditSummary_collectionBalance",
        "experianBizJson_expandedCreditSummary_highestDbt6Months",
        "experianBizJson_expandedCreditSummary_judgmentIndicator",
        "experianBizJson_expandedCreditSummary_lowBalance6Months",
        "experianBizJson_expandedCreditSummary_monthlyAverageDbt",
        "experianBizJson_expandedCreditSummary_mostRecentUccDate",
        "experianBizJson_expandedCreditSummary_oldestTaxLienDate",
        "experianBizJson_expandedCreditSummary_highBalance6Months",
        "experianBizJson_expandedCreditSummary_oldestJudgmentDate",
        "experianBizJson_expandedCreditSummary_uccDerogatoryCount",
        "experianBizJson_expandedCreditSummary_allTradelineBalance",
        "experianBizJson_expandedCreditSummary_bankruptcyIndicator",
        "experianBizJson_expandedCreditSummary_highestDbt5Quarters",
        "experianBizJson_expandedCreditSummary_openCollectionCount",
        "experianBizJson_expandedCreditSummary_activeTradelineCount",
        "experianBizJson_expandedCreditSummary_oldestBankruptcyDate",
        "experianBizJson_expandedCreditSummary_oldestCollectionDate",
        "experianBizJson_expandedCreditSummary_tradeCollectionCount",
        "experianBizJson_expandedCreditSummary_currentAccountBalance",
        "experianBizJson_expandedCreditSummary_currentTradelineCount",
        "experianBizJson_expandedCreditSummary_mostRecentTaxLienDate",
        "experianBizJson_expandedCreditSummary_openCollectionBalance",
        "experianBizJson_expandedCreditSummary_mostRecentJudgmentDate",
        "experianBizJson_expandedCreditSummary_tradeCollectionBalance",
        "experianBizJson_expandedCreditSummary_averageBalance5Quarters",
        "experianBizJson_expandedCreditSummary_mostRecentBankruptcyDate",
        "experianBizJson_expandedCreditSummary_mostRecentCollectionDate",
        "experianBizJson_expandedCreditSummary_victimStatementIndicator",
        "experianBizJson_expandedCreditSummary_unsummarizedTradelineCount",
        "experianBizJson_expandedCreditSummary_collectionCountPast24Months",
        "experianBizJson_expandedCreditSummary_commercialFraudRiskIndicatorCount",
        "experianBizJson_industryPaymentTrends_sic",
        "experianBizJson_industryPaymentTrends_trends",
        "experianBizJson_contractSpendingDetail",
        "experianBizJson_quarterlyPaymentTrends",
        "experianBizJson_contractSpendingSummary",
        "experianBizJson_tradePaymentExperiences",
        "experianBizJson_proprietorNameAndAddress",
        "experianBizJson_commercialBankInformation",
        "experianBizJson_additionalPaymentExperiences",
        "experianBizJson_commercialFraudShieldSummary_ofacMatchWarning_code",
        "experianBizJson_commercialFraudShieldSummary_ofacMatchWarning_definition",
        "experianBizJson_commercialFraudShieldSummary_activeBusinessIndicator",
        "experianBizJson_commercialFraudShieldSummary_matchingBusinessIndicator",
        "experianBizJson_commercialFraudShieldSummary_businessRiskTriggersIndicator",
        "experianBizJson_commercialFraudShieldSummary_businessRiskTriggersStatement",
        "experianBizJson_commercialFraudShieldSummary_businessVictimStatementIndicator",
        "experianBizJson_commercialFraudShieldSummary_nameAddressVerificationIndicator",
        "experianBizJson_corporateFinancialInformation_currentDate",
        "experianBizJson_corporateFinancialInformation_balanceSheets",
        "experianBizJson_corporateFinancialInformation_fiscalYearEndDate",
        "experianBizJson_corporateFinancialInformation_operatingStatements",
        "experianBizJson_corporateFinancialInformation_criticalDataAndFinancialRatios",
        "experianBizJson_commercialGovernmentEntityCode",
        "ipt",
        "experianBizJson_annualSales",
        "experianBizJson_businessFacts_fileEstablishedFlag_code",
        "experianBizJson_businessFacts_fileEstablishedFlag_definition",
        "experianBizJson_paymentTotals_combinedTradelines_totalHighCreditAmount_modifier",
        "experianBizJson_paymentTotals_continuouslyReportedTradelines_totalHighCreditAmount_modifier",
        "experianBizJson_uccFilingsSummary_uccFilingsCount",
        "experianBizJson_enhancedBusinessDescription",
        "googlePlaceJson_name",
        "googlePlaceJson_rating",
        "googlePlaceJson_reviews",
        "googlePlaceJson_website",
        "googlePlaceJson_place_id",
        "googlePlaceJson_business_status",
        "googlePlaceJson_formatted_address",
        "googlePlaceJson_address_components",
        "googlePlaceJson_user_ratings_total",
        "googlePlaceJson_googlePlaceUpdatedAt",
        "assessment",
    ]

    main_df = pd.json_normalize(input, sep="_")

    for column_name in denorm_list_cols:
        if column_name not in main_df.columns:
            main_df[column_name] = None
    warnings.simplefilter(
        action="ignore", category=pd.errors.PerformanceWarning)

    main_df["dunsNumber"] = main_df["assessment_dunsNumber"]
    main_df["FSR Score"] = main_df["experianBizJson_scoreInformation_fsrScore_score"]
    main_df["FSR Risk Class"] = main_df[
        "experianBizJson_scoreInformation_fsrScore_riskClass_definition"
    ]
    main_df["Intelliscore Plus"] = main_df[
        "experianBizJson_scoreInformation_commercialScore_score"
    ]
    main_df["Intelliscore Risk Class"] = main_df[
        "experianBizJson_scoreInformation_commercialScore_riskClass_definition"
    ]
    main_df["Experian Credit Limit Recommendation"] = main_df[
        "experianBizJson_scoreInformation_commercialScore_recommendedCreditLimitAmount"
    ]
    main_df["D&B Credit Limit Recommendation"] = main_df[
        "assessment_maximumRecommendedCredit"
    ]

    main_df = main_df.reset_index(drop=True)
    main_df["last_review_time"] = None
    main_df["user_ratings_total"] = main_df["googlePlaceJson_user_ratings_total"]
    main_df["rating"] = main_df["googlePlaceJson_rating"]
    for index in range(len(main_df.index)):
        if not (main_df["googlePlaceJson_reviews"][index] != np.nan) or (
            main_df["googlePlaceJson_reviews"][index] != None
        ):
            if type(main_df["googlePlaceJson_reviews"][index]) == list:
                main_df["googlePlaceJson_reviews"][index] = main_df[
                    "googlePlaceJson_reviews"
                ][index][0]
                main_df["last_review_time"][index] = main_df["googlePlaceJson_reviews"][
                    index
                ]["time"]
                main_df["last_review_time"][index] = datetime.fromtimestamp(
                    main_df["last_review_time"][index]
                )
            else:
                main_df["googlePlaceJson_reviews"][index] = None

    main_df["naicsCodesExperian"] = main_df["experianBizJson_naicsCodes"]
    main_df["competitors"] = main_df["experianBizJson_competitors"]
    main_df["naicsCodesDnb"]=main_df["assessment_naicsCodes"]
    main_df = main_df[
        [
            "D&B Credit Limit Recommendation",
            "Experian Credit Limit Recommendation",
            "rating",
            "user_ratings_total",
            "dunsNumber",
            "last_review_time",
            "Intelliscore Risk Class",
            "Intelliscore Plus",
            "FSR Risk Class",
            "FSR Score",
            "assessment_annualSales",
            "assessment_maximumRecommendedCredit",
            "assessment_owedAmount",
            "assessment_highestCreditAmount",
            "assessment_daysBeyondPayments",
            "assessment_tradingExperienceCount",
            "assessment_tradingExperienceTotal",
            "assessment_totalPastDueBalanceOwned",
            "assessment_paydexScore",
            "assessment_delinquencyRawScore",
            "assessment_delinquencyScoreClass",
            "assessment_delinquencyScorePercentile",
            "assessment_delinquencyProbability",
            "experianBizJson_businessFacts_salesRevenue",
            "naicsCodesExperian",
            "naicsCodesDnb",
            "competitors",
            "bizLegalName",
        ]
    ]
    ept_col_list = []
    assessment_col_list = []
    ipt_col_list = []
    for col in main_df.columns:
        if col.find("ept") != -1:
            col_list = col.split("_")
            ept_col_list.append({col: col_list[1] + "_ept"})
        if col.find("ipt") != -1:
            col_list = col.split("_")
            ipt_col_list.append({col: col_list[1] + "_ipt"})
        if col.find("assessment") != -1:
            col_list = col.split("_")
            assessment_col_list.append({col: col_list[1]})
    for rename_col_dict in ept_col_list:
        main_df = main_df.rename(columns=rename_col_dict)
    for rename_col_dict in ipt_col_list:
        main_df = main_df.rename(columns=rename_col_dict)
    for rename_col_dict in assessment_col_list:
        main_df = main_df.rename(columns=rename_col_dict)

    final_df = main_df

    final_df["annualRevenue"] = np.nan
    output_dictionary = {}

    # Convert Datatype
    final_df["daysBeyondPayments"] = final_df["daysBeyondPayments"].fillna(0)
    final_df["totalPastDueBalanceOwned"] = final_df["totalPastDueBalanceOwned"].fillna(
        0
    )

    final_df["daysBeyondPayments"] = final_df["daysBeyondPayments"].astype(int)
    final_df["totalPastDueBalanceOwned"] = final_df["totalPastDueBalanceOwned"].astype(
        int
    )
    final_df["good_bad"] = np.where(
        (
            (final_df.loc[:, "daysBeyondPayments"] <= 10)
            | (final_df.loc[:, "totalPastDueBalanceOwned"] == 0)
        ),
        1,
        0,
    )

    final_df["annualRevenue"] = np.where(
        final_df["annualSales"].notnull()
        & final_df["experianBizJson_businessFacts_salesRevenue"].notnull(),
        final_df[["annualSales", "experianBizJson_businessFacts_salesRevenue"]]
        .astype(float)
        .max(axis=1),
        final_df["annualSales"].fillna(
            final_df["experianBizJson_businessFacts_salesRevenue"].astype(
                float)
        ),
    )

    column_list = [
        "owedAmount",
        "highestCreditAmount",
        "daysBeyondPayments",
        "tradingExperienceCount",
        "tradingExperienceTotal",
        "totalPastDueBalanceOwned",
        "paydexScore",
        "maximumRecommendedCredit",
        "delinquencyRawScore",
        "delinquencyScoreClass",
        "Intelliscore Plus",
        "Intelliscore Risk Class",
        "FSR Score",
        "FSR Risk Class",
        "Experian Credit Limit Recommendation",
        "D&B Credit Limit Recommendation",
        "dunsNumber",
        "annualRevenue",
        "good_bad",
        "rating",
        "last_review_time",
        "user_ratings_total",
        "naicsCodesExperian",
        "naicsCodesDnb",
        "competitors",
        "bizLegalName",
    ]
    # "desiredPaymentTerms","requestedLimit"]
    for column in column_list:
        if column not in final_df.columns:
            final_df[column] = None
    final_df = final_df[column_list]

    final_df = final_df.fillna(value="one")

    final_df = final_df.replace(r"^\s*$", np.nan, regex=True)
    final_df.replace("one", np.nan, inplace=True)
    final_df["paydexScore"] = final_df["paydexScore"].astype("float")
    convert_list = [
        "owedAmount",
        "highestCreditAmount",
        "paydexScore",
        "maximumRecommendedCredit",
        "delinquencyRawScore",
        "delinquencyScoreClass",
        "annualRevenue",
    ]
    for column in convert_list:
        final_df[column] = final_df[column].fillna(0)

    for column in convert_list:
        final_df[column] = final_df[column].astype("float")

    # Predict the credit risk score and level

    # Load the pickle file for testing
    with open("scorecard.pickle", "rb") as f2:
        scorecard_score = pickle.load(f2)

    # Sorting the co-efficiants
    scorecard_score = scorecard_score.tolist()

    intercept = scorecard_score[:1]
    my_array = scorecard_score[1:]
    # define the mask
    mask = [5, 4, 4, 5, 5, 5, 5, 5]

    sorted_array = []
    start_index = 0
    for m in mask:
        # get the subarray to sort based on the current mask value
        sub_array = my_array[start_index: start_index + m]
        start_index += m

        # sort the subarray based on the mask value and direction
        if m == 5:
            sub_array.sort(key=lambda x: x[0])
        else:
            sub_array.sort(key=lambda x: x[0], reverse=True)

        # add the sorted subarray to the output array
        sorted_array += sub_array

    scorecard_score = intercept + sorted_array

    woe_class = WoE_Binning()

    X_test_woe_transformed = woe_class.transform(final_df)

    # insert an Intercept column in its beginning to align with the # of rows in scorecard
    X_test_woe_transformed.insert(0, "Intercept", 1)
    X_test_woe_transformed.head()

    # matrix dot multiplication of test set with scorecard scores
    y_scores = X_test_woe_transformed.dot(scorecard_score)
    final_df["Credit Score"] = y_scores
    final_df["Credit Risk"] = 1

    rating = []
    for row in final_df["Credit Score"]:
        if row >= 300 and row <= 600:
            rating.append("HIGH")
        elif row > 600 and row <= 699:
            rating.append("MEDIUM")
        elif row >= 700:
            rating.append("LOW")

    final_df["Credit Risk"] = rating

    # calculate the max credit limit using dnB amd Xperian
    data = final_df.copy()
    convert_list_1 = [
        "Experian Credit Limit Recommendation",
        "D&B Credit Limit Recommendation",
    ]
    for column in convert_list_1:
        data[column] = data[column].fillna(0)

    for column in convert_list_1:
        data[column] = data[column].astype("float")

    max_loan_1 = []
    risk_level = data["Credit Risk"][0]
    creditscore = data["Credit Score"][0]
    loss_exp = creditscore / 900
    credit_limit_exp = 0
    credit_limit_dnb = 0
    if data["Experian Credit Limit Recommendation"][0] != None:
        credit_limit_exp = data["Experian Credit Limit Recommendation"][0]
    if data["D&B Credit Limit Recommendation"][0] != None:
        credit_limit_dnb = data["D&B Credit Limit Recommendation"][0]
    credit_limit = max(credit_limit_exp, credit_limit_dnb)

    loan = loss_exp * credit_limit
    if loan == 0:
        max_loan_1.append(0)

    else:
        max_loan_1.append(loan)

    data["maximum recommended amount"] = max_loan_1

    # bureau 1 result generation-->
    bureau_1_owed_amount = final_df['owedAmount'][0]
    bureau_1_daysBeyondPayments = final_df['daysBeyondPayments'][0]
    bureau_1_paydexScore = final_df["paydexScore"][0]
    bureau_1_delinquencyRawScore = final_df["delinquencyRawScore"][0]

    bureau_1_score, bureau_1_level, bureau_1_financial_advice = calculate_final_score_bureau_1(
        bureau_1_owed_amount, bureau_1_daysBeyondPayments, bureau_1_paydexScore, bureau_1_delinquencyRawScore)

    # bureau 2 result generation-->
    bureau_2_intelli = final_df['Intelliscore Plus'][0]
    bureau_2_fsr = final_df['FSR Score'][0]

    bureau_2_score, bureau_2_level, bureau_2_financial_advice = calculate_final_score_bureau_2(
        bureau_2_intelli, bureau_2_fsr)

    # bureau 3 result generation-->
    bureau_3_rating = final_df['rating'][0]
    bureau_3_score, bureau_3_level, bureau_3_financial_advice = calculate_final_score_bureau_3(
        bureau_3_rating)
    
    ml_risk_score_input=int(data["Credit Score"][0])

    try:
        input_df = get_company_info()
        mlDnbScoreP, mlExperianScoreP, mlGooglePlaceScoreP,mlRiskScoreP = percentile_scoring(
            input_df, bureau_1_score, bureau_2_score, bureau_3_score,ml_risk_score_input)
    except Exception as e:
        # Handle the error, you can print the error message for debugging purposes
        print(f"An error occurred: {e}")
        # Assign 0.0 to the scores
        mlDnbScoreP, mlExperianScoreP, mlGooglePlaceScoreP ,mlRiskScoreP = 0.0, 0.0, 0.0 , 0.0


    # Getting the DNB and Experian NAICS code list
    # Clean DNB naics
    final_df['naicsCodesDnb'] = final_df['naicsCodesDnb'].fillna("")

    try:
        naics_dnb_list = final_df['naicsCodesDnb'].apply(lambda x: extract_codes_dnb(x))
    except Exception as e:
        naics_dnb_list = []

    # Clean Experian naics
    try:
        naics_experian_list = final_df['naicsCodesExperian'].apply(lambda x: extract_codes_experian(x))
    except Exception as e:
        naics_experian_list = []

    # Combined Experian naics
    combined_naics_list = naics_experian_list + naics_dnb_list

    # Get Peer Information
    try:
        getPeerScoreInfo = getPeersScore(combined_naics_list, input_df)
    except Exception as e:
        getPeerScoreInfo = []  


    # return the output dict
    output_dictionary = {
        "mlRiskLevel": data["Credit Risk"][0],
        "mlCreditLimit": int(data["maximum recommended amount"][0]),
        "mlRiskScore": int(data["Credit Score"][0]),
        "mlRiskScorePercentile":mlRiskScoreP,
        "mlPaymentTerms": "NET_30",
        "mlScoreDate": current_date.isoformat()[:-3] + "Z",
        "mlDnbScore": bureau_1_score,
        "mlDnbScorePercentile": mlDnbScoreP,
        "mlDnbLevel": bureau_1_level,
        "mlDnbFinAdvice": bureau_1_financial_advice,
        "mlExperianScore": bureau_2_score,
        "mlExperianScorePercentile": mlExperianScoreP,
        "mlExperianLevel": bureau_2_level,
        "mlExperianFinAdvice": bureau_2_financial_advice,
        "mlGooglePlaceScore": bureau_3_score,
        "mlGooglePlaceScorePercentile": mlGooglePlaceScoreP,
        "mlGooglePlaceLevel": bureau_3_level,
        "mlGooglePlaceFinAdvice": bureau_3_financial_advice,
        "mlpeerScoreInfo":getPeerScoreInfo

    }

    input.update(output_dictionary)
    return input


def getPeersForCompany(input):
    """
    input args:
    ==========
        dunsNumber :
        compareBy : "NAICS_CODES" or "INDUSTRY"

    output ex:
    ==========
        {
          "bizLegalName": "legion solar power inc",
          "naicsFullDescription": [
            {
              "code": "\"238210\"",
              "description": "electrical contractors and other wiring installation contractors"
            },
            {
              "code": "\"238220\"",
              "description": "plumbing, heating, and air-conditioning contractors"
            },
            {
              "code": "\"221114\"",
              "description": "solar electric power generation"
            }
          ],
          "peers": [
            {
              "bin": "507951958",
              "bizLegalName": "lucid solar inc",
              "city": "NEWPORT BEACH",
              "country": "US",
              "dunsNumber": "118481623",
              "mlCreditLimit": 35100.0,
              "mlRiskScore": 702,
              "naicsFullDescription": "[{}]",
              "state": "CA"
            },
            {
              "bin": "473297642",
              "bizLegalName": "smart wave solar, l.l.c",
              "city": "BLUFFDALE",
              "country": "US",
              "dunsNumber": "114401253",
              "mlCreditLimit": 540778.0,
              "mlRiskScore": 895,
              "naicsFullDescription": "[{\"code\": \"423720\", \"definition\": \"Plumbing and Heating Equipment and Supplies (Hydronics) Merchant Wholesalers\"}, {\"code\": \"238220\", \"definition\": \"Plumbing, Heating, and Air-Conditioning Contractors\"}, {\"code\": \"423620\", \"definition\": \"Household Appliances, Electric Housewares, and Consumer Ele ctronics Merchant Wholesalers\"}]",
              "state": "UT"
            },
            {
              "bin": "508522079",
              "bizLegalName": "evolution solar llc",
              "city": "WASHINGTON",
              "country": "US",
              "dunsNumber": "118081982",
              "mlCreditLimit": 24733.0,
              "mlRiskScore": 636,
              "naicsFullDescription": "[{}]",
              "state": "DC"
            },
            {
              "bin": "411758678",
              "bizLegalName": "orr protection systems, inc",
              "city": "LOUISVILLE",
              "country": "US",
              "dunsNumber": "064849110",
              "mlCreditLimit": 134741.0,
              "mlRiskScore": 475,
              "naicsFullDescription": "[{\"code\": \"238220\", \"definition\": \"Plumbing, Heating, and Air-Conditioning Contractors\"}, {\"code\": \"238990\", \"definition\": \"All Other Specialty Trade Contractors\"}, {\"code\": \"423810\", \"definition\": \"Construction and Mining (except Oil Well) Machinery and Equipment Merchant Wholesalers\"}]",
              "state": "KY"
            }
          ]
        }

    """
    current_date = datetime.utcnow()

    # print(input)
    dunsNum = input["dunsNumber"]
    compareBy = input["compareBy"]

    all_peers = {}
    peer_list = []
    peerScore_list = []
    peerCreditLimit_list = []
    output_dictionary = {}
    tmp_df = pd.DataFrame()
    result = pd.DataFrame()

    # read CompanyScore table
    query = """SELECT bizLegalName , dunsNumber, bin, city, state, country, mlRiskScore , mlCreditLimit ,
        JSON_EXTRACT(dnbJson, '$."lineOfBusiness"') as "dnb_lineBusiness",
        JSON_EXTRACT(dnbJson, '$."naicsFullDescripion"') as "dnb_industry" ,
        JSON_EXTRACT(dnbJson, '$."naicsCodes"') as "dnb_naics" ,
        JSON_EXTRACT(experianBizJson, '$."naicsCodes"[*]."code"') as "experian_naics",
        JSON_EXTRACT(experianBizJson, '$."naicsCodes"[*]."definition"') as "experian_industry" ,
        JSON_EXTRACT(dnbJson, '$."naicsFullDescripion"') as "dnb_naicsFullDescripion" ,
        JSON_EXTRACT(experianBizJson, '$."naicsCodes"') as "experian_naicsFullDescripion"
        from  CompanyScore ;"""
    compScore_df = run_query(query)
    # clean the bizLegalName
    compScore_df["bizLegalName"] = [x.strip()
                                    for x in compScore_df["bizLegalName"]]
    compScore_df["bizLegalName"] = [
        str(x).strip(".").lower() for x in compScore_df["bizLegalName"]
    ]

    # clean DnB naics
    compScore_df["dnb_naics"] = [clean_dnb_naics(
        x) for x in compScore_df["dnb_naics"]]

    # clean Experian naics
    compScore_df["experian_naics"] = [
        clean_exp_naics(x) for x in compScore_df["experian_naics"]
    ]

    # insert DnB naics value if experian naics is NULL
    compScore_df.experian_naics.fillna(compScore_df.dnb_naics, inplace=True)
    compScore_df["experian_naics"] = [
        func_strip(x) for x in compScore_df["experian_naics"]
    ]

    # Clean the dnb industry by extracting naics description
    compScore_df["dnb_industry"] = compScore_df["dnb_industry"].apply(
        json.loads)
    compScore_df["dnb_industry"] = [
        extract_industry(x) for x in compScore_df["dnb_industry"]
    ]

    # clean Experian industry
    compScore_df["experian_industry"] = [
        clean_exp_industry(x) for x in compScore_df["experian_industry"]
    ]

    # insert DnB Industry value if experian industry is NULL
    compScore_df.experian_industry.fillna(
        compScore_df.dnb_industry, inplace=True)

    # insert DnB FullDescripion value if experian FullDescripion is NULL
    compScore_df.experian_naicsFullDescripion.fillna(
        compScore_df.dnb_naicsFullDescripion, inplace=True
    )
    compScore_df["experian_naicsFullDescripion"] = compScore_df[
        "experian_naicsFullDescripion"
    ].str.replace("definition", "description")

    # Fetch the input CP data
    cp_data_df = compScore_df.loc[compScore_df["dunsNumber"] == dunsNum]

    cp_name = cp_data_df["bizLegalName"].values[0]
    # print(cp_name)

    cp_NailsFullDesc = (
        cp_data_df.loc[:, ["experian_naics", "experian_industry"]]
        .explode(["experian_naics", "experian_industry"])
        .rename(columns={"experian_naics": "code", "experian_industry": "description"})
        .to_dict("records")
    )
    # print(cp_NailsFullDesc)

    cp_naics_list = cp_data_df["experian_naics"].values[0]
    # print(cp_naics_list)

    cp_industry_list = cp_data_df["experian_industry"].values[0]

    # Fetch COMPANYSCORE table except input CP, and make backup column for industry
    compScore_without_cp = compScore_df.loc[compScore_df["dunsNumber"] != dunsNum]

    # Explode the NAICS
    compScore_without_cp_exp = compScore_without_cp.explode(
        ["experian_naics", "experian_industry"]
    ).reset_index()
    compScore_without_cp_exp = compScore_without_cp_exp.drop(columns=["index"])

    # print(compScore_without_cp_exp)
    print(cp_naics_list,type(cp_naics_list))
    
    # match for naics in the compScore table
    if compareBy == "NAICS_CODES":
        for naic in cp_naics_list:
            # print(naic)
            tmp_df = compScore_without_cp_exp.loc[
                compScore_without_cp_exp["experian_naics"] == naic
            ]
            # print("\n")
            if not tmp_df.empty:
                result = pd.concat([result, tmp_df])

    # match for industry in the compScore table
    if compareBy == "INDUSTRY":
        for indus in cp_industry_list:
            # print(indus)
            tmp_df = compScore_without_cp_exp.loc[
                compScore_without_cp_exp["experian_industry"] == indus
            ]
            # print("\n")
            if not tmp_df.empty:
                result = pd.concat([result, tmp_df])
                # print(result.shape)

    select_keys = [
        "bizLegalName",
        "city",
        "state",
        "country",
        "mlRiskScore",
        "mlCreditLimit",
        "dunsNumber",
        "bin",
        "experian_naicsFullDescripion",
    ]
    if not result.empty:
        result = result.loc[:, select_keys].rename(
            columns={"experian_naicsFullDescripion": "naicsFullDescription"}
        )
        result = result.drop_duplicates()
        peer_list = result.to_dict("records")

    # FInd Average credit limit of all peers
    # all_peers["peers"] = peer_list
    # peerCreditLimit_list = glom(all_peers, ("peers", ["mlCreditLimit"]))
    # if len(peerCreditLimit_list) != 0 and peerCreditLimit_list[0]:
    #     all_peers["avgMlCreditLimit"] = round(
    #         sum(peerCreditLimit_list) / len(peerCreditLimit_list), 2
    #     )

    # ##  FInd Average credit score of all peers
    # peerScore_list = glom(all_peers, ("peers", ["mlRiskScore"]))
    # if len(peerScore_list) != 0 and peerScore_list[0]:
    #     all_peers["avgMlRiskScore"] = round(
    #         sum(peerScore_list) / len(peerScore_list), 2
    #     )

    # print("########")
    # print(all_peers)
    # print("########")

    output_dictionary["bizLegalName"] = cp_name
    output_dictionary["naicsFullDescription"] = cp_NailsFullDesc
    output_dictionary["peers"] = peer_list
    input.update(output_dictionary)

    return input
