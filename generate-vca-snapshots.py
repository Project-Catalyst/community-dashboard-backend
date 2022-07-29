'''
this file was developed to run on >> Project-Catalyst/vca-tool-backend

important! :
    - this file reads data from 
        CA_TEMPLATE_DATA_PATH = 'https://raw.githubusercontent.com/Project-Catalyst/community-dashboard-backend/main/snapshots/ca-backend_snapshot.json'
        ASSESSMENTS_KEYS_PATH = 'https://raw.githubusercontent.com/Project-Catalyst/vca-tool/master/src/assets/data/assessments.csv'
    - Code errors and data inconsistencies may happen if the repositories are not concisely updated
'''

import argparse
import json
import pandas as pd
from urllib.request import urlopen
# from github import Github

OUTFILE = 'snapshots/vca-backend_snapshot.json'

# data paths
SCRIPT_URL = 'https://github.com/Project-Catalyst/vca-tool-backend/blob/master/generate-dashboard-vca-backend.py'
DASHBOARD_REPO = 'https://github.com/Project-Catalyst/community-dashboard-backend'
VCA_BACKEND_PATH = "https://vca-backend.herokuapp.com/"
CA_TEMPLATE_DATA_PATH = 'https://raw.githubusercontent.com/Project-Catalyst/community-dashboard-backend/main/snapshots/ca-backend_snapshot.json'
ASSESSMENTS_KEYS_PATH = 'https://raw.githubusercontent.com/Project-Catalyst/vca-tool/master/src/assets/data/assessments.csv'


'''
GITHUB ACCESS FUNCTIONS
'''
def getTemplateFund():
    # read latest Github folder name from TEMPLATE_FOLDER_PATH
    return 'f9'

def loadOptions(goptions = {}):
    try:
        with open('./options.json', 'r') as f:
            options = json.load(f)
            for k in options:
                goptions[k] = options[k]
    except Exception as e:
        print(e)
        print("Error loading options.json")
    return goptions

'''
DATA PROCESS
'''
def getReviewsCount():
    # read data from backend
    response = urlopen(VCA_BACKEND_PATH)
    return json.loads(response.read())

def formatReviewsCount(json_loads):
    df_assess_count = pd.Series(json_loads, name='vca_reviews_count')
    df_assess_count.index.name = 'assessment_id'
    df_assess_count = df_assess_count.reset_index()
    df_assess_count['assessment_id'] = df_assess_count['assessment_id'].astype(int)
    return df_assess_count.set_index('assessment_id')

def getTemplateData():
    # read most recent ca-data from dashboard 
    # with open(CA_TEMPLATE_DATA_PATH, 'r') as f:
    #     ca_data = json.load(f)
    response = urlopen(CA_TEMPLATE_DATA_PATH)
    ca_data = json.loads(response.read())

    # df: challenge_id by json-packed proposals (id & assessments_count)
    df_ch_by_prop = pd.json_normalize(ca_data)

    # df: proposal_id, challenge_id, assessments_count
    df_ca = pd.concat([ pd.concat( # unpack proposals by challenge into dataframe with challenge_id column
                                [ pd.Series([df_ch_by_prop['challenge_id'].loc[i]]*len(df_ch_by_prop['proposals'].loc[i]), name='challenge_id'),
                                  pd.json_normalize(df_ch_by_prop['proposals'].loc[i])], 
                                axis='columns')
                        for i in range(df_ch_by_prop.shape[0])],
                        axis='index')
    df_ca.set_index('proposal_id', inplace=True)
    return df_ca.sort_index()

def getIdKeyTable():
    # read assessments data from vca-tool repository
    assessments = pd.read_csv(ASSESSMENTS_KEYS_PATH)
    df_keys = assessments[['id','proposal_id']].copy()
    df_keys.rename(columns={'id':'assessment_id'}, inplace=True)
    return df_keys.groupby('proposal_id')['assessment_id'].apply(lambda x: x.to_list()).reset_index().set_index('proposal_id')

def generateJson(data, count):
    '''
    < Proposals by Challenge > assessments' counting FINAL
    
    Generates the .json formatted output data
        Adds the IdeaScaleAPI count to the Proposal Template dataframe
        Transforms the dataframe into json-dump object
    '''
    
    # table mapping proposal_id(index) to list of assessment_id
    keys = getIdKeyTable()
    
    # adds "assessments" column to < data > : packs < count > into json format (for list of assessment_id related to proposal_id)
    get_assessments = lambda proposal_id: count.loc[keys.loc[proposal_id].item()].sort_index().reset_index().to_dict(orient='records')
    new_data = data.copy()
    new_data['assessments'] = pd.Series(map(get_assessments, data.index), index=data.index)
    new_data.reset_index(inplace=True)
    
    # pack updated data into json format
    
    df_temp = new_data.groupby('challenge_id')[['proposal_id','assessments_count','assessments']].apply(lambda x: x.to_dict(orient='records'))
    df_temp = df_temp.reset_index()
    df_temp.rename(columns={0:'proposals'}, inplace=True)
    return df_temp.to_dict(orient='records')
   
def main(fund):    
    
    # load number of reviews by assessment from vca-tool-backend api    
    api_resp = getReviewsCount()
    
    if (len(api_resp)):
        
        count = formatReviewsCount(api_resp)
        data = getTemplateData()
                        
        json_data = generateJson(data, count)
        # goptions = loadOptions()  # (here-below) uncomment to insert Github functionality
        
        # push GitHub update
        try:
            # g = Github(goptions['github_access_token'])    
            # repo = g.get_repo(DASHBOARD_REPO)  
            # contents = repo.get_contents(OUTFILE)
            with open(OUTFILE, 'w') as outfile:  # save local files 
                json.dump(json_data, outfile, indent=2)
            # commit_txt = 'Fund={} snapshot: generated from {}'.format(fund, SCRIPT_URL)
            # repo.update_file(contents.path, commit_txt, json.dumps(json_data), contents.sha)
        except Exception as e:
            print(e)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Script to generate Project-Catalyst/community-dashboard-backend/snapshots/vca-backend_snapshot.json')
    fund = getTemplateFund()
    main(fund)

