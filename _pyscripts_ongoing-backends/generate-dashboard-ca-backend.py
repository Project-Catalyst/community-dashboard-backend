'''
this file was developed to run on >> Project-Catalyst/ca-tool-backend

important! :
    - this file reads data from https://github.com/Project-Catalyst/feedback-challenge-tool-backend/tree/master/data
        Code errors and data inconsistencies may happen if the repository is not updated accordingly to the API data requested in this script.
'''

import argparse
import json
import pandas as pd
import requests
from urllib.request import urlopen
from github import Github

# data paths
SCRIPT_URL = 'https://github.com/Project-Catalyst/ca-tool-backend/blob/master/generate-dashboard-ca-backend.py'
TEMPLATE_FILE_PATH = 'https://raw.githubusercontent.com/Project-Catalyst/feedback-challenge-tool-backend/master/data/{}/proposals.json'


'''
IDEASCALE API METHODS
    - Fetch https://github.com/Project-Catalyst/ca-tool-backend/blob/master/proposals.json directly from IdeaScale API
    - The methods below access the IdeaScale API to collect the current number of assessments per proposal
    considering the ongoing funding process
    - The methods were imported from https://github.com/Project-Catalyst/ca-tool-backend/blob/master/update-assessments-count.py
'''
def loadOptions(goptions = {}):
    try:
        with open('./dash-options.json', 'r') as f:
            options = json.load(f)
            for k in options:
                goptions[k] = options[k]
    except Exception as e:
        print(e)
        print("Error loading options.json")
    return goptions

def getAssessmentsCount(goptions):
    headers = {
        'api_token': goptions["ideascale_api_token"]
    }
    ideas = []
    for funnelStage in goptions["assess_funnel_stage_ids"]:
        url = goptions["ideascale_base_api_url"] +             goptions["assess_funnel_endpoint"].format(funnelStage)

        print("Requesting url: {}".format(url))
        r = requests.get(url, headers=headers)
        response = r.json()
        for idea in response:
            ideas.append({
                "id": idea['ideaId'],
                "assessments_count": idea['noOfAccessors']
            })
    return ideas

'''
DATA PROCESS
'''
def getTemplateData(fund):
    '''
    Proposal template
    
    Generates formatted pd.DataFrame containing all proposals and relevant information
        assessments_count feature (number of assessments received by a proposal) is initalized to zero
        reads data from Project-Catalyst/feedback-challenge-tool-backend repo
    '''  
    # collecting data from url
    url = TEMPLATE_FILE_PATH.format(fund)
    print('getTemplateData: ', url)
    response = urlopen(url)
    proposals = json.loads(response.read())
    
    df = pd.json_normalize(proposals)
    df = df[['id', 'category']].copy()
    df['assessments_count'] = 0
    df = df.sort_values('id', axis='index').reset_index(drop=True)
    df.rename(columns={'id':'proposal_id', 'category':'challenge_id'}, inplace=True)
    df.set_index('proposal_id', inplace=True)
    df.sort_index(inplace=True)
    return df

def formatAssessmentsCount(api_resp):
    '''
    Assessments counting by Proposal 
    
    Generates formatted pd.DataFrame containing ongoing assessments' count by proposal 
        reads data from IdeaScale API
        receives json formatted obj
    '''
    # format data from backend api request
    df = pd.json_normalize(api_resp)
    df.rename(columns={'id':'proposal_id'}, inplace=True)
    df.set_index('proposal_id', inplace=True)
    df.sort_index(inplace=True)
    return df

def generateJson(data, count):
    '''
    < Proposals by Challenge > assessments' counting FINAL
    
    Generates the .json formatted output data
        Adds the IdeaScaleAPI count to the Proposal Template dataframe
        Transforms the dataframe into json-dump object
    '''
    
    # update counting in the template data DataFrame
    data['assessments_count'] = count['assessments_count']
    data.reset_index(inplace=True)
    
    # pack updated data into json format
    df_temp = data.groupby('challenge_id')[['proposal_id','assessments_count']].apply(lambda x: x.to_dict(orient='records'))
    df_temp = df_temp.reset_index()
    df_temp.rename(columns={0:'proposals'}, inplace=True)
    return df_temp.to_dict(orient='records')

def getStaticResp():
    # read outfile from ca-tool-backend API request
    url = "https://raw.githubusercontent.com/Project-Catalyst/ca-tool-backend/master/proposals.json"
    response = urlopen(url)
    return json.loads(response.read())    

def main():    
    
    # load number of asessments by proposal from ideascale api    
    goptions = loadOptions()
    api_resp = getAssessmentsCount(goptions)
    # api_resp = getStaticResp()
    
    # configs
    fund = goptions["fund"]
    outfile = goptions["outfile_dashboard_backend_repo"]

    if (len(api_resp)):
        data = getTemplateData(fund)
        count = formatAssessmentsCount(api_resp)
                
        json_data = generateJson(data, count)
        
        # push GitHub update
        try:
            g = Github(goptions['github_access_token'])  
            repo = g.get_repo(goptions['github_dashboard_backend_repo'])
            contents = repo.get_contents(outfile)
            with open(outfile.split('/')[1], 'w') as f:  # save local files 
                json.dump(json_data, f, indent=2)
            commit_txt = 'Fund={} snapshot: generated from {}'.format(fund, SCRIPT_URL)
            repo.update_file(contents.path, commit_txt, json.dumps(json_data), contents.sha)
        except Exception as e:
            print(e)

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Script to generate Project-Catalyst/community-dashboard-backend/snapshots/ca-backend_snapshot.json')
    main()