import argparse
from datetime import datetime
import json
import pandas as pd
import pathlib
from urllib.request import urlopen

DATESTAMP =  datetime.now().strftime('%Y%m%d')

# repository's paths
_ROOT = 'community-dashboard-backend'
_MASK_PATH = str(pathlib.Path(__file__).absolute()).split(_ROOT)[0] 
SCRIPT_PATH = str(pathlib.Path(__file__).absolute()).replace(_MASK_PATH,'')
ROOT_PATH = _MASK_PATH + _ROOT + '/'
HIST_SNAPSHOT_PATH = ROOT_PATH+'historical_snapshots/full_snapshot_{}.json'
HIST_METADATA_PATH = ROOT_PATH+'historical_snapshots/metadata.json'
HIST_METADATA_FUND_PATH = ROOT_PATH+'historical_snapshots/metadata_{}.json'

# data paths
TEMPLATE_PATH = 'https://raw.githubusercontent.com/Project-Catalyst/feedback-challenge-tool-backend/master/data/{}/proposals.json'
XLSX_PATHS = {
    'f6': 'historical_raw_data/vCA Aggregated - Fund 6.xlsx',
    'f7': 'historical_raw_data/vCA Aggregated - Fund 7.xlsx',
    'f8': 'historical_raw_data/vCA Aggregated - Fund 8 (Final MVP candidate).xlsx'
}
SHEETS = {
    'assessments_count': 'Proposals scores',
    'reviews_count': 'vCA Aggregated'
}


## Fetch data from vCA-Aggregated xslx file
def getAssessmentsCount(fund, sheet):
    if fund=='f6':
        df = sheet[['proposal_id', '# of valid reviews']].copy()
        df.rename(columns={'# of valid reviews':'assessments_count'}, inplace=True)
    else:
        df = sheet[['proposal_id', 'No. Assessments']].copy()
        df.rename(columns={'No. Assessments':'assessments_count'}, inplace=True)
    return df.astype(int).set_index('proposal_id').sort_index()

def getReviewsCount(fund, sheet):
    df = sheet[['id','proposal_id', '# of vCAs Reviews']].copy()
    df.rename(columns={'id':'assessment_id', '# of vCAs Reviews': 'vca_reviews_count'}, inplace=True)
    return df.astype(int)

## DATA PROCESS
def getTemplateData(fund):
    '''
    Proposal template
    
    Generates formatted pd.DataFrame containing all proposals and relevant information
        assessments_count feature (number of assessments received by a proposal) is initalized to zero
        reads data from Project-Catalyst/feedback-challenge-tool-backend repo
    '''
    # collecting data from url
    try:
        url = TEMPLATE_PATH.format(fund)
        response = urlopen(url)
    except:
        raise ValueError('urllib.request.urlopen ERROR:\nurl={}'.format(url))
    proposals = json.loads(response.read())

    df = pd.json_normalize(proposals)
    df = df[['id', 'category']].copy()
    df['assessments_count'] = 0
    df = df.sort_values('id', axis='index').reset_index(drop=True)
    df.rename(columns={'id':'proposal_id', 'category':'challenge_id'}, inplace=True)
    df.set_index('proposal_id', inplace=True)
    df.sort_index(inplace=True)
    return df

def updateAssessmentsCount(data, count):
    new_data = data.copy()
    new_data['assessments_count'] = count['assessments_count']
    return new_data.reset_index()

def updateReviewsCount(data, count):
    new_data = data.set_index('proposal_id')
    new_data['assessments'] = count.groupby('proposal_id')[['assessment_id','vca_reviews_count']].apply(lambda x: x.to_dict(orient='records'))                                    .reset_index()                                    .set_index('proposal_id').sort_index().rename(columns={0: 'assessments'})
    return new_data.reset_index()

def generateJson(data):
    df = data.groupby('challenge_id')[['proposal_id','assessments_count','assessments']].apply(lambda x: x.to_dict(orient='records')).reset_index().rename(columns={0:'proposals'})
    return df.to_dict(orient='records')

def get_metadata(fund):
    metadata = {
        'origin_script': SCRIPT_PATH,
        'outfile': HIST_SNAPSHOT_PATH.format(fund).replace(_MASK_PATH,''),
        'datestamp': DATESTAMP,
        'data_sources': {
            'templateData': TEMPLATE_PATH.format(fund),
            'catalystData': {
                'xlsx': _ROOT+'/'+XLSX_PATHS[fund],
                'sheet_assessments_count': SHEETS['assessments_count'],
                'sheet_reviews_count': SHEETS['reviews_count']
            }
        }
    }
    return metadata

def run_single_fund(fund, save_metadata=True):
    print('\nprocessing fund {}'.format(fund))

    # load number of asessments by proposal from xls files
    xlsx_obj = pd.ExcelFile(XLSX_PATHS[fund])
    df_assess_count = getAssessmentsCount(fund, xlsx_obj.parse(sheet_name=SHEETS['assessments_count']))
    df_review_count = getReviewsCount(fund, xlsx_obj.parse(sheet_name=SHEETS['reviews_count']))

    # update proposal's assessments count
    template = getTemplateData(fund)
    data = updateAssessmentsCount(template, df_assess_count)
    data = updateReviewsCount(data, df_review_count)

    json_data = generateJson(data)

    # save data snapshot
    path = HIST_SNAPSHOT_PATH.format(fund)
    with open(path, 'w') as f:
        json.dump(json_data, f, indent=2)
    print('...saved:', path.replace(_MASK_PATH,''))
    
    metadata = get_metadata(fund)
    if save_metadata:
        path = HIST_METADATA_FUND_PATH.format(fund)
        with open(path, 'w') as f:
            json.dump(metadata, f, indent=2)
        print('...saved: ', path.replace(_MASK_PATH,''))
    else:
        return metadata
    
def run_all_funds():    
    print('>> generate historical_snapshots for funds {}'.format(list(XLSX_PATHS.keys())))
    metadatas = []
    # available funds in Project-Catalyst/feedback-challenge-tool-backend (used as base-template data)
    for fund in XLSX_PATHS.keys(): 
        meta = run_single_fund(fund, save_metadata=False)
        metadatas.append(meta)
        
    path = HIST_METADATA_PATH
    with open(path, 'w') as f:
        json.dump(metadatas, f, indent=2)
    print('\n> saved: ', path.replace(_MASK_PATH,''))
    
    return

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Script to generate historical_snapshots.')
    parser.add_argument("--fund", type=str,
                        default='',
                        help="Specific fund to generate historical_snapshots/full_snapshot_{fund}.json")
    args = parser.parse_args()
    
    if args.fund:
        if args.fund in XLSX_PATHS.keys():
            run_single_fund(args.fund)
        else:
            raise ValueError('Unidentified value --fund. Use one of {}'.format(list(XLSX_PATHS.keys())))
    else:
        run_all_funds()

