#%%
from bs4 import BeautifulSoup

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import requests
import pandas as pd
import json 

def dates():
    one_month = relativedelta(months=1)
    date = datetime(2021, 2, 25)
    while date.year>=2010:
        yield date
        date=date-one_month 


def get_archived_snapshot(timestamp):

    print('Procurando ', timestamp.strftime('%b %Y'))
    url = 'http://archive.org/wayback/available?url=https://store.steampowered.com/hwsurvey/videocard&timestamp={}'.format(timestamp.strftime('%Y%m%d%H%H%H'))
    wayback = requests.get(url).json()
    url = None
    if wayback['archived_snapshots']:
        try:
            url = wayback['archived_snapshots']['closest']['url']    
            print('Archived Snapshot achado: {}'.format(url))
        except:
            print('ERRO SNAPSHOT: {}'.format(wayback['archived_snapshots']))
    else:
        print('Não foi encontrado nenhum snapshot:\n{}'.format(wayback))
    return url

def get_table_from_snapshot(snapshot_link, timestamp):
    html = requests.get(snapshot_link).content
    soup = BeautifulSoup(html, 'html.parser')

    div_table = soup.find('div', id='sub_stats')

    table_list=[]

    columns = []
    row = {}
    amount=0
    for sub in div_table.find_all():
        if sub.name=='br' and sub.attrs['clear']=='all' and len(columns)!=0:
            columns.append('VARIACAO')
            survey_date = soup.find('h1').string.split(': ')[-1]
            dict_video_cards = {'survey_date':survey_date, 'timestamp':timestamp, 'data':row, 'columns':columns}
            #table_list.append(pd.DataFrame.from_dict(row, orient='index', columns=columns))
            table_list.append(dict_video_cards)
            columns = []
            row = {}

        if 'class' in sub.attrs and 'col_header' in sub['class']:
            amount=0
            columns.append(sub.string)
        elif 'class' in sub.attrs and 'substats_row' in sub['class']:
            row_data = list(filter(None, sub.get_text().split('\n')))
            row[amount] = row_data

            amount+=1

    print('Achou {} placas'.format(len(table_list[1]['data'])))
    return table_list[1]



''' ###Tentativa de pegar pelo texto
def get_text_page(snapshot_link):
    html = requests.get(snapshot_link).content
    soup = BeautifulSoup(html, 'html.parser')

    div_table = soup.find('div', id='sub_stats')
    tabelas_string = div_table.get_text().split('\n\n\n\n\n')
    tabela_video_cards = tabelas_string[1].split('\xa0\n')

archived_snapshot = 'http://web.archive.org/web/20171013075745/http://store.steampowered.com:80/hwsurvey/videocard'
teste = get_text_page(archived_snapshot)
for t in teste:
    print("'"+t+"'")
teste
#'''
#%%

df_list = []
for timestamp in dates():
    month = timestamp.month
    while True:
        archived_snapshot = get_archived_snapshot(timestamp)
        
        dict_survey = None
        if archived_snapshot:
            dict_survey = get_table_from_snapshot(archived_snapshot, timestamp)
        
        if dict_survey and len(dict_survey['data'])>1:
            break
        
        #Quando não encontra vai voltando de 2 em 2 dias até achar ou sair do mês
        timestamp-=relativedelta(days=2)
        print('Não foi possível encontrar esse dia!')
        if timestamp.month!=month:
            print('NÃO FOI ACHADO ESSE MÊS')
            dict_survey = None 
            break 
    df_list.append(dict_survey)

#%% Testando dataframes
d = -55
if df_list[d]:
    print(df_list[d]['survey_date'] )
    df = pd.DataFrame.from_dict(df_list[d]['data'], orient='index', columns=df_list[d]['columns'])
else:
    print('Sem dado')
df 

#%%

for d in df_list:
    if d:
        print(d['survey_date'], d['timestamp'])
    else:
        print('Sem dado!')
#%%


#%%
df_complete = pd.DataFrame(columns=['ALL VIDEO CARDS'])
for survey in df_list[1:]:
    if not survey:
        continue
    date = survey['survey_date']
    timestamp = survey['timestamp']
    survey = pd.DataFrame.from_dict(survey['data'], orient='index', columns=survey['columns'])
    last_month_column = survey.columns[-2]
    
    df_last_month = survey[['ALL VIDEO CARDS', last_month_column]]
    df_last_month.rename(columns={last_month_column: date}, inplace=True)
    df_complete = pd.merge(df_complete, df_last_month, how='outer', on='ALL VIDEO CARDS')

    print(date, timestamp)
df_complete

#%%
df_complete.to_csv('placa.csv', index=False)