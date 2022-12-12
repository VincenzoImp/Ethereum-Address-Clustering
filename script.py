import pandas as pd
from web3 import Web3
from web3.middleware import geth_poa_middleware
from multiprocessing import Pool
from tqdm import tqdm
import csv

chain = 'eth'
query_mode = 'rpc'
core_number = 20
chunk_size = 1000
w3 = None
data = {
    'eth': {
        'rpc': '/eth/eth_node/node/geth.ipc',
        'http': 'https://mainnet.infura.io/v3/e0a4e987f3ff4f4fa9aa21bb08f09ef5',
        'datafile': 'data/one_day_exit_scam_eth.csv',
        'edgefile': 'data/transactions_eth.csv',
        'nodefile': 'data/addresses_eth.csv',
        'logfile': 'data/chunk_log_eth.csv'
    },
    'bsc': {
        'rpc': '',
        'http': 'https://bsc-dataseed.binance.org/',
        'datafile': 'data/one_day_exit_scam_bsc.csv',
        'edgefile': 'data/transactions_bsc.csv',
        'nodefile': 'data/addresses_bsc.csv',
        'logfile': 'data/chunk_log_bsc.csv'
    }
}

def get_w3():
    tmp = data[chain][query_mode]
    w3_data = {
        'eth': {
            'rpc': Web3(Web3.IPCProvider(tmp)),
            'http': Web3(Web3.HTTPProvider(tmp))
        },
        'bsc': {
            'rpc': None,
            'http': Web3(Web3.HTTPProvider(tmp)).middleware_onion.inject(geth_poa_middleware, layer=0)
        } 
    }
    return w3_data[chain][query_mode]

def write_edges(rows):
    with open(data[chain]['edgefile'], "a", encoding="UTF8") as tx_file:
        csv.writer(tx_file).writerows(rows)
    return

def write_log(row):
    with open(data[chain]['logfile'], "a", encoding="UTF8") as log_file:
        csv.writer(log_file).writerow(row)
    return

def preprocessing():
    datafile = data[chain]['datafile']
    df = pd.read_csv(datafile, dtype={'address':str, 'block_number_remove':int})
    address_df = df.\
        sort_values(["block_number_remove"]).\
        drop_duplicates(subset=["from"], keep="last").\
        reset_index(drop=True)\
        [["from", "block_number_remove"]].\
        rename({"from":"address", "block_number_remove":"use_untill"}, axis="columns")
    address_df["level"] = 0
    address_df = address_df.astype({'address': str, 'use_untill':int, 'level':int})
    return address_df

def get_txstring(tx, new_level):
    string = "{},{},{},{},{},{},{},{},{},{}".format(
        tx["from"] if 'from' in tx.keys() else None, 
        tx["to"] if 'to' in tx.keys() else None, 
        tx["value"] if 'value' in tx.keys() else None, 
        tx["effectiveGasPrice"] if 'effectiveGasPrice' in tx.keys() else None, 
        tx["gasUsed"] if 'gasUsed' in tx.keys() else None, 
        tx["hash"].hex() if 'hash' in tx.keys() else None, 
        tx["input"][:10] if 'input' in tx.keys() else None, 
        tx["blockNumber"] if 'blockNumber' in tx.keys() else None, 
        new_level, 
        tx["status"] if 'status' in tx.keys() else None
    ).split(',')
    return string

def task(parameters):
    chunkID, max_block_heigth, new_level, curr_level_address_set, curr_level_address_df = parameters
    rows_to_write = []
    new_level_address_subset = set()
    new_level_address_subdf = pd.DataFrame.from_dict({"address": [], "use_untill": [], "level": []}).astype({'address': str, 'use_untill':int, 'level':int})
    for block_number in range(chunkID, min(chunkID+chunk_size, max_block_heigth+1)):
        block = w3.eth.get_block(block_number)
        local_filtered_curr_level_addresses = curr_level_address_df[curr_level_address_df["use_untill"]>=block_number]["address"].values
        for transaction in block.transactions:
        #for transaction in block.transactions[130:140]:
            tx = w3.eth.get_transaction(transaction.hex())
            if 'to' not in tx.keys() and 'from' not in tx.keys(): continue
            if tx["to"] in curr_level_address_set and tx["to"] in local_filtered_curr_level_addresses:
                address_to_add = tx["from"]
                tx = {**tx, **w3.eth.get_transaction_receipt(transaction.hex())}
                string = get_txstring(tx, new_level)
                rows_to_write.append(string)
                if address_to_add not in new_level_address_subset:
                    new_level_address_subset.add(address_to_add)
                    row = pd.DataFrame.from_dict({"address": [address_to_add], "use_untill": [block_number], "level": [new_level]}).astype({'address': str, 'use_untill':int, 'level':int})
                    new_level_address_subdf = pd.concat([new_level_address_subdf, row], ignore_index=True)
    write_edges(rows_to_write)
    return chunkID, new_level_address_subdf

def multi(depth, store_mode='w', log=True):
    """
    depth: (int >=0) ultimo livello da archiviare compreso
    max_block_heigth e chunk_size sono due parametri da poter tarare con dei MA:
        siccome nel preprocessing vengono accettati tutti gli address del cvs, max_block_heigth non puo' essere minore di address_df["use_untill"].max(), per evitare di disegnare nel grafo alcuni nodi incorretti
        max_block_heigth puo' essere cio' che ci pare al netto del vincolo appena citato solo se 
    di default max_block_heigth e chunk_size sono:
        max_block_heigth = address_df["use_untill"].max()
        chunk_size = 1000
    """
    global w3
    w3 = get_w3()
    if store_mode == "w":
        if log == True:
            with open(data[chain]['logfile'], "w", encoding="UTF8") as log_file:
                csv.writer(log_file).writerow("level,chunkID,chunk_size".split(','))
        address_df = preprocessing()
        address_df.to_csv(data[chain]['nodefile'], index=False)
        with open(data[chain]['edgefile'], "w", encoding="UTF8") as tx_file:
            csv.writer(tx_file).writerow("from,to,value,effectiveGasPrice,gasUsed,hash,input,blockNumber,level,status".split(','))
    if store_mode == "a":
        address_df = pd.read_csv(data[chain]['nodefile'])

    curr_level = int(address_df["level"].max())
    curr_level_address_set = set(address_df["address"].values)
    curr_level_address_df = address_df.copy()
    while curr_level < depth:
        max_block_heigth = int(curr_level_address_df["use_untill"].max())
        #max_block_heigth = 10613897
        new_level = curr_level + 1
        #generate new level of edges and nodes
        with Pool(core_number) as pool:
            items = [(chunkID, max_block_heigth, new_level, curr_level_address_set, curr_level_address_df) for chunkID in range(0, max_block_heigth+1, chunk_size)]
            #items = [(chunkID, max_block_heigth, new_level, curr_level_address_set, curr_level_address_df) for chunkID in range(max_block_heigth-50, max_block_heigth+1, chunk_size)]
            new_level_address_df = pd.DataFrame.from_dict({"address":[], "use_untill":[], "level":[]}).astype({'address': str, 'use_untill':int, 'level':int})
            for chunkID, new_level_address_subdf in tqdm(pool.imap(task, items), total=len(items)):
                new_level_address_df = pd.concat([new_level_address_df, new_level_address_subdf])
                if log == True:
                    write_log('{},{},{}'.format(new_level, chunkID, chunk_size).split(','))
        #elimina da new_level_address_df tutte le righe con address ripeturi e con use_untill che non è massimo tra i doppioni
        new_level_address_df = new_level_address_df.\
            sort_values(["use_untill"]).\
            drop_duplicates(subset=["address"], keep="last").\
            reset_index(drop=True)
        #elimina da new_level_address_df le righe che hanno address presenti in address_df
        new_level_address_df = new_level_address_df[~new_level_address_df["address"].isin(address_df["address"])]
        address_df = pd.concat([address_df, new_level_address_df])
        new_level_address_df.to_csv(data[chain]['nodefile'], mode="a", header=False, index=False)
        curr_level += 1
        curr_level_address_set = set(new_level_address_df["address"].values)
        curr_level_address_df = new_level_address_df.copy()
    return

multi(depth=1, store_mode='w', log=True)