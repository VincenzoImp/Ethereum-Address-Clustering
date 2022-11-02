import pandas as pd
from web3 import Web3
#from web3.middleware import geth_poa_middleware
from multiprocessing import Pool, Manager
from tqdm import tqdm
import time

def multi(address_df, edgefile, nodefile, depth=10, mode="w", store="received", use_untill=True): 
    """
    depth: (int >=0) ultimo livello da archiviare compreso
    mode: ("w", "a") 
        "w": dal livello 0 a depth compreso sovrascrivendo i file
        "a": dall'ultimo livello gia' archiviato nei file fino a depth compreso appendendo nei file
    store: ("received", "sent", "both")
        "received": per la creazione di un nuovo livello vengono archiviate solo le transazioni ricevute dagli address del livello corrente
        "sent": per la creazione di un nuovo livello vengono archiviate solo le transazioni inviate dagli address del livello corrente
        "both": per la creazione di un nuovo livello vengono archiviate solo le transazioni inviate e ricevute dagli address del livello corrente
    use_untill: (True, False)
        True: le transazioni, una volta filtrate da store, vengono filtrate da block_number <= use_untill
        False: le transazioni, una volta filtrate da store, vengono accettate tutte
    """
    """
    max_block_heigth e step sono due parametri da poter tarare con dei MA:
        siccome nel preprocessing vengono accettati tutti gli address del cvs, max_block_heigth non puo' essere minore di address_df["use_untill"].max(), per evitare di disegnare nel grafo alcuni nodi incorretti
        max_block_heigth puo' essere cio' che ci pare al netto del vincolo appena citato solo se 
    di default max_block_heigth e step sono:
        max_block_heigth = address_df["use_untill"].max()
        step = 1000
    """
    if mode == "w":
        if "level" not in address_df.columns:
            address_df["level"] = 0
        address_df.to_csv(nodefile, index=False)
        with open(edgefile, "w", encoding="UTF8") as tx_file:
            tx_file.write("from,to,value,effectiveGasPrice,gasUsed,hash,input,blockNumber,level\n")
        curr_level = 0
        curr_level_address_set = set(address_df["address"].values)
        curr_level_address_df = address_df
        while curr_level < depth:
            max_block_heigth = address_df["use_untill"].max()
            number_of_chunck = 10000
            step = max_block_heigth//number_of_chunck
            pbar = tqdm(total=number_of_chunck)
            new_level = curr_level + 1
            #generate new level of edges and nodes
            with Manager() as manager:
                lock = manager.Lock()
                with Pool() as pool:
                    items = [(i, lock, store, use_untill, step, max_block_heigth, new_level, curr_level_address_set, curr_level_address_df, pbar) for i in range(0, max_block_heigth+1, step)]
                    new_level_address_subdf_list = pool.map(task, items)
            new_level_address_df = pd.concat(new_level_address_subdf_list)
            #elimina da new_level_address_df tutte le righe con address ripeturi e con use_untill che non è massimo tra i doppioni
            new_level_address_df = new_level_address_df.\
                sort_values(["use_untill"]).\
                drop_duplicates(subset=["address"], keep="last").\
                reset_index(drop=True)\
            #elimina da new_level_address_df le righe che hanno address presenti in address_df
            new_level_address_df = new_level_address_df[~new_level_address_df["address"].isin(address_df["address"])]
            address_df = pd.concat([new_level_address_df, address_df])
            new_level_address_df.to_csv(nodefile, mode="a", header=False, index=False)
            curr_level += 1
            curr_level_address_set = set(new_level_address_df["address"].values)
            curr_level_address_df = new_level_address_df
            pbar.close()
    return curr_level_address_df

def task(data):
    start, lock, store, use_untill, step, max_block_heigth, new_level, curr_level_address_set, curr_level_address_df, pbar = data
    new_level_address_subset = set()
    new_level_address_subdf = pd.DataFrame.from_dict({"address": [], "use_untill": [], "level": []})
    for block_number in range(min(start+step-1, max_block_heigth), start-1, -1):
        block = w3.eth.get_block(block_number)
        for transaction in block.transactions[::-1]:
            tx = {**w3.eth.get_transaction(transaction.hex()), **w3.eth.get_transaction_receipt(transaction.hex())}
            if tx["status"] == 1:
                address_to_add = None
                if store == "received" and use_untill == True:
                    if tx["to"] in curr_level_address_set and tx["to"] in curr_level_address_df[curr_level_address_df["use_untill"]<=block_number]["address"].values:
                        address_to_add = tx["from"]
                elif store == "received" and use_untill == False:
                    if tx["to"] in curr_level_address_set:
                        address_to_add = tx["from"]
                elif store == "sent" and use_untill == True:
                    if tx["from"] in curr_level_address_set and tx["from"] in curr_level_address_df[curr_level_address_df["use_untill"]<=block_number]["address"].values:
                        address_to_add = tx["to"]
                elif store == "sent" and use_untill == False:
                    if tx["from"] in curr_level_address_set:
                        address_to_add = tx["to"]
                elif store == "both" and use_untill == True:
                    if tx["from"] in curr_level_address_set and tx["from"] in curr_level_address_df[curr_level_address_df["use_untill"]<=block_number]["address"].values:
                        address_to_add = tx["to"]
                    elif tx["to"] in curr_level_address_set and tx["to"] in curr_level_address_df[curr_level_address_df["use_untill"]<=block_number]["address"].values:
                        address_to_add = tx["from"]
                elif store == "both" and use_untill == False:
                    if tx["from"] in curr_level_address_set:
                        address_to_add = tx["to"]
                    elif tx["to"] in curr_level_address_set:
                        address_to_add = tx["from"]
                if address_to_add == None:
                    with lock:
                        with open(edgefile, "a", encoding="UTF8") as tx_file:
                            tx_file.write("{},{},{},{},{},{},{},{},{}\n".format(
                                tx["from"], tx["to"], tx["value"], tx["effectiveGasPrice"], tx["gasUsed"], tx["hash"].hex(), tx["input"][:10], tx["blockNumber"], new_level
                                ))
                    if address_to_add not in new_level_address_subset:
                        new_level_address_subset.add(address_to_add)
                        row = pd.DataFrame.from_dict({"address": [address_to_add], "use_untill": [block_number], "level": [new_level]})
                        new_level_address_subdf = pd.concat([new_level_address_subdf, row], ignore_index=True)
    pbar.update(1)
    return new_level_address_subdf

def preprocessing(datafile):
    df = pd.read_csv(datafile)
    address_df = df.\
        sort_values(["block_number_remove"]).\
        drop_duplicates(subset=["from"], keep="last").\
        reset_index(drop=True)\
        [["from", "block_number_remove"]].\
        rename({"from":"address", "block_number_remove":"use_untill"}, axis="columns")
    return address_df


eth_url = 'https://mainnet.infura.io/v3/e0a4e987f3ff4f4fa9aa21bb08f09ef5'
#bsc_url = "https://bsc-dataseed.binance.org/"

eth_datafile = 'one_day_exit_scam_eth.csv'
#bsc_datafile = 'one_day_exit_scam_bsc.csv'

eth_edgefile = 'tx_eth.csv'
#bsc_edgefile = 'tx_bsc.csv'

eth_nodefile = 'address_eth.csv'
#bsc_nodefile = 'address_bsc.csv'

eth_w3 = Web3(Web3.HTTPProvider(eth_url))
#bsc_w3 = Web3(Web3.HTTPProvider(bsc_url))
#bsc_w3.middleware_onion.inject(geth_poa_middleware, layer=0)



datafile, edgefile, nodefile, w3 = eth_datafile, eth_edgefile, eth_nodefile, eth_w3
#datafile, edgefile, nodefile, w3 = bsc_datafile, bsc_edgefile, bsc_nodefile, bsc_w3

multi(preprocessing(datafile), edgefile, nodefile)