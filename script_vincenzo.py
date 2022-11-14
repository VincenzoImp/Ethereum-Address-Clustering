import pandas as pd
from web3 import Web3
from web3.middleware import geth_poa_middleware
from multiprocessing import Pool, Manager
from tqdm import tqdm


w3 = None


#ETH
rpc = '/eth/eth_node/node/geth.ipc'
http = 'https://mainnet.infura.io/v3/e0a4e987f3ff4f4fa9aa21bb08f09ef5'
datafile = 'data/one_day_exit_scam_eth.csv'
edgefile = 'data/tx_eth.csv'
nodefile = 'data/address_eth.csv'

"""
#BSC
rpc = ''
http = 'https://bsc-dataseed.binance.org/'
datafile = 'data/one_day_exit_scam_bsc.csv'
edgefile = 'data/tx_bsc.csv'
nodefile = 'data/address_bsc.csv'
"""

def preprocessing(datafile):
    df = pd.read_csv(datafile)
    address_df = df.\
        sort_values(["block_number_remove"]).\
        drop_duplicates(subset=["from"], keep="last").\
        reset_index(drop=True)\
        [["from", "block_number_remove"]].\
        rename({"from":"address", "block_number_remove":"use_untill"}, axis="columns")
    return address_df


def task(data):
    start, lock, store, use_untill, step, max_block_heigth, new_level, curr_level_address_set, curr_level_address_df = data
    new_level_address_subset = set()
    new_level_address_subdf = pd.DataFrame.from_dict({"address": [], "use_untill": [], "level": []})
    for block_number in range(min(start+step-1, max_block_heigth), start-1, -1):
        block = w3.eth.get_block(block_number)
        for transaction in block.transactions[::-1]:
            tx = w3.eth.get_transaction(transaction.hex())
            try:
                t_status = tx["status"]
            except KeyError:
                t_status = None
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
            if address_to_add != None:
                tx = {**tx, **w3.eth.get_transaction_receipt(transaction.hex())}
                with lock:
                    with open(edgefile, "a", encoding="UTF8") as tx_file:
                        tx_file.write("{},{},{},{},{},{},{},{},{},{}\n".format(
                            tx["from"], tx["to"], w3.fromWei(tx["value"], 'ether'), tx["effectiveGasPrice"], tx["gasUsed"], tx["hash"].hex(), tx["input"][:10], tx["blockNumber"], new_level, t_status
                            ))
                if address_to_add not in new_level_address_subset:
                    new_level_address_subset.add(address_to_add)
                    row = pd.DataFrame.from_dict({"address": [address_to_add], "use_untill": [block_number], "level": [new_level]})
                    new_level_address_subdf = pd.concat([new_level_address_subdf, row], ignore_index=True)
    return new_level_address_subdf


def multi(address_df, chain, depth=2, mode="w", store="received", use_untill=True): 
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
    global w3
    if chain == "eth":
        w3 = Web3(Web3.HTTPProvider(http))
    elif chain == "bsc":
        w3 = Web3(Web3.HTTPProvider(http))
        w3.middleware_onion.inject(geth_poa_middleware, layer=0)
    if mode == "w":
        if "level" not in address_df.columns:
            address_df["level"] = 0
        address_df.to_csv(nodefile, index=False)
        with open(edgefile, "w", encoding="UTF8") as tx_file:
            tx_file.write("from,to,value,effectiveGasPrice,gasUsed,hash,input,blockNumber,level,status\n")
        curr_level = 0
        curr_level_address_set = set(address_df["address"].values)
        curr_level_address_df = address_df
        while curr_level < depth:
            max_block_heigth = address_df["use_untill"].max()
            step = 10000
            new_level = curr_level + 1
            #generate new level of edges and nodes
            with Manager() as manager:
                lock = manager.Lock()
                with Pool(25) as pool:
                    items = [(i, lock, store, use_untill, step, max_block_heigth, new_level, curr_level_address_set, curr_level_address_df) for i in range(0, max_block_heigth+1, step)]
                    #new_levemax_block_heigth_address_subdf_list = list(tqdm(pool.imap(task, items), total=len(items)))
                    for new_level_address_subdf in tqdm(pool.imap(task, items), total=len(items)): 
                        new_level_address_df = pd.concat([new_level_address_subdf])
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
    return curr_level_address_df


multi(preprocessing(datafile), "eth")