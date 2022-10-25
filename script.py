import pandas as pd
from web3 import Web3
from web3.middleware import geth_poa_middleware

eth_url = 'https://mainnet.infura.io/v3/e0a4e987f3ff4f4fa9aa21bb08f09ef5'
bsc_url = "https://bsc-dataseed.binance.org/"

eth_datafile = 'one_day_exit_scam_eth.csv'
bsc_datafile = 'one_day_exit_scam_bsc.csv'

eth_edgefile = 'tx_eth.csv'
bsc_edgefile = 'tx_bsc.csv'

eth_nodefile = 'address_eth.csv'
bsc_nodefile = 'address_bsc.csv'

eth_w3 = Web3(Web3.HTTPProvider(eth_url))
bsc_w3 = Web3(Web3.HTTPProvider(bsc_url))
bsc_w3.middleware_onion.inject(geth_poa_middleware, layer=0)

def foo(datafile, edgefile, nodefile, w3):
    df = pd.read_csv(datafile)
    block_heigth = df["block_number_remove"].max()
    address_df = df.\
        sort_values(["block_number_remove"]).\
        drop_duplicates(subset=["from"], keep="last").\
        reset_index(drop=True)\
        [["from", "block_number_remove"]].\
        rename({"from":"address", "block_number_remove":"last_rugpull"}, axis="columns")

    with open(edgefile, "w", encoding="UTF8") as tx_file:
        tx_file.write("from,to,value,gas,hash,blockNumber,transactionIndex\n")
        idx = address_df.shape[0]
        for block_number in range(block_heigth, block_heigth-1, -1):
            block = w3.eth.get_block(block_number)
            for transaction in block.transactions[::-1]:
                tx = w3.eth.get_transaction(transaction.hex())
                if tx["to"] in address_df[address_df["last_rugpull"]<=block_number]["address"].values:
                    tx_file.write("{},{},{},{},{},{},{}\n".format(
                        tx["from"], tx["to"], tx["value"], tx["gas"], tx["hash"].hex(), tx["blockNumber"], tx["transactionIndex"]
                    ))
                    if tx["from"] not in address_df["address"].values:
                        row = pd.DataFrame.from_dict({"address": [tx["from"]], "last_rugpull": [block_number]})
                        address_df = pd.concat([address_df, row], ignore_index=True)
    address_df.to_csv(nodefile, index=False)
    return

foo(eth_datafile, eth_edgefile, eth_nodefile, eth_w3)
foo(bsc_datafile, bsc_edgefile, bsc_nodefile, bsc_w3)