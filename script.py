from web3 import Web3
import pandas as pd

url_provider = 'https://mainnet.infura.io/v3/e0a4e987f3ff4f4fa9aa21bb08f09ef5'
w3 = Web3(Web3.HTTPProvider(url_provider))
w3.isConnected()

filename = 'one_day_exit_scam_eth.csv'
df = pd.read_csv(filename)

block_heigth = df["block_number_remove"].max()
address_df = df["from"].drop_duplicates().reset_index(drop=True).to_frame().rename({"from": "address"}, axis="columns")

with open("eth_tx.csv", "w", encoding="UTF8") as tx_file:
    tx_file.write("from,to,value,gas,hash,blockNumber,transactionIndex\n")
    idx = 0
    for block_number in range(block_heigth, block_heigth-1, -1):
        block = w3.eth.get_block(block_number)
        for transaction in block.transactions[::-1]:
            tx = w3.eth.get_transaction(transaction.hex())
            if tx["to"] in address_df["address"].values:
                tx_file.write("{},{},{},{},{},{},{}\n".format(
                    tx["from"], tx["to"], tx["value"], tx["gas"], tx["hash"].hex(), tx["blockNumber"], tx["transactionIndex"]
                ))
                if tx["from"] not in address_df["address"].values:
                    address_df.loc[idx] = tx["from"]
                    idx += 1
