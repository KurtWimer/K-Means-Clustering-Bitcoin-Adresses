#!/bin/bash

wget https://www.walletexplorer.com/wallet/Bittrex.com?format=csv -O ->> exchanges.csv

wget https://www.walletexplorer.com/wallet/BTCCPool?format=csv -O ->> pool.csv

wget https://www.walletexplorer.com/wallet/Xapo.com?format=csv -O ->> services.csv

wget https://www.walletexplorer.com/wallet/LuckyB.it?format=csv -O ->> gambling.csv
